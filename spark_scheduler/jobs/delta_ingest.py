"""Generate a day's worth of synthetic orders and write them to a Delta table on MinIO.

Re-running for the same --date replaces that date's rows (replaceWhere), so retries
never duplicate data. Runs unchanged on Spark and on Sail.
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from delta_util import delta_table_exists

p = argparse.ArgumentParser()
p.add_argument("--date", required=True, help="logical date, ISO format")
p.add_argument("--path", default="s3a://warehouse/orders")
p.add_argument("--rows", type=int, default=10_000)
a = p.parse_args()
day = a.date[:10]

spark = SparkSession.builder.getOrCreate()
orders = (
    spark.range(a.rows)
    .select(
        F.concat(F.lit(day + "-"), F.col("id").cast("string")).alias("order_id"),
        (F.col("id") % 500).alias("customer_id"),
        F.round(F.rand(seed=42) * 200, 2).cast("decimal(10,2)").alias("amount"),
        F.to_date(F.lit(day)).alias("order_date"),
        F.current_timestamp().alias("ingested_at"),
    )
)
writer = orders.write.format("delta").partitionBy("order_date")
if delta_table_exists(spark, a.path):
    writer.mode("overwrite").option("replaceWhere", f"order_date = '{day}'").save(a.path)
else:
    # First run creates the table. Sail rejects replaceWhere on a table that does not exist
    # yet; append (not overwrite) so two first runs for different days cannot wipe each other.
    writer.mode("append").save(a.path)

n = spark.read.format("delta").load(a.path).where(F.col("order_date") == day).count()
print(f"wrote {n} orders for {day} to {a.path}")
spark.stop()
