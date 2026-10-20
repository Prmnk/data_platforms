"""Upsert per-customer aggregates from the orders table into a customers Delta table (MERGE).

Uses SQL MERGE INTO rather than delta-spark's DeltaTable API, so it also runs on Sail."""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from delta_util import delta_table_exists

p = argparse.ArgumentParser()
p.add_argument("--orders", default="s3a://warehouse/orders")
p.add_argument("--path", default="s3a://warehouse/customers")
a = p.parse_args()

spark = SparkSession.builder.getOrCreate()
updates = (
    spark.read.format("delta").load(a.orders)
    .groupBy("customer_id")
    .agg(F.count("*").alias("order_count"), F.sum("amount").alias("total_amount"),
         F.max("order_date").alias("last_order_date"))
    .withColumn("updated_at", F.current_timestamp())
)

if delta_table_exists(spark, a.path):
    updates.createOrReplaceTempView("customer_updates")
    spark.sql(f"""
        MERGE INTO delta.`{a.path}` t
        USING customer_updates u ON t.customer_id = u.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """).collect()
else:
    updates.write.format("delta").save(a.path)

print(f"customers: {spark.read.format('delta').load(a.path).count()} rows")
spark.stop()
