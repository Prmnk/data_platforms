"""Join, aggregate and rank over the raw orders (run gen_orders.py first); writes a Parquet mart."""
import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from saillab.sdk import report, timed

p = argparse.ArgumentParser()
p.add_argument("--raw", default="s3://lake/raw")
p.add_argument("--out", default="s3://lake/marts")
a = p.parse_args()

spark = SparkSession.builder.getOrCreate()
orders = spark.read.parquet(f"{a.raw}/orders").where(F.col("status").isin("PAID", "SHIPPED"))
customers = spark.read.parquet(f"{a.raw}/customers")
enriched = orders.join(customers, "customer_id")

daily = (enriched.groupBy("order_date", "country")
         .agg(F.count("*").alias("orders"), F.sum("amount").cast("decimal(14,2)").alias("revenue"),
              F.countDistinct("customer_id").alias("buyers"))
         .withColumn("revenue_7d", F.sum("revenue").over(
             Window.partitionBy("country").orderBy("order_date").rowsBetween(-6, 0)).cast("decimal(14,2)")))

top_customers = (enriched.groupBy("country", "customer_id", "name", "segment")
                 .agg(F.sum("amount").cast("decimal(14,2)").alias("spend"))
                 .withColumn("rank", F.row_number().over(Window.partitionBy("country").orderBy(F.desc("spend"))))
                 .where("rank <= 3"))

with timed("daily_revenue"):
    daily.write.mode("overwrite").partitionBy("country").parquet(f"{a.out}/daily_revenue")
with timed("top_customers"):
    top_customers.write.mode("overwrite").parquet(f"{a.out}/top_customers")

spark.read.parquet(f"{a.out}/top_customers").orderBy("country", "rank").show(24, truncate=False)
days = spark.read.parquet(f"{a.out}/daily_revenue")
days.orderBy(F.desc("order_date"), "country").show(8)
report(daily_rows=days.count(), top_customer_rows=spark.read.parquet(f"{a.out}/top_customers").count())
spark.stop()
