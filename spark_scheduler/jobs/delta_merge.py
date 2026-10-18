"""Upsert per-customer aggregates from the orders table into a customers Delta table (MERGE)."""
import argparse

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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

if DeltaTable.isDeltaTable(spark, a.path):
    (DeltaTable.forPath(spark, a.path).alias("t")
        .merge(updates.alias("u"), "t.customer_id = u.customer_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
else:
    updates.write.format("delta").save(a.path)

print(f"customers: {spark.read.format('delta').load(a.path).count()} rows")
spark.stop()
