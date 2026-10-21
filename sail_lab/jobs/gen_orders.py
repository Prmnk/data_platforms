"""Generate synthetic customers and orders as Parquet on MinIO (input for the other jobs)."""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from saillab.sdk import report, timed

p = argparse.ArgumentParser()
p.add_argument("--orders", type=int, default=2_000_000)
p.add_argument("--customers", type=int, default=50_000)
p.add_argument("--days", type=int, default=90)
p.add_argument("--out", default="s3://lake/raw")
p.add_argument("--files", type=int, default=8)
a = p.parse_args()

spark = SparkSession.builder.getOrCreate()
countries = F.array(*[F.lit(c) for c in ("IN", "US", "GB", "DE", "FR", "BR", "JP", "SG")])
segments = F.array(*[F.lit(s) for s in ("consumer", "smb", "enterprise")])

customers = spark.range(a.customers).select(
    F.col("id").alias("customer_id"),
    F.concat(F.lit("customer-"), F.col("id").cast("string")).alias("name"),
    F.element_at(countries, (F.col("id") % 8 + 1).cast("int")).alias("country"),
    F.element_at(segments, (F.col("id") % 3 + 1).cast("int")).alias("segment"),
    F.date_sub(F.lit("2026-01-01").cast("date"), (F.col("id") % 1000).cast("int")).alias("signup_date"),
)

statuses = F.array(*[F.lit(s) for s in ("PAID", "PAID", "PAID", "SHIPPED", "CANCELLED", "REFUNDED")])
orders = spark.range(a.orders).select(
    F.col("id").alias("order_id"),
    F.pmod(F.xxhash64("id"), F.lit(a.customers)).alias("customer_id"),  # spread orders over customers
    F.date_add(F.lit("2026-01-01").cast("date"), (F.col("id") % a.days).cast("int")).alias("order_date"),
    F.round(F.rand(seed=7) * 490 + 10, 2).alias("amount"),
    (F.col("id") % 5 + 1).alias("items"),
    F.element_at(statuses, (F.col("id") % 6 + 1).cast("int")).alias("status"),
)

with timed("write_customers"):
    customers.repartition(2).write.mode("overwrite").parquet(f"{a.out}/customers")
with timed("write_orders"):
    orders.repartition(a.files).write.mode("overwrite").parquet(f"{a.out}/orders")

n_orders = spark.read.parquet(f"{a.out}/orders").count()
n_customers = spark.read.parquet(f"{a.out}/customers").count()
print(f"wrote {n_orders} orders and {n_customers} customers under {a.out}")
report(orders=n_orders, customers=n_customers)
spark.stop()
