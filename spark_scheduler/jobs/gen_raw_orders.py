"""Generate one batch of raw order events as JSON lines, with the defects a real feed has.

    --batch 1   N events for ~0.7N new orders (several versions per order)
    --batch 2+  half the events update orders from earlier batches, half are new orders

Defects (rates are of all events):
  ~3%   exact duplicate deliveries (same event_id)
  ~0.5% missing order_id         ~0.5% amount not a number or negative
  ~0.5% unknown currency/status  ~0.3% unparsable updated_at
  ~1%   an extra `promo_code` field (schema drift; the pipeline ignores unknown fields)

Generated once and read by both engines, so every benchmark run sees identical input.
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

p = argparse.ArgumentParser()
p.add_argument("--batch", type=int, required=True)
p.add_argument("--events", type=int, default=2_000_000)
p.add_argument("--out", default="s3a://warehouse/landing/orders")
p.add_argument("--files", type=int, default=16)
a = p.parse_args()

spark = SparkSession.builder.getOrCreate()
b, n = a.batch, a.events
orders_per_batch = int(n * 0.7)
base_ts = 1_790_000_000 + b * 86_400  # one day per batch

r = lambda k: F.rand(seed=b * 100 + k)
ev = spark.range(n)
if b == 1:
    order_num = F.col("id") % orders_per_batch
else:  # even ids update older orders, odd ids create new ones
    order_num = F.when(F.col("id") % 2 == 0, (F.col("id") * 7919) % (orders_per_batch * (b - 1))) \
                 .otherwise(orders_per_batch * (b - 1) + F.col("id") % orders_per_batch)

currency = F.when(r(1) < 0.005, F.lit("XXX")).when(F.col("id") % 4 == 0, "USD") \
            .when(F.col("id") % 4 == 1, "EUR").when(F.col("id") % 4 == 2, "INR").otherwise("GBP")
status = F.when(r(2) < 0.005, F.lit("LOST")).when(F.col("id") % 4 == 0, "CREATED") \
          .when(F.col("id") % 4 == 1, "PAID").when(F.col("id") % 4 == 2, "SHIPPED").otherwise("CANCELLED")
amount = F.when(r(3) < 0.0025, F.lit("N/A")).when(r(4) < 0.0025, F.lit("-12.50")) \
          .otherwise(F.format_string("%.2f", F.round(r(5) * 500 + 1, 2)))
updated_at = F.when(r(6) < 0.003, F.lit("not-a-date")) \
              .otherwise(F.from_unixtime(F.lit(base_ts) + (F.col("id") * 13) % 86_400))

events = ev.select(
    F.concat(F.lit(f"b{b}-"), F.col("id").cast("string")).alias("event_id"),
    F.when(r(7) < 0.005, F.lit(None)).otherwise(F.concat(F.lit("o"), order_num.cast("string"))).alias("order_id"),
    (order_num % 50_000).alias("customer_id"),
    amount.alias("amount"),
    currency.alias("currency"),
    status.alias("status"),
    (F.col("id") % 5 + 1).alias("items"),
    updated_at.alias("updated_at"),
    F.when(r(8) < 0.01, F.lit("FESTIVE10")).alias("promo_code"),
)
dupes = events.sample(fraction=0.03, seed=b)
(events.unionByName(dupes).repartition(a.files)
    .write.mode("overwrite").json(f"{a.out}/batch={b}"))
print(f"batch {b}: {n} events + ~{int(n * 0.03)} duplicates -> {a.out}/batch={b}")
spark.stop()
