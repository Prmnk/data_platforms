"""Ingest one batch of raw order events into a Delta "silver" orders table.

  1. read the batch's JSON with an explicit schema (unknown fields are dropped)
  2. schema checks: required fields, number and timestamp formats, allowed values
     -> failing rows go to a quarantine Delta table with the reasons
  3. dedup: exact redeliveries (event_id), then keep the latest version per order_id
  4. MERGE into silver: insert new orders, update only when the event is newer

Re-running a batch is safe: the quarantine write replaces that batch's rows and the MERGE only
applies newer versions. Only DataFrame + SQL APIs, so it runs unchanged on Spark and on Sail.
The last line is `PIPELINE_METRICS {json}` with row counts and step timings.
"""
import argparse
import json
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from delta_util import delta_table_exists

p = argparse.ArgumentParser()
p.add_argument("--batch", type=int, required=True)
p.add_argument("--landing", default="s3a://warehouse/landing/orders")
p.add_argument("--silver", default="s3a://warehouse/silver/orders")
p.add_argument("--quarantine", default="s3a://warehouse/quarantine/orders")
a = p.parse_args()

CURRENCIES = ["USD", "EUR", "INR", "GBP"]
STATUSES = ["CREATED", "PAID", "SHIPPED", "CANCELLED"]
# Numbers are declared as numbers: Spark would turn a JSON number into a string for a string
# field, but Sail 0.7.2 fails the read ("expected string got 43156").
RAW = T.StructType([
    T.StructField("event_id", T.StringType()), T.StructField("order_id", T.StringType()),
    T.StructField("customer_id", T.LongType()), T.StructField("amount", T.StringType()),
    T.StructField("currency", T.StringType()), T.StructField("status", T.StringType()),
    T.StructField("items", T.LongType()), T.StructField("updated_at", T.StringType()),
])

spark = SparkSession.builder.getOrCreate()
timings, t0 = {}, time.time()


def lap(name):
    global t0
    timings[name] = round(time.time() - t0, 2)
    print(f"STEP {name} {timings[name]}", flush=True)
    t0 = time.time()


raw = spark.read.schema(RAW).json(f"{a.landing}/batch={a.batch}")

# ---- 2. schema checks (regex first, so casts never see bad input on either engine)
checks = [
    ("missing_event_id", F.col("event_id").isNull()),
    ("missing_order_id", F.col("order_id").isNull()),
    ("bad_customer_id", F.coalesce(F.col("customer_id") < 0, F.lit(True))),
    ("bad_amount", ~F.coalesce(F.col("amount").rlike(r"^[0-9]+(\.[0-9]{1,2})?$"), F.lit(False))),
    ("bad_currency", ~F.coalesce(F.col("currency").isin(CURRENCIES), F.lit(False))),
    ("bad_status", ~F.coalesce(F.col("status").isin(STATUSES), F.lit(False))),
    ("bad_items", ~F.coalesce(F.col("items").between(1, 9999), F.lit(False))),
    ("bad_updated_at", ~F.coalesce(
        F.col("updated_at").rlike(r"^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$"), F.lit(False))),
]
checked = raw.withColumn("reasons", F.concat_ws(",", *[F.when(cond, F.lit(name)) for name, cond in checks]))

rejects = (checked.where(F.col("reasons") != "")
           .withColumn("batch", F.lit(a.batch))
           .withColumn("quarantined_at", F.current_timestamp()))
qwriter = rejects.write.format("delta").partitionBy("batch")
if delta_table_exists(spark, a.quarantine):
    qwriter.mode("overwrite").option("replaceWhere", f"batch = {a.batch}").save(a.quarantine)
else:
    qwriter.mode("append").save(a.quarantine)
lap("quarantine_write_s")

valid = checked.where(F.col("reasons") == "").select(
    "event_id", "order_id",
    "customer_id",
    F.col("amount").cast("decimal(12,2)").alias("amount"),
    "currency", "status",
    F.col("items").cast("int").alias("items"),
    F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss").alias("updated_at"),
)

# ---- 3. dedup: redeliveries first, then latest version per order (event_id breaks ties)
latest = (valid.dropDuplicates(["event_id"])
          .withColumn("rn", F.row_number().over(
              Window.partitionBy("order_id").orderBy(F.col("updated_at").desc(), F.col("event_id").desc())))
          .where(F.col("rn") == 1).drop("rn")
          .withColumn("source_batch", F.lit(a.batch)))

# ---- 4. MERGE into silver
if delta_table_exists(spark, a.silver):
    latest.createOrReplaceTempView("batch_orders")
    spark.sql(f"""
        MERGE INTO delta.`{a.silver}` t
        USING batch_orders s ON t.order_id = s.order_id
        WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """).collect()
else:
    latest.write.format("delta").mode("append").save(a.silver)
lap("merge_s")

# ---- metrics (separate read so the timings above are just the pipeline)
q = spark.read.format("delta").load(a.quarantine).where(F.col("batch") == a.batch)
by_reason = {r["reason"]: int(r["n"]) for r in q.select(F.explode(F.split("reasons", ",")).alias("reason"))
             .groupBy("reason").agg(F.count(F.lit(1)).alias("n")).collect()}
silver = spark.read.format("delta").load(a.silver)
s = silver.agg(F.count(F.lit(1)).alias("rows"), F.sum("amount").alias("amount"),
               F.sum(F.when(F.col("source_batch") == a.batch, 1).otherwise(0)).alias("from_batch")).first()
lap("metrics_s")

metrics = {
    "batch": a.batch,
    "rejected": int(q.count()),
    "rejected_by_reason": dict(sorted(by_reason.items())),
    "silver_rows": int(s["rows"]), "silver_amount": str(s["amount"]),
    "silver_rows_from_batch": int(s["from_batch"]),  # int(): Spark Connect returns numpy ints
    "timings": timings,
}
spark.stop()
print("PIPELINE_METRICS " + json.dumps(metrics))
