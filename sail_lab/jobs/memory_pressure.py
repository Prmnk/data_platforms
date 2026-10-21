"""A full sort + high-cardinality aggregation sized to exceed Sail's memory pool and spill to disk."""
import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from saillab.sdk import report, timed

p = argparse.ArgumentParser()
p.add_argument("--rows", type=int, default=20_000_000)
a = p.parse_args()

spark = SparkSession.builder.getOrCreate()
df = spark.range(a.rows).select(
    F.col("id"),
    F.sha2(F.col("id").cast("string"), 256).alias("payload"),   # 64-char string per row
    F.pmod(F.xxhash64("id"), F.lit(a.rows // 2)).alias("key"),
)

# A global row_number needs the whole input sorted (orderBy().limit() would only keep a top-k).
with timed("sort"):
    ranked = df.withColumn("rn", F.row_number().over(Window.orderBy("payload")))
    sample = ranked.where(F.col("rn") % (a.rows // 4) == 1).select("rn", "payload").collect()
# Use the aggregate's output, so the optimizer cannot prune max(payload) away.
with timed("aggregate"):
    agg = df.groupBy("key").agg(F.max("payload").alias("m"), F.count("*").alias("n"))
    stats = agg.select(F.count("*").alias("groups"), F.sum(F.length("m")).alias("chars")).collect()[0]

for r in sorted(sample, key=lambda r: r["rn"]):
    print(f"row {r['rn']:>10}: {r['payload'][:16]}...")
print(f"{a.rows} rows sorted; {stats['groups']} groups aggregated")
report(rows=a.rows, groups=stats["groups"])
spark.stop()
