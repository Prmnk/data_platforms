"""Smoke test: an aggregation, Sail's physical plan for it, and a Python UDF (seconds)."""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from saillab.sdk import report, timed

p = argparse.ArgumentParser()
p.add_argument("--rows", type=int, default=1_000_000)
a = p.parse_args()

spark = SparkSession.builder.getOrCreate()
df = spark.range(a.rows).select(
    (F.col("id") % 10).alias("bucket"),
    (F.rand(seed=1) * 100).alias("value"),
)
agg = df.groupBy("bucket").agg(F.count("*").alias("n"), F.round(F.avg("value"), 2).alias("avg_value")).orderBy("bucket")

print("Sail's plan (DataFusion operators, not Spark's):")
agg.explain()
with timed("aggregate"):
    agg.show()

# Row-at-a-time Python UDFs run in Sail's embedded interpreter and are far slower than
# built-in expressions (compare them in python_udfs.py), so use a sample here.
square = F.udf(lambda x: x * x, "long")
with timed("python_udf"):
    df.limit(100_000).select(square("bucket").alias("bucket_squared")).distinct().orderBy("bucket_squared").show()

report(rows=a.rows)
spark.stop()
