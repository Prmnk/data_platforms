"""Exit non-zero unless a Delta table exists with at least --min-rows rows. Used by e2e tests."""
import argparse
import sys

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

p = argparse.ArgumentParser()
p.add_argument("--path", required=True)
p.add_argument("--min-rows", type=int, default=1)
a = p.parse_args()

spark = SparkSession.builder.getOrCreate()
if not DeltaTable.isDeltaTable(spark, a.path):
    print(f"{a.path} is not a Delta table")
    sys.exit(2)
n = spark.read.format("delta").load(a.path).count()
DeltaTable.forPath(spark, a.path).history(5).select("version", "operation", "timestamp").show(truncate=False)
spark.stop()
print(f"{a.path}: {n} rows")
sys.exit(0 if n >= a.min_rows else 1)
