"""Exit non-zero unless a Delta table exists with at least --min-rows rows. Used by e2e tests."""
import argparse
import sys

from pyspark.sql import SparkSession

from delta_util import delta_table_exists

p = argparse.ArgumentParser()
p.add_argument("--path", required=True)
p.add_argument("--min-rows", type=int, default=1)
a = p.parse_args()

spark = SparkSession.builder.getOrCreate()
if not delta_table_exists(spark, a.path):
    print(f"{a.path} is not a Delta table")
    sys.exit(2)
n = spark.read.format("delta").load(a.path).count()
try:
    spark.sql(f"DESCRIBE HISTORY delta.`{a.path}`").select("version", "operation", "timestamp") \
        .show(5, truncate=False)
except Exception as e:  # noqa: BLE001 - Sail does not support DESCRIBE HISTORY yet
    print(f"(no table history: {str(e).splitlines()[0]})")
spark.stop()
print(f"{a.path}: {n} rows")
sys.exit(0 if n >= a.min_rows else 1)
