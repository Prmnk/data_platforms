"""Fails until attempt N, to demonstrate retries. Pass --attempt "{{ attempt }}"."""
import argparse
import sys

from pyspark.sql import SparkSession

p = argparse.ArgumentParser()
p.add_argument("--attempt", type=int, required=True)
p.add_argument("--succeed-on", type=int, default=3)
a = p.parse_args()

spark = SparkSession.builder.getOrCreate()
total = spark.range(1_000_000).selectExpr("sum(id)").first()[0]
spark.stop()

if a.attempt < a.succeed_on:
    print(f"attempt {a.attempt}: failing on purpose (will succeed on {a.succeed_on})")
    sys.exit(1)
print(f"attempt {a.attempt}: success, sum={total}")
