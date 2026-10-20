"""Keeps two tasks busy for --seconds, to demonstrate timeouts and cancellation.

A Python UDF rather than an RDD, so it runs on Sail (no RDD support) as well as Spark."""
import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

p = argparse.ArgumentParser()
p.add_argument("--seconds", type=int, default=120)
a = p.parse_args()

spark = SparkSession.builder.getOrCreate()
secs = a.seconds
nap = F.udf(lambda _: time.sleep(secs) or 1, "int")
spark.range(2).repartition(2).select(nap("id")).collect()
print(f"slept {secs}s")
spark.stop()
