"""Keeps executors busy for --seconds, to demonstrate timeouts and cancellation."""
import argparse
import time

from pyspark.sql import SparkSession

p = argparse.ArgumentParser()
p.add_argument("--seconds", type=int, default=120)
a = p.parse_args()

spark = SparkSession.builder.getOrCreate()
secs = a.seconds
spark.sparkContext.parallelize(range(2), 2).map(lambda _: time.sleep(secs) or 1).count()
print(f"slept {secs}s")
spark.stop()
