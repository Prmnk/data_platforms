"""Keeps Sail busy for --seconds with repeated queries: use it to try `./lab cancel` and timeouts."""
import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

p = argparse.ArgumentParser()
p.add_argument("--seconds", type=int, default=300)
a = p.parse_args()

spark = SparkSession.builder.getOrCreate()
deadline, n = time.time() + a.seconds, 0
while time.time() < deadline:
    n += 1
    s = spark.range(20_000_000).select(F.sum(F.sqrt("id"))).collect()[0][0]
    print(f"query {n}: {s:,.0f}", flush=True)
spark.stop()
