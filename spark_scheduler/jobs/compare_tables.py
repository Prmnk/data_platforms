"""Exit non-zero unless two Delta tables hold exactly the same rows (ignoring --ignore columns).

Used to check that Spark and Sail produced identical silver and quarantine tables.
"""
import argparse
import sys

from pyspark.sql import SparkSession

p = argparse.ArgumentParser()
p.add_argument("left")
p.add_argument("right")
p.add_argument("--ignore", nargs="*", default=[], help="columns to leave out, e.g. load timestamps")
a = p.parse_args()

spark = SparkSession.builder.getOrCreate()
left, right = (spark.read.format("delta").load(t) for t in (a.left, a.right))
cols = [c for c in left.columns if c not in a.ignore]
if sorted(cols) != sorted(c for c in right.columns if c not in a.ignore):
    print(f"columns differ: {left.columns} vs {right.columns}")
    sys.exit(1)
left, right = left.select(cols), right.select(cols)
only_left, only_right = left.exceptAll(right).count(), right.exceptAll(left).count()
n_left, n_right = left.count(), right.count()
spark.stop()
print(f"left {n_left} rows, right {n_right} rows, only-left {only_left}, only-right {only_right}")
sys.exit(0 if only_left == only_right == 0 else 1)
