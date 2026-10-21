"""Same computation as a built-in expression, Python UDF, pandas UDF and mapInArrow, timed."""
import argparse

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from saillab.sdk import report, timed

p = argparse.ArgumentParser()
p.add_argument("--rows", type=int, default=500_000)
a = p.parse_args()

spark = SparkSession.builder.getOrCreate()
df = spark.range(a.rows).select((F.col("id") % 1000).cast("double").alias("x"))


def run(name, frame):
    with timed(name):
        total = frame.agg(F.sum("y")).collect()[0][0]
    print(f"  {name}: sum = {total:,.0f}")


run("builtin", df.select((F.col("x") * 2 + 1).alias("y")))

py_udf = F.udf(lambda x: x * 2 + 1, "double")                  # row at a time, pickled
run("python_udf", df.select(py_udf("x").alias("y")))


@F.pandas_udf("double")                                           # vectorized, Arrow batches
def pandas_fn(x: pd.Series) -> pd.Series:
    return x * 2 + 1


run("pandas_udf", df.select(pandas_fn("x").alias("y")))


def arrow_fn(batches):                                            # whole Arrow record batches
    for b in batches:
        yield pa.RecordBatch.from_arrays([pc.add(pc.multiply(b.column(0), 2), 1)], names=["y"])


run("map_in_arrow", df.mapInArrow(arrow_fn, "y double"))


def per_group(pdf: pd.DataFrame) -> pd.DataFrame:                 # grouped map
    return pd.DataFrame({"bucket": [pdf["bucket"].iloc[0]], "p90": [pdf["x"].quantile(0.9)]})


with timed("apply_in_pandas"):
    (df.withColumn("bucket", (F.col("x") % 8).cast("int"))
       .groupBy("bucket").applyInPandas(per_group, "bucket int, p90 double").orderBy("bucket").show())

report(rows=a.rows)
spark.stop()
