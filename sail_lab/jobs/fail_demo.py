"""Fails on purpose (bad column), to show how a failed run looks in the CLI, logs and Grafana."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.range(10).select("no_such_column").show()
