"""Helpers shared by the Delta jobs. They use only the DataFrame API and SQL, so the same
scripts run on Spark (spark-submit) and on Sail (Spark Connect), where delta-spark's
DeltaTable Python API is not available."""
from pyspark.errors import AnalysisException


def delta_table_exists(spark, path: str) -> bool:
    try:
        spark.read.format("delta").load(path).schema  # resolving the schema reads the log
        return True
    except AnalysisException:  # Spark: not a Delta table / path missing; Sail: no commit files
        return False
