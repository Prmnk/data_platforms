"""Apache Iceberg on MinIO without a catalog: write, append, dynamic partition overwrite, MERGE, time travel."""
import argparse
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from saillab.sdk import report, timed

p = argparse.ArgumentParser()
p.add_argument("--path", default="s3://lake/iceberg/events")
p.add_argument("--rows", type=int, default=200_000)
a = p.parse_args()

spark = SparkSession.builder.getOrCreate()


def events(start, n, day_offset=0):
    return spark.range(start, start + n).select(
        F.col("id").alias("event_id"),
        F.element_at(F.array(F.lit("click"), F.lit("view"), F.lit("buy")), (F.col("id") % 3 + 1).cast("int")).alias("kind"),
        F.date_add(F.lit("2026-10-01").cast("date"), (F.col("id") % 7 + day_offset).cast("int")).alias("day"),
    )


def per_day():
    return {str(r["day"]): r["count"] for r in spark.table("events").groupBy("day").count().collect()}


with timed("create"):
    events(0, a.rows).write.format("iceberg").mode("overwrite").partitionBy("day").save(a.path)
spark.sql(f"CREATE TABLE IF NOT EXISTS events USING iceberg LOCATION '{a.path}'")

time.sleep(1)
# Time travel takes a timestamp *string* (RFC 3339); epoch millis are rejected.
after_create = datetime.now(timezone.utc).isoformat()
time.sleep(1)

with timed("append"):  # a second week of events
    events(a.rows, a.rows // 2, day_offset=7).write.format("iceberg").mode("append").save(a.path)

with timed("overwrite_partitions"):  # replace only the days present in the input
    events(10_000_000, 1_000).where("day = '2026-10-01'").writeTo("events").overwritePartitions()

with timed("merge"):
    spark.sql("""
        MERGE INTO events t
        USING (SELECT event_id FROM events WHERE kind = 'buy' AND day = '2026-10-02' LIMIT 100) s
        ON t.event_id = s.event_id
        WHEN MATCHED THEN DELETE
    """)

spark.table("events").groupBy("kind").count().orderBy("kind").show()
latest = spark.table("events").count()
then = spark.read.format("iceberg").option("timestampAsOf", after_create).load(a.path).count()
then_sql = spark.sql(f"SELECT count(*) AS n FROM events TIMESTAMP AS OF '{after_create}'").collect()[0]["n"]
print(f"latest: {latest} rows; as of {after_create}: {then} rows (SQL: {then_sql})")
print("rows per day:", dict(sorted(per_day().items())))
report(rows_latest=latest, rows_after_create=then)
spark.stop()
