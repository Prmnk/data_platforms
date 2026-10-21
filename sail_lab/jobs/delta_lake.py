"""Delta Lake lifecycle on MinIO: create, append, replaceWhere, MERGE, UPDATE, DELETE, time travel."""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from saillab.sdk import report, timed

p = argparse.ArgumentParser()
p.add_argument("--path", default="s3://lake/delta/accounts")
p.add_argument("--rows", type=int, default=100_000)
a = p.parse_args()

spark = SparkSession.builder.getOrCreate()


def accounts(start, n, balance_seed):
    return spark.range(start, start + n).select(
        F.col("id").alias("account_id"),
        (F.col("id") % 4).cast("int").alias("region"),
        F.round(F.rand(seed=balance_seed) * 1000, 2).alias("balance"),
        F.lit("active").alias("state"),
    )


def version_count():
    return spark.read.format("delta").load(a.path).count()


with timed("create"):  # v0
    accounts(0, a.rows, 1).write.format("delta").mode("overwrite").partitionBy("region").save(a.path)
print("v0 rows:", version_count())

with timed("append"):  # v1
    accounts(a.rows, a.rows // 10, 2).write.format("delta").mode("append").save(a.path)
print("v1 rows:", version_count())

with timed("replace_where"):  # v2: rewrite only region 0
    (accounts(0, a.rows, 3).where("region = 0").withColumn("state", F.lit("rebalanced"))
     .write.format("delta").mode("overwrite").option("replaceWhere", "region = 0").save(a.path))

spark.sql(f"CREATE TABLE IF NOT EXISTS accounts USING delta LOCATION '{a.path}'")

# Upserts: odd ids get a deposit, plus brand-new accounts.
updates = (spark.range(0, a.rows * 2, 997)
           .select(F.col("id").alias("account_id"), (F.col("id") % 4).cast("int").alias("region"),
                   F.lit(50.0).alias("amount")))
updates.createOrReplaceTempView("updates")
with timed("merge"):  # v3
    spark.sql("""
        MERGE INTO accounts t USING updates s ON t.account_id = s.account_id
        WHEN MATCHED THEN UPDATE SET balance = t.balance + s.amount
        WHEN NOT MATCHED THEN INSERT (account_id, region, balance, state)
                              VALUES (s.account_id, s.region, s.amount, 'new')
    """)
with timed("update"):  # v4
    spark.sql("UPDATE accounts SET state = 'vip' WHERE balance > 990")
with timed("delete"):  # v5
    spark.sql("DELETE FROM accounts WHERE state = 'new' AND region = 3")

spark.sql("SELECT state, count(*) AS accounts, CAST(sum(balance) AS decimal(14,2)) AS balance "
          "FROM accounts GROUP BY state ORDER BY state").show()

v0 = spark.read.format("delta").option("versionAsOf", 0).load(a.path).count()
latest = spark.read.format("delta").load(a.path).count()
print(f"time travel: version 0 had {v0} rows, latest has {latest}")
report(rows_v0=v0, rows_latest=latest)
spark.stop()
