# syntax=docker/dockerfile:1.6
# The cluster and the scheduler (which hosts the drivers) are built from one file so they
# always share the exact same Spark, Delta, Hadoop-AWS and Python versions. The last stage
# is the Sail server for engine: sail jobs.
ARG SPARK_IMAGE=apache/spark:3.5.8-scala2.12-java17-python3-ubuntu

# ---------------------------------------------------------------- spark (master/workers)
FROM ${SPARK_IMAGE} AS spark
USER root

ARG DELTA_VERSION=3.3.2
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG MAVEN=https://repo1.maven.org/maven2

# Bake jars into the image instead of resolving --packages on every run.
ADD --chmod=644 ${MAVEN}/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar /opt/spark/jars/

# Python bindings for Delta (pyspark itself ships with the image under /opt/spark/python).
RUN pip3 install --no-cache-dir --no-deps delta-spark==${DELTA_VERSION} \
 && mkdir -p /tmp/spark-work && chown spark:spark /tmp/spark-work

COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
ENV SPARK_WORKER_DIR=/tmp/spark-work
USER spark

# ---------------------------------------------------------------- scheduler + api
FROM spark AS scheduler
USER root
WORKDIR /opt/sparksched

COPY requirements.txt docker/requirements-sail-client.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt -r requirements-sail-client.txt

COPY pyproject.toml .
COPY scheduler ./scheduler
RUN pip3 install --no-cache-dir --no-deps . && mkdir -p /var/log/sparksched
COPY jobs ./jobs
COPY examples ./examples

# PYTHONPATH makes /opt/spark's pyspark importable by plain python3, which runs Sail jobs.
ENV PYTHONUNBUFFERED=1 \
    LOG_DIR=/var/log/sparksched \
    SPARK_SUBMIT=/opt/spark/bin/spark-submit \
    PYTHONPATH=/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
CMD ["python3", "-m", "scheduler", "loop"]

# ---------------------------------------------------------------- sail (Spark Connect server)
# Sail (https://github.com/lakehq/sail): a Rust engine speaking the Spark Connect protocol.
# Python UDFs run inside the server: it unpickles them with pyspark, so it needs the client's
# Python minor version (the Spark image's 3.10) and the same PySpark and pandas/pyarrow,
# the same rule as Spark's driver/executor Python.
FROM python:3.10-slim AS sail
ARG SAIL_VERSION=0.7.2
ARG PYSPARK_VERSION=3.5.8
COPY docker/requirements-sail-client.txt /tmp/
RUN pip install --no-cache-dir "pysail==${SAIL_VERSION}" "pyspark==${PYSPARK_VERSION}" \
    -r /tmp/requirements-sail-client.txt
EXPOSE 50051
CMD ["sail", "spark", "server", "--ip", "0.0.0.0", "--port", "50051"]
