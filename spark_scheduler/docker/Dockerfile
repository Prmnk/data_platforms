# syntax=docker/dockerfile:1.6
# Two targets from one file so the cluster and the scheduler (which hosts the drivers)
# always share the exact same Spark, Delta, Hadoop-AWS and Python versions.
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

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY pyproject.toml .
COPY scheduler ./scheduler
RUN pip3 install --no-cache-dir --no-deps . && mkdir -p /var/log/sparksched
COPY jobs ./jobs
COPY examples ./examples

ENV PYTHONUNBUFFERED=1 \
    LOG_DIR=/var/log/sparksched \
    SPARK_SUBMIT=/opt/spark/bin/spark-submit
CMD ["python3", "-m", "scheduler", "loop"]
