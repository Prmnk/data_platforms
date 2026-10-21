# One Python for everything: the Sail server, the submit service (which hosts the PySpark
# clients) and the optional notebook share a base stage, so Python UDFs always unpickle.
ARG PYTHON_IMAGE=python:3.10-slim

# ---------------------------------------------------------------- base: Spark Connect client
FROM ${PYTHON_IMAGE} AS base
ARG PYSPARK_VERSION=3.5.8
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
COPY docker/requirements-client.txt /tmp/
RUN pip install "pyspark==${PYSPARK_VERSION}" -r /tmp/requirements-client.txt

# ---------------------------------------------------------------- sail: the engine
# LakeSail Sail (https://github.com/lakehq/sail): a Rust engine speaking Spark Connect.
FROM base AS sail
ARG SAIL_VERSION=0.7.2
RUN pip install "pysail==${SAIL_VERSION}"
EXPOSE 50051
CMD ["sail", "spark", "server", "--ip", "0.0.0.0", "--port", "50051"]

# ---------------------------------------------------------------- lab: submit API + clients
FROM base AS lab
WORKDIR /opt/lab
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY pyproject.toml .
COPY saillab ./saillab
RUN pip install --no-deps . && mkdir -p /var/lib/lab /var/log/lab/runs
COPY jobs ./jobs
EXPOSE 8010
CMD ["python", "-m", "saillab", "api"]

# ---------------------------------------------------------------- notebook (optional profile)
FROM lab AS notebook
RUN pip install jupyterlab==4.2.5
COPY notebooks ./notebooks
EXPOSE 8888
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", \
     "--ServerApp.root_dir=/opt/lab", "--IdentityProvider.token=", "--ServerApp.password="]
