FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 CASS_DRIVER_NO_CYTHON=1 PROMETHEUS_MULTIPROC_DIR=/tmp/prom
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY features.py app.py ./
# metric files from a previous run must not leak into this one
CMD ["sh", "-c", "rm -rf $PROMETHEUS_MULTIPROC_DIR && mkdir -p $PROMETHEUS_MULTIPROC_DIR && exec uvicorn app:app --host 0.0.0.0 --port 8000 --workers 2"]
