FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 CASS_DRIVER_NO_CYTHON=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# sources are bind-mounted at /app by docker-compose
