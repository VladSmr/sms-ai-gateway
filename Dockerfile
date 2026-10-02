# SMS -> GigaChat gateway image.
#
# Base image: python:3.14.7-slim (pinned to the exact interpreter version the
# project is developed and tested against; its Debian base also ships a
# recent libsqlite3, which matters for the Phase 2 sqlite-vec extension).
FROM python:3.14.7-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1
# JSON logs are mirrored to stdout AND to /app/data/logs/app.log.  /app/data
# is backed by the app_data volume in docker-compose, so log files survive
# container restarts (stdout alone would already be rotated by the Docker
# json-file driver; the file copy is for host-side inspection/backup).
ENV LOG_FILE=/app/data/logs/app.log

WORKDIR /app

# Install dependencies first to leverage Docker layer caching.
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Russian trusted root CA (NUTS Mintsifry): both GigaChat endpoints
# (api.giga.chat and ngw.devices.sberbank.ru) present certificates chained
# to this root. Two installations are needed:
#   1. the system store (update-ca-certificates) - for tools that read it;
#   2. the certifi bundle - httpx (used by the GigaChat client) trusts the
#      certifi CA file, NOT the system store, so appending the root there
#      is what actually makes TLS verification pass.
# TLS verification is never disabled anywhere in the app.
COPY docker/russian_trusted_root_ca_pem.crt /usr/local/share/ca-certificates/russian-trusted-root-ca.crt
RUN update-ca-certificates \
    && cat /usr/local/share/ca-certificates/russian-trusted-root-ca.crt \
        >> "$(python -c 'import certifi; print(certifi.where())')"

# Application code only: tests, local config and data stay out of the image.
COPY app/ /app/app/

# Non-root runtime user (uid/gid 10001).
RUN useradd --create-home --uid 10001 appuser \
    && mkdir -p /app/data /app/logs \
    && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

# create_app() is a FastAPI application factory, hence "--factory".
CMD ["uvicorn", "app.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
