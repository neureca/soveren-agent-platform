FROM python:3.12-alpine@sha256:6d43704baacd1bfbe7c295d7f13079d5d8104ed33568873133f8fc69980419df

ARG AIOHTTP_VERSION=3.14.1

RUN apk add --no-cache ca-certificates \
    && pip install --no-cache-dir "aiohttp==${AIOHTTP_VERSION}" \
    && adduser -D -u 10001 -h /nonexistent -s /sbin/nologin soveren

COPY deploy/sandbox/credential_broker.py /opt/soveren/credential_broker.py

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

USER 10001:10001
EXPOSE 8080

HEALTHCHECK --interval=2s --timeout=3s --retries=15 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=2).read()"

ENTRYPOINT ["python", "/opt/soveren/credential_broker.py"]
