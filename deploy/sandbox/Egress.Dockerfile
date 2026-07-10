FROM ubuntu/squid:6.6-24.04_edge@sha256:8a3baed477e2c282ab8aa5edad442f69873246964f225c5c2ae8364b6610963c

RUN apt-get update \
    && apt-get install -y --no-install-recommends iptables \
    && rm -rf /var/lib/apt/lists/*

COPY deploy/sandbox/squid.conf /etc/squid/squid.conf

HEALTHCHECK --interval=2s --timeout=3s --retries=15 \
    CMD timeout 2 bash -c '</dev/tcp/127.0.0.1/3128'
