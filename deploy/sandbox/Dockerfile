FROM node:22-bookworm-slim@sha256:53ada149d435c38b14476cb57e4a7da73c15595aba79bd6971b547ceb6d018bf

ARG CODEX_VERSION=0.143.0

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git python3 ripgrep \
    && npm install --global "@openai/codex@${CODEX_VERSION}" \
    && npm cache clean --force \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 --shell /bin/bash soveren \
    && mkdir -p /workspace /codex-home \
    && chown -R 10001:10001 /workspace /codex-home

ENV HOME=/home/soveren
ENV CODEX_HOME=/codex-home

USER 10001:10001
WORKDIR /workspace

HEALTHCHECK --interval=30s --timeout=10s --retries=3 CMD ["codex", "--version"]
CMD ["sleep", "infinity"]
