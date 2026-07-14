#!/usr/bin/env bash
set -euo pipefail

compose_file="deploy/sandbox/compose.yaml"
image="soveren-codex-sandbox:test"
egress_image="soveren-sandbox-egress:test"
broker_image="soveren-credential-broker:test"

docker compose -f "$compose_file" config >/dev/null
docker build -f deploy/sandbox/Dockerfile -t "$image" .
docker build -f deploy/sandbox/Egress.Dockerfile -t "$egress_image" .
docker build -f deploy/sandbox/CredentialBroker.Dockerfile -t "$broker_image" .
docker run --rm --entrypoint codex "$image" --version

if docker run --rm --storage-opt size=1g "$image" true >/dev/null 2>&1; then
  export SOVEREN_SMOKE_DISK_LIMIT="1g"
else
  export SOVEREN_SMOKE_DISK_LIMIT=""
  echo "Docker storage driver has no per-container quota support; skipping quota-only smoke assertion" >&2
fi

SOVEREN_EGRESS_IMAGE="$egress_image" docker compose -f "$compose_file" up -d --force-recreate
cleanup() {
  docker rm -f soveren-sandbox-smoke-peer soveren-sandbox-smoke-host >/dev/null 2>&1 || true
  docker compose -f "$compose_file" down --remove-orphans
}
trap cleanup EXIT

container_id="$(docker compose -f "$compose_file" ps -q soveren-sandbox-egress)"
for _ in $(seq 1 30); do
  if [ "$(docker inspect -f '{{.State.Health.Status}}' "$container_id")" = "healthy" ]; then
    break
  fi
  sleep 1
done
test "$(docker inspect -f '{{.State.Health.Status}}' "$container_id")" = "healthy"

docker run -d \
  --name soveren-sandbox-smoke-host \
  --network host \
  "$image" \
  sh -c 'printf host-secret > /tmp/probe && cd /tmp && python3 -m http.server 18080' \
  >/dev/null

uv run python - <<'PY'
import asyncio
import hashlib
import json
import os

from soveren_agent_platform.sandbox import (
    CredentialBrokerPolicy,
    DockerCredentialBrokerSpec,
    DockerEgressSpec,
    DockerSandboxManager,
    SandboxSpec,
)
from soveren_agent_platform.sessions import (
    CodexApiKeyCredentials,
    OpenSpec,
    SandboxedCodexAppServerBackend,
)


async def main() -> None:
    manager = DockerSandboxManager(
        egress=DockerEgressSpec(image="soveren-sandbox-egress:test"),
        credential_broker=DockerCredentialBrokerSpec(image="soveren-credential-broker:test"),
        recover_orphaned_sandboxes=True,
    )
    sandbox_spec = SandboxSpec(
        tenant_id="smoke-tenant",
        conversation_id="smoke-chat",
        image="soveren-codex-sandbox:test",
        network="soveren-sandbox-egress",
        disk_limit=os.environ.get("SOVEREN_SMOKE_DISK_LIMIT") or None,
        env={
            "https_proxy": "http://soveren-sandbox-egress:3128",
            "http_proxy": "http://soveren-sandbox-egress:3128",
        },
    )
    handle = await manager.acquire(sandbox_spec)
    try:
        broker = await manager.provision_credential_broker(
            handle,
            api_key=b"sk-smoke-provider-secret",
            policy=CredentialBrokerPolicy(),
        )
    except BaseException:
        await manager.destroy(handle)
        raise
    codex_backend = SandboxedCodexAppServerBackend(
        sandbox_manager=manager,
        sandbox_spec=sandbox_spec,
        credentials=CodexApiKeyCredentials("sk-smoke-provider-secret"),
        idle_stop_after_s=None,
    )
    try:
        opened = await codex_backend.open(OpenSpec(kind="codex_cli", cwd="/workspace"))
        if not opened.backend_session_id:
            raise AssertionError("Codex app-server did not open a brokered thread")
    except BaseException:
        await codex_backend.destroy_sandbox()
        raise
    network = handle.metadata["network"]
    gateway = (await manager.runner.run([
        "docker", "network", "inspect", "-f", "{{(index .IPAM.Config 0).Gateway}}", network,
    ])).stdout.strip()
    peer = await manager.runner.run([
        "docker", "run", "-d", "--name", "soveren-sandbox-smoke-peer",
        "--network", network, "soveren-codex-sandbox:test", "sh", "-c",
        "printf peer-secret > /tmp/probe && cd /tmp && python3 -m http.server 18081",
    ])
    if peer.returncode != 0:
        await codex_backend.destroy_sandbox()
        raise RuntimeError(peer.stderr)
    try:
        await manager.run_command(handle, [
            "sh",
            "-ec",
            """
            test "$(curl -sS -o /dev/null -w '%{http_code}' https://api.openai.com/v1/models)" = 401
            test "$(curl --noproxy '*' -sS -o /dev/null -w '%{http_code}' \
              http://soveren-credential-broker:8080/healthz)" = 204
            test "$(curl --noproxy '*' -sS -o /dev/null -w '%{http_code}' \
              -H 'content-type: application/json' \
              -H 'authorization: Bearer sandbox-controlled-value' \
              -d '{"model":"gpt-5.1-codex-mini","input":"smoke"}' \
              http://soveren-credential-broker:8080/v1/responses)" = 401
            test "$(curl --noproxy '*' -sS -o /dev/null -w '%{http_code}' \
              http://soveren-credential-broker:8080/v1/responses)" = 405
            test "$(curl --noproxy '*' -sS -o /dev/null -w '%{http_code}' \
              -X POST http://soveren-credential-broker:8080/v1/files)" = 404
            ! grep -R -F 'sk-smoke-provider-secret' /codex-home /workspace
            test "$(curl -sS -o /dev/null -w '%{http_code}' http://127.0.0.1:80)" = 403
            test "$(curl -sS -o /dev/null -w '%{http_code}' http://169.254.169.254)" = 403
            if curl --noproxy '*' --connect-timeout 2 --max-time 3 -fsS \
              http://soveren-sandbox-smoke-peer:18081/probe; then exit 11; fi
            if curl --noproxy '*' --connect-timeout 2 --max-time 3 -fsS \
              http://${SOVEREN_GATEWAY}:18080/probe; then exit 12; fi
            """,
        ], env={"SOVEREN_GATEWAY": gateway})
        if broker.base_url != "http://soveren-credential-broker:8080/v1":
            raise AssertionError("unexpected broker endpoint")
        broker_container = await manager.runner.run([
            "docker", "ps", "-q", "--filter", "label=soveren.credential_broker=true",
        ])
        broker_id = broker_container.stdout.strip()
        inspected = await manager.runner.run(["docker", "inspect", broker_id])
        if b"sk-smoke-provider-secret" in inspected.stdout.encode():
            raise AssertionError("provider key leaked into Docker metadata")
        broker_networks = await manager.runner.run([
            "docker", "inspect", "-f", "{{json .NetworkSettings.Networks}}", broker_id,
        ])
        if "soveren-sandbox-public-egress" in json.loads(broker_networks.stdout):
            raise AssertionError("credential broker joined the shared public network")
    finally:
        await manager.runner.run(["docker", "rm", "-f", "soveren-sandbox-smoke-peer"])
        await codex_backend.destroy_sandbox()

    brokers = await manager.runner.run([
        "docker", "ps", "-aq", "--filter", "label=soveren.credential_broker=true",
    ])
    if brokers.stdout.strip():
        raise AssertionError("destroying the last conversation leaked its tenant broker")

    failed_tenant = "smoke-failed-acquire"
    failed_conversation = "failed-chat"
    failed_network = "soveren-sandbox-egress-" + hashlib.sha256(
        f"{failed_tenant}\0{failed_conversation}".encode("utf-8")
    ).hexdigest()[:12]
    try:
        failed_handle = await manager.acquire(SandboxSpec(
            tenant_id=failed_tenant,
            conversation_id=failed_conversation,
            image="INVALID IMAGE",
            network="soveren-sandbox-egress",
            disk_limit=None,
        ))
    except RuntimeError:
        pass
    else:
        await manager.destroy(failed_handle)
        raise AssertionError("invalid sandbox image unexpectedly started")

    network = await manager.runner.run(["docker", "network", "inspect", failed_network])
    if network.returncode == 0:
        raise AssertionError("failed sandbox acquisition leaked its tenant network")
    containers = await manager.runner.run([
        "docker",
        "ps",
        "-aq",
        "--filter",
        "label=soveren.tenant_key=" + hashlib.sha256(failed_tenant.encode("utf-8")).hexdigest(),
    ])
    if containers.stdout.strip():
        raise AssertionError("failed sandbox acquisition leaked its tenant container")


asyncio.run(main())
PY
