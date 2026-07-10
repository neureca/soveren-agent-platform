#!/usr/bin/env bash
set -euo pipefail

compose_file="deploy/sandbox/compose.yaml"
image="soveren-codex-sandbox:test"
egress_image="soveren-sandbox-egress:test"

docker compose -f "$compose_file" config >/dev/null
docker build -f deploy/sandbox/Dockerfile -t "$image" .
docker build -f deploy/sandbox/Egress.Dockerfile -t "$egress_image" .
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
import os

from soveren_agent_platform.sandbox import DockerEgressSpec, DockerSandboxRuntime, SandboxSpec


async def main() -> None:
    runtime = DockerSandboxRuntime(
        egress=DockerEgressSpec(image="soveren-sandbox-egress:test"),
        recover_orphaned_sandboxes=True,
    )
    handle = await runtime.acquire(SandboxSpec(
        tenant_id="smoke-tenant",
        image="soveren-codex-sandbox:test",
        network="soveren-sandbox-egress",
        disk_limit=os.environ.get("SOVEREN_SMOKE_DISK_LIMIT") or None,
        env={
            "https_proxy": "http://soveren-sandbox-egress:3128",
            "http_proxy": "http://soveren-sandbox-egress:3128",
        },
    ))
    network = handle.metadata["network"]
    gateway = (await runtime.runner.run([
        "docker", "network", "inspect", "-f", "{{(index .IPAM.Config 0).Gateway}}", network,
    ])).stdout.strip()
    peer = await runtime.runner.run([
        "docker", "run", "-d", "--name", "soveren-sandbox-smoke-peer",
        "--network", network, "soveren-codex-sandbox:test", "sh", "-c",
        "printf peer-secret > /tmp/probe && cd /tmp && python3 -m http.server 18081",
    ])
    if peer.returncode != 0:
        raise RuntimeError(peer.stderr)
    try:
        await runtime.run_command(handle, [
            "sh",
            "-ec",
            """
            test "$(curl -sS -o /dev/null -w '%{http_code}' https://api.openai.com/v1/models)" = 401
            test "$(curl -sS -o /dev/null -w '%{http_code}' http://127.0.0.1:80)" = 403
            test "$(curl -sS -o /dev/null -w '%{http_code}' http://169.254.169.254)" = 403
            if curl --noproxy '*' --connect-timeout 2 --max-time 3 -fsS \
              http://soveren-sandbox-smoke-peer:18081/probe; then exit 11; fi
            if curl --noproxy '*' --connect-timeout 2 --max-time 3 -fsS \
              http://${SOVEREN_GATEWAY}:18080/probe; then exit 12; fi
            """,
        ], env={"SOVEREN_GATEWAY": gateway})
    finally:
        await runtime.runner.run(["docker", "rm", "-f", "soveren-sandbox-smoke-peer"])
        await runtime.destroy(handle)

    failed_tenant = "smoke-failed-acquire"
    failed_network = "soveren-sandbox-egress-" + hashlib.sha256(
        failed_tenant.encode("utf-8")
    ).hexdigest()[:12]
    try:
        failed_handle = await runtime.acquire(SandboxSpec(
            tenant_id=failed_tenant,
            image="INVALID IMAGE",
            network="soveren-sandbox-egress",
            disk_limit=None,
        ))
    except RuntimeError:
        pass
    else:
        await runtime.destroy(failed_handle)
        raise AssertionError("invalid sandbox image unexpectedly started")

    network = await runtime.runner.run(["docker", "network", "inspect", failed_network])
    if network.returncode == 0:
        raise AssertionError("failed sandbox acquisition leaked its tenant network")
    containers = await runtime.runner.run([
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
