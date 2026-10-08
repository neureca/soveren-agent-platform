"""Offline route oracle against the production Squid image and Docker launch path.

Run with SOVEREN_TEST_EGRESS_IMAGE set to a locally built Egress.Dockerfile image.
Documentation-range IPs stand in for public origins; no provider credentials or
external network requests are used. CONNECT tests send HTTP bytes inside the tunnel
to isolate routing from TLS/certificate behavior.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
import uuid
from dataclasses import replace

import pytest

from soveren_agent_platform.app_api import AgentPlatformApp
from soveren_agent_platform.llm.backends.sandboxed_codex import _DefaultSandboxedCodexRuntime
from soveren_agent_platform.sandbox import SandboxEgressUpstream
from soveren_agent_platform.sessions import ExistingCodexCredentials

pytestmark = pytest.mark.skipif(
    not os.environ.get("SOVEREN_TEST_EGRESS_IMAGE"),
    reason="set SOVEREN_TEST_EGRESS_IMAGE to run real Squid routing checks",
)

_SERVER = r'''
import os, socketserver, threading, sys
marker = sys.argv[1]
class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        line = self.rfile.readline().decode().strip()
        if not line:
            return
        print(marker, line, flush=True)
        while self.rfile.readline() not in (b"\r\n", b"\n", b""):
            pass
        if os.path.exists("/tmp/refuse"):
            self.wfile.write(b"HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 0\r\n\r\n")
            return
        if line.startswith("CONNECT "):
            self.wfile.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
            line = self.rfile.readline().decode().strip()
            while self.rfile.readline() not in (b"\r\n", b"\n", b""):
                pass
        body = marker.encode()
        self.wfile.write(b"HTTP/1.1 200 OK\r\nContent-Length: " + str(len(body)).encode()
                        + b"\r\nConnection: close\r\n\r\n" + body)
class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
for port in map(int, sys.argv[2:]):
    server = Server(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
print("ready", flush=True)
threading.Event().wait()
'''

_REQUEST = r'''
import http.client, json, sys
connection = http.client.HTTPConnection(sys.argv[1], 3128, timeout=15)
target, tunnel = sys.argv[2], sys.argv[3] == "1"
try:
    if tunnel:
        connection.set_tunnel(target, 443)
        connection.request("GET", "/probe", headers={"Host": target})
    else:
        connection.request("GET", "http://" + target + "/probe")
    response = connection.getresponse()
    print(json.dumps({"status": response.status, "body": response.read().decode()}))
except OSError as error:
    print(json.dumps({"error": str(error)}))
finally:
    connection.close()
'''


def _docker(*args: str, check: bool = True) -> str:
    result = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=90)
    if check and result.returncode:
        raise AssertionError(f"docker {' '.join(args[:3])} failed: {result.stderr}")
    return result.stdout.strip()


def _wait_ready(container: str) -> None:
    for _ in range(50):
        if "ready" in _docker("logs", container):
            return
        time.sleep(0.1)
    raise AssertionError(f"route fixture {container} did not start")


@pytest.fixture
def routes(tmp_path, monkeypatch):
    from soveren_agent_platform.sessions import sandboxing

    image = os.environ["SOVEREN_TEST_EGRESS_IMAGE"]
    monkeypatch.setattr(sandboxing, "DEFAULT_EGRESS_IMAGE", image)
    suffix = uuid.uuid4().hex[:12]
    network = f"soveren-route-test-{suffix}"
    origin = f"soveren-route-origin-{suffix}"
    parents = {name: f"soveren-route-{name}-{suffix}" for name in ("parent-a", "parent-b")}
    proxy = f"soveren-route-squid-{suffix}"
    _docker("network", "create", "--internal", "--subnet", "203.0.113.0/24", network)
    app = AgentPlatformApp(db_path=tmp_path / "app.db", bootstrap_storage=False)

    async def credentials(tenant_id: str) -> ExistingCodexCredentials:
        return ExistingCodexCredentials()

    runtime = app.configure_sandboxed_codex(
        credentials_for_tenant=credentials, model="route-test", max_active_sandboxes=3,
        egress_upstreams=(
            SandboxEgressUpstream(
                proxy_url="http://203.0.113.11:8080",
                destination_hosts=("selected.provider.example", "localhost", "unresolved-provider.invalid"),
            ),
            SandboxEgressUpstream(
                proxy_url="http://203.0.113.12:8080",
                destination_hosts=("chat.other-provider.example", "other-unresolved.invalid"),
            ),
        ),
    )
    assert isinstance(runtime, _DefaultSandboxedCodexRuntime)
    manager = runtime._sandbox_manager
    assert manager.egress is not None
    manager.egress = replace(manager.egress, container_name=proxy, public_network=network)
    try:
        _docker(
            "run", "-d", "--name", origin, "--network", network, "--ip", "203.0.113.10",
            "--network-alias", "selected.provider.example", "--network-alias", "direct.provider.example",
            "--network-alias", "chat.other-provider.example", "--network-alias", "child.selected.provider.example",
            "--entrypoint", "python3", image, "-u", "-c", _SERVER, "direct", "80", "443",
        )
        for index, (name, container) in enumerate(parents.items(), start=11):
            _docker(
                "run", "-d", "--name", container, "--network", network, "--ip", f"203.0.113.{index}",
                "--entrypoint", "python3", image, "-u", "-c", _SERVER, name, "8080",
            )
            _wait_ready(container)
        _wait_ready(origin)
        proxy_id = asyncio.run(manager._create_egress_container())
        configuration = asyncio.run(manager._inspect_egress_container(proxy_id))
        assert configuration.upstreams == manager.egress.upstreams
        manager._validate_egress_routing_support(configuration)
        asyncio.run(manager._wait_for_egress_health(proxy_id))
        # Inspect the actual generated config and use Squid's own parser.
        _docker("exec", proxy, "squid", "-k", "parse", "-f", "/run/soveren-squid.conf")
        rendered = _docker("exec", proxy, "cat", "/run/soveren-squid.conf")
        assert "never_direct allow upstream_destination" in rendered
        assert "http_access deny blocked_destination" in rendered
        assert rendered.count("cache_peer ") == 2
        for index in range(2):
            assert f"cache_peer_access selected_upstream_{index} deny all" in rendered
        yield origin, parents, proxy
    finally:
        asyncio.run(app.stop())
        for container in (proxy, *parents.values(), origin):
            _docker("rm", "-f", container, check=False)
        _docker("network", "rm", network)


def _request(origin: str, proxy: str, destination: str, tunnel: bool) -> dict:
    return json.loads(_docker("exec", origin, "python3", "-c", _REQUEST, proxy, destination, str(int(tunnel))))


@pytest.mark.parametrize("tunnel", [False, True], ids=["http", "connect"])
@pytest.mark.parametrize("failed_group", ["parent-a", "parent-b"])
def test_selected_direct_and_fail_closed_per_group_through_real_squid(routes, tunnel, failed_group):
    origin, parents, proxy = routes
    for selected, expected in (
        ("selected.provider.example", "parent-a"), ("SELECTED.PROVIDER.EXAMPLE", "parent-a"),
        ("selected.provider.example.", "parent-a"), ("chat.other-provider.example", "parent-b"),
    ):
        result = _request(origin, proxy, selected, tunnel)
        assert result == {"status": 200, "body": expected}, result
    assert "direct GET" not in _docker("logs", origin)
    for name, parent in parents.items():
        assert name + " " in _docker("logs", parent)

    parent = parents[failed_group]
    other_group = "parent-b" if failed_group == "parent-a" else "parent-a"
    destinations = {"parent-a": "selected.provider.example", "parent-b": "chat.other-provider.example"}
    destination = destinations[failed_group]
    other_destination = destinations[other_group]
    _docker("exec", parent, "touch", "/tmp/refuse")
    other_before = _docker("logs", parents[other_group])
    refused = _request(origin, proxy, destination, tunnel)
    if tunnel:
        assert "Tunnel connection failed:" in refused.get("error", ""), refused
    else:
        assert refused.get("status") == 407, refused
    assert "direct GET" not in _docker("logs", origin)
    assert _docker("logs", parents[other_group]) == other_before, "refusal used another group's proxy"
    _docker("exec", parent, "rm", "/tmp/refuse")
    for direct in ("direct.provider.example", "child.selected.provider.example", "203.0.113.10"):
        assert _request(origin, proxy, direct, tunnel) == {"status": 200, "body": "direct"}

    _docker("stop", "-t", "0", parent)
    before = _docker("logs", origin)
    other_before = _docker("logs", parents[other_group])
    for _ in range(2):
        failed = _request(origin, proxy, destination, tunnel)
        if tunnel:
            assert "Tunnel connection failed: 503" in failed.get("error", ""), failed
        else:
            assert failed.get("status") == 503, failed
    assert _docker("logs", origin) == before, "upstream failure reached the direct destination"
    assert _docker("logs", parents[other_group]) == other_before, "upstream failure used another group's proxy"
    assert _request(origin, proxy, other_destination, tunnel) == {"status": 200, "body": other_group}
    assert _request(origin, proxy, "direct.provider.example", tunnel) == {"status": 200, "body": "direct"}

    _docker("start", parent)
    _wait_ready(parent)
    # Squid temporarily marks an unreachable parent dead; give its bounded retry a chance.
    deadline = time.monotonic() + 20
    while True:
        restored = _request(origin, proxy, destination, tunnel)
        if restored == {"status": 200, "body": failed_group}:
            break
        assert time.monotonic() < deadline, restored
        time.sleep(0.5)


@pytest.mark.parametrize("tunnel", [False, True], ids=["http", "connect"])
def test_private_destinations_remain_denied_even_when_selected(routes, tunnel):
    origin, parents, proxy = routes
    before = {name: _docker("logs", container) for name, container in parents.items()}
    for destination in (
        "127.0.0.1", "169.254.169.254", "10.0.0.1", "localhost", "unresolved-provider.invalid",
        "other-unresolved.invalid",
    ):
        result = _request(origin, proxy, destination, tunnel)
        if tunnel:
            assert "Tunnel connection failed: 403" in result.get("error", ""), result
        else:
            assert result.get("status") == 403, result
    assert {name: _docker("logs", container) for name, container in parents.items()} == before, (
        "private destination was forwarded to a parent"
    )
