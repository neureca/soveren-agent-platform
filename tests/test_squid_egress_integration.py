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
from soveren_agent_platform.sandbox import (
    CommandResult,
    DockerEgressHostRelay,
    SandboxEgressUpstream,
    SandboxSpec,
    SubprocessDockerCommandRunner,
)
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


_DELAYED_FIREWALL = r'''
import os, subprocess, sys, time
payload = sys.stdin.buffer.read()
open('/tmp/ready', 'w').close()
print('ready', flush=True)
deadline = time.monotonic() + 10
while not os.path.exists('/tmp/apply'):
    if time.monotonic() > deadline:
        sys.exit(2)
    time.sleep(.02)
sys.exit(subprocess.run(['iptables-restore', '--wait', '5', '--noflush'], input=payload).returncode)
'''


class DelayedFirewallRunner:
    """Execute real Docker operations; inject only CLI/response failure boundaries."""

    def __init__(self) -> None:
        self.base = SubprocessDockerCommandRunner()
        self.mode: str | None = None
        self.started = asyncio.Event()
        self.jobs: list[asyncio.Task[CommandResult]] = []
        self.mutations = 0
        self.fail_cleanup_once = False

    async def run(self, args: list[str], *, input_data: bytes | None = None) -> CommandResult:
        if self.fail_cleanup_once and args[1:3] == ["rm", "-f"]:
            self.fail_cleanup_once = False
            return CommandResult(returncode=1, stderr="injected lost cleanup response")
        if "iptables-restore" not in args or input_data is None:
            return await self.base.run(args, input_data=input_data)
        self.mutations += 1
        mode, self.mode = self.mode, None
        if mode is None:
            return await self.base.run(args, input_data=input_data)
        delayed = list(args)
        delayed[delayed.index("--entrypoint") + 1] = "python3"
        # Preserve actual name, ownership, init, namespace, caps and stdin payload.
        delayed = delayed[:-3] + ["-u", "-c", _DELAYED_FIREWALL]
        job = asyncio.create_task(self.base.run(delayed, input_data=input_data))
        self.jobs.append(job)
        name = args[args.index("--name") + 1]
        for _ in range(100):
            ready = await self.base.run(["docker", "exec", name, "test", "-f", "/tmp/ready"])
            if ready.returncode == 0:
                break
            if job.done():
                raise AssertionError(f"delayed helper exited before ready: {job.result()}")
            await asyncio.sleep(.02)
        else:
            raise AssertionError("delayed helper did not become ready")
        self.started.set()
        if mode == "lost_reply":
            return CommandResult(returncode=1, stderr="injected lost apply response")
        try:
            return await job
        finally:
            if not job.done():
                job.cancel()
                await asyncio.gather(job, return_exceptions=True)


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


def test_host_relay_identity_reconciles_on_acquire_rotation_failure_and_recovery(tmp_path, monkeypatch, caplog):
    """Actual acquire → Squid rotation → Linux INPUT → host HTTP parent.

    Refuse to run on a daemon hosting managed workloads: rotation is deliberately
    host-wide. Every fixture endpoint and every firewall rule is removed below.
    """
    from soveren_agent_platform.sessions import sandboxing

    if _docker("ps", "-aq", "--filter", "label=soveren.egress=true") or _docker(
        "ps", "-q", "--filter", "label=soveren.runtime=docker"
    ):
        pytest.skip("host relay oracle needs a Docker daemon without managed workloads")
    image = os.environ["SOVEREN_TEST_EGRESS_IMAGE"]
    monkeypatch.setattr(sandboxing, "DEFAULT_EGRESS_IMAGE", image)
    suffix = uuid.uuid4().hex[:12]
    network = f"soveren-host-route-{suffix}"
    proxy = f"soveren-host-squid-{suffix}"
    origin = f"soveren-host-origin-{suffix}"
    parent = f"soveren-host-parent-{suffix}"
    listener = f"soveren-host-listener-{suffix}"
    old_identity_holder = f"soveren-host-old-identity-{suffix}"
    sandbox_image = f"soveren-host-sandbox:{suffix}"
    gateway = _docker("network", "inspect", "bridge", "-f", "{{(index .IPAM.Config 0).Gateway}}")
    port = 30000 + int(suffix[:4], 16) % 20000
    relay = DockerEgressHostRelay(proxy_url=f"http://{gateway}:{port}")
    boot_chain = "SVR-BOOT-" + suffix
    build = subprocess.run(
        ["docker", "build", "-t", sandbox_image, "-"],
        input=f"FROM {image}\nENTRYPOINT []\n", text=True, capture_output=True, timeout=90,
    )
    assert build.returncode == 0, build.stderr
    _docker("network", "create", "--subnet", "203.0.113.0/24", network)
    app = AgentPlatformApp(db_path=tmp_path / "host.db", bootstrap_storage=False)

    async def credentials(tenant_id: str) -> ExistingCodexCredentials:
        return ExistingCodexCredentials()

    runtime = app.configure_sandboxed_codex(
        credentials_for_tenant=credentials, model="host-route-test",
        egress_upstreams=(
            SandboxEgressUpstream(proxy_url=relay.proxy_url, destination_hosts=("selected.provider.example",)),
            SandboxEgressUpstream(proxy_url="http://203.0.113.12:8080", destination_hosts=("other.provider.example",)),
        ),
        egress_host_relays=(relay,),
    )
    assert isinstance(runtime, _DefaultSandboxedCodexRuntime)
    manager = runtime._sandbox_manager
    assert manager.egress is not None
    manager.egress = replace(manager.egress, container_name=proxy, public_network=network,
                             internal_network=f"soveren-host-internal-{suffix}")
    # This oracle tests egress, not credential provisioning. Keep its network
    # acquisition path intact while leaving the unrelated broker uncomposed.
    manager.credential_broker = None
    manager._credential_broker_manager = None
    manager.recover_orphaned_sandboxes = False
    spec = SandboxSpec(tenant_id=suffix, conversation_id="relay-oracle", image=sandbox_image,
                       network=manager.egress.internal_network, disk_limit=None)
    handle = None

    def firewall(*args, check=True):
        return _docker("run", "--rm", "--network", "host", "--cap-drop", "ALL", "--cap-add", "NET_ADMIN",
                       "--entrypoint", "iptables", image, "-w", "5", *args, check=check)

    def request(destination, tunnel=False):
        assert handle is not None
        return _request(handle.id, proxy, destination, tunnel)

    try:
        # Stand in for the operator's permanent boot guard, without changing
        # any global policy or unrelated host service. Runtime must remain
        # narrow while overriding this endpoint's default deny.
        initial_input_policy = firewall("-S", "INPUT").splitlines()[0]
        firewall("-N", boot_chain)
        firewall("-A", boot_chain, "-j", "DROP")
        firewall("-I", "INPUT", "1", "-d", gateway + "/32", "-p", "tcp", "--dport", str(port),
                 "-j", boot_chain)
        _docker("run", "-d", "--name", origin, "--network", network, "--ip", "203.0.113.10",
                "--network-alias", "selected.provider.example", "--network-alias", "direct.provider.example",
                "--network-alias", "other.provider.example", "--entrypoint", "python3", image,
                "-u", "-c", _SERVER, "direct", "80", "443")
        _docker("run", "-d", "--name", parent, "--network", network, "--ip", "203.0.113.12",
                "--entrypoint", "python3", image, "-u", "-c", _SERVER, "parent-b", "8080")
        _docker("run", "-d", "--name", listener, "--network", "host", "--entrypoint", "python3", image,
                "-u", "-c", _SERVER.replace('"0.0.0.0"', repr(gateway)), "host-parent", str(port))
        for container in (origin, parent, listener):
            _wait_ready(container)
        caplog.set_level("INFO", logger="soveren_agent_platform.sandbox.docker")
        handle = asyncio.run(manager.acquire(spec))
        old_id = _docker("inspect", "-f", "{{.Id}}", proxy)
        old_identity = json.loads(_docker("inspect", "-f", "{{json .NetworkSettings.Networks}}", proxy))[network]
        assert request("selected.provider.example") == {"status": 200, "body": "host-parent"}
        assert request("selected.provider.example", True) == {"status": 200, "body": "host-parent"}
        assert any(getattr(record, "available", None) is True for record in caplog.records)
        # A neighboring container on the same public uplink cannot use the relay.
        probe = "import socket,sys; socket.create_connection((sys.argv[1],int(sys.argv[2])),timeout=1)"
        denied = subprocess.run(["docker", "exec", origin, "python3", "-c", probe, gateway, str(port)],
                                capture_output=True, timeout=5)
        assert denied.returncode != 0
        assert request(gateway)["status"] == 403

        async def cancellation_and_failure_checks():
            from soveren_agent_platform.sandbox import docker as docker_module

            original_runner = manager.runner
            delayed = DelayedFirewallRunner()
            manager.runner = delayed
            helper = manager._host_relay_helper_name(relay)
            initial_rules = firewall("-S", relay.chain)
            extra_handle = None
            try:
                delayed.mode = "delay"
                cancelled = asyncio.create_task(manager.acquire(replace(spec, conversation_id="cancelled")))
                await asyncio.wait_for(delayed.started.wait(), timeout=5)
                assert cancelled.cancel("first cancellation")
                # Let the first cancellation reach the writer before sending
                # another: back-to-back cancel() calls coalesce into one delivery.
                await asyncio.sleep(0)
                assert not cancelled.done()
                assert manager._egress_lock.locked()
                assert cancelled.cancel("second cancellation")
                await asyncio.sleep(0)
                assert not cancelled.done()
                assert manager._egress_lock.locked()
                next_writer = asyncio.create_task(manager.acquire(replace(
                    spec, tenant_id=spec.tenant_id + "-next", conversation_id="next",
                )))
                await asyncio.sleep(.1)
                assert not cancelled.done()
                assert not next_writer.done()
                assert delayed.mutations == 1
                assert request("selected.provider.example") == {"status": 200, "body": "host-parent"}
                assert firewall("-S", relay.chain) == initial_rules
                _docker("exec", helper, "touch", "/tmp/apply")
                with pytest.raises(asyncio.CancelledError, match="second cancellation"):
                    await cancelled
                extra_handle = await asyncio.wait_for(next_writer, timeout=10)
                assert delayed.mutations == 2
                assert not _docker("ps", "-aq", "--filter", f"name=^/{helper}$")
                assert firewall("-S", relay.chain) == initial_rules
                await manager.destroy(extra_handle)
                extra_handle = None

                # Lose apply response and fail cleanup with an actual owned
                # helper left alive. Retry must terminate it before any new write.
                delayed.mode = "lost_reply"
                delayed.fail_cleanup_once = True
                with pytest.raises(BaseExceptionGroup, match="helper cleanup failed"):
                    await manager.acquire(spec)
                assert _docker("inspect", "-f", "{{.State.Running}}", helper) == "true"
                assert firewall("-S", relay.chain) == initial_rules
                assert request("selected.provider.example") == {"status": 200, "body": "host-parent"}
                await manager.acquire(spec)
                assert not _docker("ps", "-aq", "--filter", f"name=^/{helper}$")
                assert firewall("-S", relay.chain) == initial_rules

                # Timeout cleans the Docker helper, not merely its host CLI.
                delayed.mode = "delay"
                delayed.started.clear()
                with monkeypatch.context() as bounded:
                    # Include Docker inspect/readiness latency, but expire before
                    # the delayed helper's ten-second self-deadline. Assert that
                    # the actual privileged mutation was reached, not just setup.
                    bounded.setattr(docker_module, "HOST_RELAY_UPDATE_TIMEOUT_S", 4.0)
                    with pytest.raises(TimeoutError):
                        await manager.acquire(spec)
                assert not _docker("ps", "-aq", "--filter", f"name=^/{helper}$")
                assert firewall("-S", relay.chain) == initial_rules
                assert delayed.started.is_set() and delayed.mode is None
                await manager.acquire(spec)

                with pytest.raises(RuntimeError, match="could not be reconciled"):
                    await manager._write_host_relay_chain(relay, allow="-p INVALID_PROTOCOL")
                assert firewall("-S", relay.chain) == initial_rules

                _docker("run", "-d", "--name", helper, "--network", "none", "--entrypoint", "sleep",
                        image, "infinity")
                try:
                    with pytest.raises(RuntimeError, match="outside this relay policy"):
                        await manager.acquire(spec)
                    assert _docker("inspect", "-f", "{{.State.Running}}", helper) == "true"
                    assert firewall("-S", relay.chain) == initial_rules
                finally:
                    _docker("rm", "-f", helper)
                await manager.acquire(spec)
                hooks = [line for line in firewall("-S", "INPUT").splitlines() if relay.chain in line]
                assert len(hooks) == 1
                assert request("selected.provider.example") == {"status": 200, "body": "host-parent"}
            finally:
                if extra_handle is not None:
                    await manager.destroy(extra_handle)
                _docker("rm", "-f", helper, check=False)
                await asyncio.gather(*delayed.jobs, return_exceptions=True)
                manager.runner = original_runner

        asyncio.run(cancellation_and_failure_checks())

        asyncio.run(manager.stop(handle))
        # Reserve the old uplink address after detach so actual replacement must
        # use a different IP/MAC; do not rely on Docker's allocator happening to
        # choose a new address. The holder cannot impersonate the old MAC.
        _docker("network", "disconnect", network, proxy)
        _docker("run", "-d", "--name", old_identity_holder, "--network", network,
                    "--ip", old_identity["IPAddress"], "--mac-address", "02:00:00:00:00:02",
                    "--entrypoint", "sleep", image, "infinity")
        # Simulate a control-plane crash with a delayed old-generation writer.
        # Rotation must kill this owned helper before revoking/replacing Squid.
        helper = manager._host_relay_helper_name(relay)
        old_rules = firewall("-S", relay.chain).splitlines()
        old_payload = (f"*filter\n:{relay.chain} - [0:0]\n-F {relay.chain}\n"
                       + "\n".join(line for line in old_rules if line.startswith("-A ")) + "\nCOMMIT\n")
        _docker("run", "-d", "--name", helper, "--init", "--network", "host", "--cap-drop", "ALL",
                "--cap-add", "NET_ADMIN", "--label", "soveren.managed=true",
                "--label", f"soveren.host_relay_helper={relay.chain}", "--entrypoint", "python3", image,
                "-u", "-c", _DELAYED_FIREWALL.replace("payload = sys.stdin.buffer.read()",
                                                       "payload = sys.argv[1].encode()"), old_payload)
        _wait_ready(helper)
        # Actual routing-policy replacement before sandbox resume.
        manager.egress = replace(manager.egress, upstreams=(
            *manager.egress.upstreams,
            SandboxEgressUpstream(proxy_url=relay.proxy_url, destination_hosts=("extra.provider.example",)),
        ))
        handle = asyncio.run(manager.acquire(spec))
        new_identity = json.loads(_docker("inspect", "-f", "{{json .NetworkSettings.Networks}}", proxy))[network]
        assert _docker("inspect", "-f", "{{.Id}}", proxy) != old_id
        assert not _docker("ps", "-aq", "--filter", f"name=^/{helper}$")
        assert new_identity["IPAddress"] != old_identity["IPAddress"]
        assert new_identity["MacAddress"] != old_identity["MacAddress"]
        assert request("selected.provider.example") == {"status": 200, "body": "host-parent"}
        denied = subprocess.run(
            ["docker", "exec", old_identity_holder, "python3", "-c", probe, gateway, str(port)],
            capture_output=True, timeout=5,
        )
        assert denied.returncode != 0
        # Also prove acquire repairs firewall drift without replacing Squid.
        asyncio.run(manager._write_host_relay_chain(relay, allow=(
            f"-s {old_identity['IPAddress']}/32 -m mac --mac-source 02:00:00:00:00:01"
        )))
        asyncio.run(manager.acquire(spec))
        rules = firewall("-S", relay.chain)
        assert new_identity["IPAddress"] + "/32" in rules
        assert new_identity["MacAddress"].lower() in rules.lower()
        assert "02:00:00:00:00:01" not in rules
        assert request("selected.provider.example") == {"status": 200, "body": "host-parent"}

        _docker("stop", "-t", "0", listener)
        caplog.clear()
        # A failed parent probe must not turn acquire into global failure.
        handle = asyncio.run(manager.acquire(spec))
        assert any(getattr(record, "available", None) is False for record in caplog.records)
        for tunnel in (False, True):
            before = _docker("logs", origin), _docker("logs", parent)
            failed = request("selected.provider.example", tunnel)
            assert failed.get("status") == 503 or "503" in failed.get("error", ""), failed
            assert (_docker("logs", origin), _docker("logs", parent)) == before
            assert request("direct.provider.example", tunnel) == {"status": 200, "body": "direct"}
            assert request("other.provider.example", tunnel) == {"status": 200, "body": "parent-b"}
        _docker("start", listener)
        _wait_ready(listener)
        handle = asyncio.run(manager.acquire(spec))
        deadline = time.monotonic() + 20
        while request("selected.provider.example") != {"status": 200, "body": "host-parent"}:
            assert time.monotonic() < deadline
            time.sleep(0.5)
        # Removing the managed host policy revokes access rather than leaving a
        # stale allow alive; other routes continue on the new generation.
        asyncio.run(manager.stop(handle))
        manager.egress = replace(manager.egress, host_relays=())
        handle = asyncio.run(manager.acquire(spec))
        assert "ACCEPT" not in firewall("-S", relay.chain)
        before = tuple(_docker("logs", container) for container in (origin, parent, listener))
        assert request("selected.provider.example").get("status") in (502, 503)
        assert tuple(_docker("logs", container) for container in (origin, parent, listener)) == before
        assert request("direct.provider.example") == {"status": 200, "body": "direct"}
        assert request("other.provider.example") == {"status": 200, "body": "parent-b"}
        assert firewall("-S", "INPUT").splitlines()[0] == initial_input_policy
        assert firewall("-S", boot_chain) == f"-N {boot_chain}\n-A {boot_chain} -j DROP"
    finally:
        if handle is not None:
            asyncio.run(manager.destroy(handle))
        asyncio.run(app.stop())
        for container in (proxy, origin, parent, listener, old_identity_holder,
                          manager._host_relay_helper_name(relay)):
            _docker("rm", "-f", container, check=False)
        _docker("network", "rm", network, check=False)
        # Leave no INPUT guard on the daemon after the offline test.
        firewall("-D", "INPUT", "-d", gateway + "/32", "-p", "tcp", "--dport", str(port), "-j", relay.chain,
                 check=False)
        firewall("-F", relay.chain, check=False)
        firewall("-X", relay.chain, check=False)
        firewall("-D", "INPUT", "-d", gateway + "/32", "-p", "tcp", "--dport", str(port), "-j", boot_chain,
                 check=False)
        firewall("-F", boot_chain, check=False)
        firewall("-X", boot_chain, check=False)
        _docker("image", "rm", sandbox_image, check=False)
