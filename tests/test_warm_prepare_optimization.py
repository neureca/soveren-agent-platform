"""Production broker entrypoints, generation-aware no-op restore and stdlib admin wire."""

import asyncio
import base64
import importlib.util
import struct
import sys
from pathlib import Path

import pytest
from test_docker_credential_broker import FakeBrokerHost, _provision_test_http_binding

from soveren_agent_platform.sandbox import HttpCredentialBinding
from soveren_agent_platform.sandbox.docker_broker import DockerCredentialBrokerManager, DockerCredentialBrokerSpec

ADMIN_SPEC = importlib.util.spec_from_file_location(
    "soveren_test_stdlib_admin", Path(__file__).parents[1] / "deploy/sandbox/credential_broker_admin.py"
)
assert ADMIN_SPEC is not None and ADMIN_SPEC.loader is not None
credential_broker_admin = importlib.util.module_from_spec(ADMIN_SPEC)
ADMIN_SPEC.loader.exec_module(credential_broker_admin)

NETWORK = "soveren-sandbox-egress-conversation-a"


def setup_broker():
    host = FakeBrokerHost({NETWORK: ("172.30.1.0/24", "172.30.1.4")})
    return DockerCredentialBrokerManager(host=host, spec=DockerCredentialBrokerSpec(image="broker:test")), host


@pytest.mark.parametrize("drift", ["none", "restart", "address", "membership", "rules", "foreign"])
def test_warm_prepare_skips_only_acknowledged_unchanged_registry(drift):
    async def run():
        manager, host = setup_broker()
        await _provision_test_http_binding(manager, network=NETWORK)
        initial_updates = len(host.registry_payloads)
        original_rules = set(host.firewall_rules)
        if drift == "restart":
            host.started_at = "2026-10-16T02:00:00Z"  # same container ID, new server process
        elif drift == "address":
            host.networks[NETWORK] = ("172.30.1.0/24", "172.30.1.5")
        elif drift == "membership":
            # Add an obsolete authorized network, then remove it from authoritative discovery.
            from dataclasses import replace

            bindings = manager._bindings["a" * 64]
            key = next(iter(bindings))
            bindings[key] = replace(bindings[key], networks=frozenset({NETWORK, "obsolete"}))
            manager._synced_payloads.clear()
        elif drift == "rules":
            host.firewall_rules.clear()
        elif drift == "foreign":
            host.network_tenants[NETWORK] = "f" * 64
        if drift == "foreign":
            with pytest.raises(RuntimeError, match="ownership"):
                await manager.prepare_tenant_network("a" * 64, NETWORK)
            assert host.container_id is None  # no stale serving grants on ownership failure
            assert not manager._synced_payloads
            return
        await manager.prepare_tenant_network("a" * 64, NETWORK)
        assert len(host.registry_payloads) == initial_updates + (drift in {"restart", "address", "membership"})
        assert host.firewall_rules  # drift is repaired even when registry write is skipped
        if drift in {"none", "restart", "rules", "membership"}:
            assert host.firewall_rules == original_rules
        assert any(call[1:3] == ["network", "ls"] for call in host.calls)
        assert any("{{.State.StartedAt}}" in call for call in host.calls)
        if drift == "address":
            assert not (host.firewall_rules & original_rules)  # no stale broker-IP grants
            assert host.registry_payloads[-1]["bindings"][0]["allowed_local_ips"] == ["172.30.1.5"]

    asyncio.run(run())


def test_concurrent_prepare_rotation_and_revoke_cannot_use_an_old_ack():
    async def run():
        manager, host = setup_broker()
        handle = await _provision_test_http_binding(manager, network=NETWORK)
        binding = HttpCredentialBinding(
            name="github",
            target_origin="https://api.github.com",
            allowed_methods=("GET",),
            allowed_path_prefixes=("/repos",),
        )
        await asyncio.gather(
            manager.prepare_tenant_network("a" * 64, NETWORK),
            manager.provision_http(
                handle, tenant_key="a" * 64, conversation_key="b" * 64, credential=b"rotated-fixture", binding=binding
            ),
            manager.prepare_tenant_network("a" * 64, NETWORK),
        )
        assert len(host.registry_payloads) == 2  # first provision + forced current-token update
        assert base64.b64decode(host.registry_payloads[-1]["bindings"][0]["secret"]) == b"rotated-fixture"
        await manager.revoke_http(
            handle, tenant_key="a" * 64, conversation_key="b" * 64, name="github", scope="conversation"
        )
        assert not manager._synced_payloads
        assert host.container_id is None
        await manager.prepare_tenant_network("a" * 64, NETWORK)
        assert host.container_id is None  # no registry resurrected after revoke

    asyncio.run(run())


@pytest.mark.parametrize("failure", ["error", "lost_response", "cancel"])
def test_uncertain_rotation_invalidates_all_acks_and_decommissions(failure):
    async def run():
        manager, host = setup_broker()
        handle = await _provision_test_http_binding(manager, network=NETWORK)
        original = host._run_docker

        async def edge(args, *, input_data=None):
            if args[-1] == "/opt/soveren/credential_broker_admin.py":
                if failure == "lost_response":
                    await original(args, input_data=input_data)  # applied, response unknown
                if failure == "cancel":
                    raise asyncio.CancelledError()
                raise RuntimeError("fixture unavailable")
            return await original(args, input_data=input_data)

        host._run_docker = edge
        with pytest.raises(asyncio.CancelledError if failure == "cancel" else RuntimeError):
            await manager.provision_http(
                handle,
                tenant_key="a" * 64,
                conversation_key="b" * 64,
                credential=b"new-fixture",
                binding=HttpCredentialBinding(
                    name="github",
                    target_origin="https://api.github.com",
                    allowed_methods=("GET",),
                    allowed_path_prefixes=("/repos",),
                ),
            )
        assert not manager._synced_payloads
        assert host.container_id is None
        host._run_docker = original
        await manager.prepare_tenant_network("a" * 64, NETWORK)
        assert host.container_id is not None  # restore acknowledged pre-failure state before next use
        assert base64.b64decode(host.registry_payloads[-1]["bindings"][0]["secret"]) == b"test-secret"

    asyncio.run(run())


def test_admin_entrypoint_runs_with_stdlib_only_and_secret_in_stdin(tmp_path):
    async def run():
        path = tmp_path / "broker.sock"
        payload = b"fixture-private-token"
        received = []

        async def serve(reader, writer):
            size = struct.unpack("!I", await reader.readexactly(4))[0]
            received.append(await reader.readexactly(size))
            response = b'{"ok":true}'
            writer.write(struct.pack("!I", len(response)) + response)
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_unix_server(serve, path=str(path))
        script = (
            "import asyncio,runpy,sys; from pathlib import Path; "
            f"m=runpy.run_path({str(Path(credential_broker_admin.__file__))!r}); "
            f"m['main'].__globals__['ADMIN_SOCKET_PATH']=Path({str(path)!r}); "
            "asyncio.run(m['main']()); "
            "assert not any(n.startswith(('aiohttp','multidict','yarl')) for n in sys.modules)"
        )
        try:
            child = await asyncio.create_subprocess_exec(
                sys.executable,
                "-S",
                "-c",
                script,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await child.communicate(payload)
            assert child.returncode == 0, stderr
            assert not stdout and not stderr
            assert received == [payload]
            assert payload.decode() not in script
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(run())


@pytest.mark.parametrize("response", [b"bad", b"[]", b"", None])
def test_admin_rejects_invalid_or_lost_response_with_bounded_deadline(tmp_path, monkeypatch, response):
    monkeypatch.setattr(credential_broker_admin, "ADMIN_TIMEOUT_S", 0.02)

    async def run():
        path = tmp_path / "broker.sock"
        finished = asyncio.Event()

        async def serve(reader, writer):
            try:
                await reader.readexactly(4)
                if response is None:
                    await finished.wait()
                else:
                    writer.write(struct.pack("!I", len(response)) + response)
                    await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_unix_server(serve, path=str(path))
        try:
            with pytest.raises((ValueError, RuntimeError, TimeoutError)):
                await credential_broker_admin._send_admin_payload(path, b"{}")
        finally:
            finished.set()
            server.close()
            await server.wait_closed()
            await asyncio.sleep(0)

    asyncio.run(run())


def test_ownership_failure_removes_exact_previous_grants_after_subnet_metadata_drift():
    async def run():
        manager, host = setup_broker()
        await _provision_test_http_binding(manager, network=NETWORK)
        assert host.firewall_rules
        host.networks[NETWORK] = ("172.31.1.0/24", "172.31.1.4")
        host.network_tenants[NETWORK] = "f" * 64
        with pytest.raises(RuntimeError, match="ownership"):
            await manager.prepare_tenant_network("a" * 64, NETWORK)
        assert host.container_id is None
        assert not host.firewall_rules  # revoke the original subnet/IP, not fresh metadata
        assert not manager._network_policies

    asyncio.run(run())


def test_repeated_cancellation_cannot_release_broker_owner_before_decommission_finishes():
    async def run():
        manager, host = setup_broker()
        handle = await _provision_test_http_binding(manager, network=NETWORK)
        original = host._run_docker
        entered = asyncio.Event()
        release = asyncio.Event()

        async def edge(args, *, input_data=None):
            if args[1:3] == ["rm", "-f"]:
                entered.set()
                await release.wait()
            return await original(args, input_data=input_data)

        host._run_docker = edge
        host.fail_next_admin = True
        update = asyncio.create_task(
            manager.provision_http(
                handle,
                tenant_key="a" * 64,
                conversation_key="b" * 64,
                credential=b"new-fixture",
                binding=HttpCredentialBinding(
                    name="github",
                    target_origin="https://api.github.com",
                    allowed_methods=("GET",),
                    allowed_path_prefixes=("/repos",),
                ),
            )
        )
        await entered.wait()
        update.cancel()
        await asyncio.sleep(0)
        update.cancel()
        await asyncio.sleep(0)
        assert not update.done()
        assert manager._shared_lock.locked() and manager._locks["a" * 64].locked()
        assert not manager._synced_payloads
        assert host.container_id is not None
        release.set()
        with pytest.raises(BaseExceptionGroup):
            await update
        assert host.container_id is None
        assert not host.firewall_rules
        assert not manager._shared_lock.locked()
        assert not manager._locks["a" * 64].locked()

    asyncio.run(run())
