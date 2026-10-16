"""Deep timing at real Docker/SIWC entrypoints, with only OS/provider edges faked."""

import asyncio
import hashlib
import json
import logging
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from test_docker_credential_broker import FakeBrokerHost
from test_sandbox_manager import CompletedFakeCodexClient
from test_telegram_ptb_adapter import FakeUpdate
from test_turn_latency import Handler, composition, summaries, until

from soveren_agent_platform.app_api import AgentPlatformApp
from soveren_agent_platform.llm.backends import sandboxed_codex as runtime_module
from soveren_agent_platform.sandbox import DockerEgressHostRelay, DockerEgressSpec, DockerSandboxManager
from soveren_agent_platform.sandbox import docker as docker_module
from soveren_agent_platform.sandbox.docker_broker import DockerCredentialBrokerManager, DockerCredentialBrokerSpec
from soveren_agent_platform.sandbox.docker_commands import CommandResult
from soveren_agent_platform.sandbox.egress import EGRESS_ROUTING_LABEL, EGRESS_ROUTING_VERSION, upstreams_environment
from soveren_agent_platform.sessions import ChatGPTPlanAccessToken, CodexChatGPTPlanCredentials, CodexClientInfo
from soveren_agent_platform.sessions.backends import codex_app_server as native_module
from soveren_agent_platform.sessions.codex_plan_credentials import CodexCredentialUnavailable

TOKEN = "fixture-private-bearer"
RELAY = "http://172.17.0.1:10810"
MAC = "02:00:00:00:00:01"


class DockerEdge:
    def __init__(self, egress, host):
        self.egress = egress
        self.host = host
        self.sandbox = None
        self.spec_hash = None
        self.running = False
        self.calls = []

    async def run(self, args, *, input_data=None):
        self.calls.append(list(args))
        await asyncio.sleep(0.001)
        value = ""
        if args[1] == "ps":
            if "label=soveren.egress=true" in args:
                value = "egress-123"
            elif "label=soveren.runtime=docker" in args:
                value = self.sandbox or ""
        elif args[1:3] == ["network", "inspect"]:
            template = args[args.index("-f") + 1]
            if template == "{{.Internal}}":
                value = "false" if args[-1] == self.egress.public_network else "true"
            elif template == "{{json .Labels}}":
                value = json.dumps(
                    {
                        "soveren.managed": "true",
                        "soveren.tenant_key": hashlib.sha256(b"tenant").hexdigest(),
                        "soveren.conversation_key": hashlib.sha256(b"tenant\0" + b"456").hexdigest(),
                    }
                )
            elif ".Driver" in template:
                value = "bridge\nfalse"
            elif ".EnableIPv6" in template:
                value = "false"
            elif ".Subnet" in template:
                value = "172.30.0.0/16"
            else:
                raise AssertionError(template)
        elif args[1] == "inspect":
            template = args[args.index("-f") + 1]
            if template == "{{json .Config}}":
                value = json.dumps(
                    {
                        "Image": self.egress.image,
                        "Labels": {
                            "soveren.egress_policy": "1",
                            EGRESS_ROUTING_LABEL: EGRESS_ROUTING_VERSION,
                            "soveren.egress_host_relays": json.dumps([RELAY]),
                        },
                        "Env": [
                            f"{key}={value}" for key, value in upstreams_environment(self.egress.upstreams).items()
                        ],
                    }
                )
            elif template == "{{json .NetworkSettings.Networks}}":
                value = json.dumps(
                    {
                        self.egress.public_network: {"IPAddress": "172.31.0.2", "MacAddress": MAC},
                        **{network: {"IPAddress": "172.30.0.2"} for network in self.host.networks},
                    }
                )
            elif template == "{{json .Config.Labels}}":
                return CommandResult(returncode=1, stderr="Error: No such object: helper")
            elif ".State.Health" in template:
                value = "healthy"
            elif ".State.Running" in template:
                value = "true"
            elif ".IPAddress" in template:
                value = "172.30.0.2"
            elif "soveren.spec_hash" in template:
                value = self.spec_hash
            else:
                raise AssertionError(template)
        elif args[1] == "run":
            if "soveren-network-firewall-apply" in args:
                return CommandResult(returncode=0, stdout=json.dumps([False] * len(json.loads(input_data)["rules"])))
            if args[2] == "-d":
                self.sandbox = "sandbox-123"
                self.running = self.host.sandbox_running = True
                self.spec_hash = next(arg.split("=", 1)[1] for arg in args if arg.startswith("soveren.spec_hash="))
                value = self.sandbox
            elif "-S" in args:
                value = "-A INPUT -p tcp --dport 22 -j ACCEPT"
        elif args[1] == "exec":
            if any("gethostbyname" in arg for arg in args):
                value = "172.17.0.1"
            # All other exec calls are guarded auth checks, mkdir or relay probes.
        elif args[1] == "stop":
            self.running = self.host.sandbox_running = False
        elif args[1:3] in (["network", "connect"], ["network", "disconnect"]):
            pass
        else:
            raise AssertionError(args)
        return CommandResult(returncode=0, stdout=value + "\n")


def setup_manager():
    key = hashlib.sha256(b"tenant\0" + b"456").hexdigest()
    tenant = hashlib.sha256(b"tenant").hexdigest()
    network = "soveren-sandbox-egress-" + key[:12]
    host = FakeBrokerHost({network: ("172.30.0.0/16", "172.30.0.4")}, network_tenants={network: tenant})
    route = docker_module.SandboxEgressUpstream(proxy_url=RELAY, destination_hosts=("api.openai.com",))
    egress = DockerEgressSpec(image="egress:test", upstreams=(route,), host_relays=(DockerEgressHostRelay(RELAY),))
    runner = DockerEdge(egress, host)
    spec = DockerCredentialBrokerSpec(image="broker:test")
    manager = DockerSandboxManager(runner=runner, egress=egress, credential_broker=spec)
    manager._credential_broker_manager = DockerCredentialBrokerManager(host=host, spec=spec)
    return manager, runner, host


@pytest.mark.parametrize("failure", [False, True])
def test_deep_acquire_and_siwc_metrics_in_one_production_summary(tmp_path, monkeypatch, caplog, failure):
    caplog.set_level(logging.INFO)

    class Client(CompletedFakeCodexClient):
        def set_last_turn(self, thread_id, turn_id):
            state = super().set_last_turn(thread_id, turn_id)
            state.text_parts = ['{"kind":"reply","text":"done"}']
            return state

    async def run():
        manager, runner, host = setup_manager()
        callbacks = 0

        async def get_token():
            nonlocal callbacks
            callbacks += 1
            await asyncio.sleep(0.005)
            if failure and callbacks == 2:
                raise CodexCredentialUnavailable()
            return ChatGPTPlanAccessToken(TOKEN, datetime.now(UTC) + timedelta(hours=1))

        provider = CodexChatGPTPlanCredentials("tenant", get_token, CodexClientInfo("Pulsy", "0.1.0"))

        async def resolve(tenant):
            assert tenant == "tenant"
            return provider

        monkeypatch.setattr(runtime_module, "_create_sandbox_manager", lambda **kwargs: manager)
        monkeypatch.setattr(native_module, "JsonRpcStdioClient", lambda **kwargs: Client())
        path = tmp_path / "app.db"
        platform = AgentPlatformApp(db_path=path)
        codex = platform.configure_sandboxed_codex(credentials_for_tenant=resolve, model="test")
        handler = Handler(path, llm_backend=codex)
        app, builder = await composition(path, handler, platform=platform, batching_idle_max_s=0.02)
        try:
            await builder.app.handlers[0].callback(FakeUpdate(), SimpleNamespace())
            await app.start()
            await until(lambda: summaries(caplog), timeout=4)
            assert len(summaries(caplog)) == 1
            payload = summaries(caplog)[0]
            assert payload["outcome"] == ("agent_failed" if failure else "sent")
            runtime = payload["codex_turn_details"]["runtime"]
            acquire = runtime["sandbox_acquire_details"]
            refresh = runtime["credentials_refresh_details"]
            for name in (
                "conversation_lock_wait",
                "capacity_reservation",
                "tenant_lock_wait",
                "ensure_egress",
                "internal_network",
                "egress_health",
                "relay_identity",
                "relay_resolve",
                "relay_host_check",
                "relay_firewall",
                "relay_probe",
                "network_policy_inspect",
                "network_policy_apply",
                "broker_network_prepare",
                "broker_verified_networks",
                "reuse_existing",
            ):
                assert acquire[name]["samples"] == 1, name
                assert acquire[name]["availability"] == "measured", name
            assert acquire["broker_registry_sync"]["duration_ms"] is None  # verified warm registry is unchanged
            assert acquire["container_create"]["duration_ms"] is None
            assert acquire["accounted"]["duration_ms"] + acquire["unmeasured"]["duration_ms"] == pytest.approx(
                runtime["sandbox_acquire"]["duration_ms"], abs=0.01
            )
            for name in ("bind", "binding_lock_wait", "auth_file_check", "get_access_token"):
                assert refresh[name]["samples"] == 1
            assert refresh["get_access_token"]["availability"] == ("partial" if failure else "measured")
            if failure:
                assert refresh["revocation"]["samples"] == 1
                assert refresh["revocation"]["availability"] == "measured"
                assert refresh["provision_http_credential"]["duration_ms"] is None
            else:
                assert refresh["provision_http_credential"]["samples"] == 1
                assert refresh["broker_registry_sync"]["samples"] == 1
                assert refresh["broker_health"]["samples"] == 1
                assert refresh["revocation"]["duration_ms"] is None
            assert callbacks == 2
            assert sum("gethostbyname" in str(call) for call in runner.calls) == 2
            assert sum("socket.create_connection" in str(call) for call in runner.calls) == 2
            assert sum("iptables-restore" in call for call in runner.calls) == 2
            for secret in (TOKEN, RELAY, MAC, "172.31.0.2", "sandbox-123", "egress-123"):
                assert secret not in json.dumps(payload)
        finally:
            await app.stop()
            handler.conn.close()

    asyncio.run(run())


def test_deep_groups_use_ancestry_even_at_equal_clock_boundaries(tmp_path, monkeypatch):
    from soveren_agent_platform.runtime import latency
    from soveren_agent_platform.runtime.latency import (
        SQLiteTurnLatencyLogger,
        _runtime_details,
        measure_codex_runtime,
        measure_phase,
    )

    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(latency, "time", SimpleNamespace(monotonic=lambda: clock.now))
    handler = Handler(tmp_path / "app.db")
    logger = SQLiteTurnLatencyLogger._from_connection(handler.conn)
    changes = handler.conn.total_changes
    with logger.agent("tenant", "456", "boundaries", None):
        with measure_phase("codex_turn"):
            with measure_codex_runtime("sandbox_acquire"):
                with measure_codex_runtime("acquire_ensure_egress"):
                    with measure_codex_runtime("acquire_relay_reconcile"):
                        with measure_codex_runtime("acquire_relay_probe"):
                            clock.now = 2
                        clock.now = 3
                    clock.now = 4
                with measure_codex_runtime("broker_health"):
                    pass  # same clock as the following refresh child
            with measure_codex_runtime("credentials_refresh"):
                with measure_codex_runtime("siwc_bind"):
                    with measure_codex_runtime("broker_health"):
                        pass
                    clock.now = 5
    details = _runtime_details(logger._traces[("tenant", "456", "boundaries")])
    acquire = details["sandbox_acquire_details"]
    refresh = details["credentials_refresh_details"]
    assert acquire["broker_health"]["samples"] == refresh["broker_health"]["samples"] == 1
    assert acquire["accounted"]["duration_ms"] == acquire["unmeasured"]["duration_ms"] == 2000
    assert refresh["accounted"]["duration_ms"] == 0
    assert refresh["unmeasured"]["duration_ms"] == 1000
    assert details["egress_unmeasured"]["duration_ms"] == 2000
    assert details["relay_unmeasured"]["duration_ms"] == 1000
    assert acquire["container_create"]["duration_ms"] is None
    assert handler.conn.total_changes == changes
    handler.conn.close()


@pytest.mark.parametrize("waiting", ["conversation", "capacity"])
def test_actual_docker_acquire_cancelled_wait_preserves_locks_and_capacity(tmp_path, waiting):
    from soveren_agent_platform.runtime.latency import (
        SQLiteTurnLatencyLogger,
        _runtime_details,
        measure_codex_runtime,
        measure_phase,
    )
    from soveren_agent_platform.sandbox import SandboxSpec

    async def run():
        manager, runner, host = setup_manager()
        manager.max_active_sandboxes = 1
        spec = SandboxSpec("tenant", "456", "fixture")
        key = hashlib.sha256(b"tenant\0" + b"456").hexdigest()
        lock = manager._sandbox_locks.setdefault(key, asyncio.Lock())
        if waiting == "conversation":
            await lock.acquire()
        else:
            await manager._reserve_capacity("another-conversation")
        handler = Handler(tmp_path / "app.db")
        logger = SQLiteTurnLatencyLogger._from_connection(handler.conn)
        with logger.agent("tenant", "456", waiting, None):
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.02):
                    with measure_phase("codex_turn"):
                        with measure_codex_runtime("sandbox_acquire"):
                            await manager.acquire(spec)
        details = _runtime_details(logger._traces[("tenant", "456", waiting)])
        group = details["sandbox_acquire_details"]
        stage = "conversation_lock_wait" if waiting == "conversation" else "capacity_reservation"
        assert group[stage]["availability"] == "partial"
        assert group[stage]["duration_ms"] >= 10
        assert group["unmeasured"]["availability"] == "partial"
        assert not runner.calls and not host.calls
        assert not manager._capacity_wait_queue
        if waiting == "conversation":
            assert lock.locked()  # a cancelled waiter cannot release someone else's lock
            assert not manager._active_conversation_keys
            lock.release()
        else:
            assert not lock.locked()
            assert manager._active_conversation_keys == {"another-conversation"}
            await manager._release_capacity("another-conversation")
        # Cancellation did not strand the real entrypoint or leak capacity.
        handle = await manager.acquire(spec)
        assert handle.id == "sandbox-123"
        assert not lock.locked()
        handler.conn.close()

    asyncio.run(run())


@pytest.mark.parametrize("failure", ["guard", "timeout", "cancel"])
def test_real_siwc_failure_substep_and_revocation_are_observed(tmp_path, failure):
    from dataclasses import replace

    from test_sandbox_manager import FakeSandboxManager

    from soveren_agent_platform.runtime.latency import (
        SQLiteTurnLatencyLogger,
        _runtime_details,
        measure_codex_runtime,
        measure_phase,
    )

    class Manager(FakeSandboxManager):
        async def run_command(self, *args, **kwargs):
            await super().run_command(*args, **kwargs)
            if failure == "guard":
                raise RuntimeError("fixture auth.json exists")

    async def run():
        manager = Manager()
        manager.handle = replace(manager.handle, tenant_id="tenant")
        callback_calls = 0
        entered = asyncio.Event()

        async def token():
            nonlocal callback_calls
            callback_calls += 1
            entered.set()
            await asyncio.Event().wait()

        provider = CodexChatGPTPlanCredentials("tenant", token, CodexClientInfo("Pulsy", "0.1.0"))
        handler = Handler(tmp_path / "app.db")
        logger = SQLiteTurnLatencyLogger._from_connection(handler.conn)

        async def refresh():
            with logger.agent("tenant", "456", failure, None):
                with measure_phase("codex_turn"):
                    with measure_codex_runtime("credentials_refresh"):
                        await provider.refresh_for_turn(manager, manager.handle)

        if failure == "cancel":
            task = asyncio.create_task(refresh())
            await entered.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        elif failure == "timeout":
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.02):
                    await refresh()
        else:
            with pytest.raises(RuntimeError, match="auth.json"):
                await refresh()
        group = _runtime_details(logger._traces[("tenant", "456", failure)])["credentials_refresh_details"]
        stage = "auth_file_check" if failure == "guard" else "get_access_token"
        assert group[stage]["availability"] == "partial"
        assert group["bind"]["availability"] == "partial"
        assert group["revocation"]["availability"] == "measured"
        assert group["provision_http_credential"]["duration_ms"] is None
        assert callback_calls == (0 if failure == "guard" else 1)
        assert len(manager.http_broker_revocations) == 1
        assert not manager.http_broker_calls
        assert not provider._binding_lock.locked()
        handler.conn.close()

    asyncio.run(run())
