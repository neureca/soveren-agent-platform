"""Opt-in, offline packet checks against real Docker, Squid and broker images."""

import asyncio
import os
import uuid
from dataclasses import replace
from urllib.parse import urlsplit

import pytest

from soveren_agent_platform.app_api import AgentPlatformApp
from soveren_agent_platform.sandbox import (
    DockerEgressSpec,
    HttpCredentialBinding,
    SandboxSpec,
    SubprocessDockerCommandRunner,
)
from soveren_agent_platform.sandbox.docker import _DockerNetworkPolicy
from soveren_agent_platform.sessions import ExistingCodexCredentials

pytestmark = pytest.mark.skipif(
    not (os.environ.get("SOVEREN_TEST_EGRESS_IMAGE") and os.environ.get("SOVEREN_TEST_BROKER_IMAGE")),
    reason="requires explicitly selected local egress and candidate broker images",
)


class RecordingDocker(SubprocessDockerCommandRunner):
    def __init__(self):
        super().__init__()
        self.admin_calls = 0
        self.batch_calls = 0

    async def run(self, args, *, input_data=None):
        self.admin_calls += args[-1] == "/opt/soveren/credential_broker_admin.py"
        self.batch_calls += "soveren-network-firewall-apply" in args and args[1] == "run"
        return await super().run(args, input_data=input_data)


def test_real_warm_prepare_batch_drift_restart_and_packet_isolation(tmp_path):
    async def run():
        runner = RecordingDocker()

        async def docker(*args):
            result = await runner.run(["docker", *args])
            assert result.returncode == 0, result.stderr
            return result.stdout.strip()

        # This adapter deliberately has one control-plane owner per Docker host.
        # Never reconcile or delete a pre-existing test/developer deployment.
        existing = await docker("ps", "-aq", "--filter", "label=soveren.managed=true")
        assert not existing, "real platform test requires an otherwise unused platform Docker test daemon"
        suffix = uuid.uuid4().hex[:10]
        egress = DockerEgressSpec(
            image=os.environ["SOVEREN_TEST_EGRESS_IMAGE"],
            container_name="svr-opt-egress-" + suffix,
            internal_network="svr-opt-private-" + suffix,
            public_network="svr-opt-public-" + suffix,
        )

        async def credentials(tenant):
            return ExistingCodexCredentials()

        platform = AgentPlatformApp(db_path=tmp_path / "app.db", bootstrap_storage=False)
        runtime = platform.configure_sandboxed_codex(
            credentials_for_tenant=credentials,
            model="fixture",
            credential_broker_image=os.environ["SOVEREN_TEST_BROKER_IMAGE"],
        )
        manager = runtime._sandbox_manager
        manager.runner = runner
        manager.egress = egress
        broker = manager.credential_broker
        assert broker is not None and broker.image == os.environ["SOVEREN_TEST_BROKER_IMAGE"]
        sandbox_image = os.environ.get("SOVEREN_TEST_SANDBOX_IMAGE", "soveren-codex-sandbox:test")
        handles = []
        policies = []
        witness = None
        foreign = ["DOCKER-USER", "-s", "198.18.254.123/32", "-j", "DROP"]
        foreign_created = False

        async def connect(handle, address, port):
            result = await runner.run(
                [
                    "docker",
                    "exec",
                    handle.id,
                    "python3",
                    "-c",
                    "import socket,sys; s=socket.socket(); s.settimeout(.3); "
                    "print(s.connect_ex((sys.argv[1],int(sys.argv[2])))); s.close()",
                    address,
                    str(port),
                ]
            )
            assert result.returncode == 0, result.stderr
            return int(result.stdout.strip()) == 0

        try:
            for tenant in ("tenant-a-" + suffix, "tenant-b-" + suffix):
                handle = await manager.acquire(
                    SandboxSpec(
                        tenant,
                        "conversation",
                        sandbox_image,
                        network=egress.internal_network,
                        disk_limit=None,
                    )
                )
                handles.append(handle)
                policies.append(
                    _DockerNetworkPolicy(
                        handle.metadata["network"],
                        handle.metadata["network_subnet"],
                        handle.metadata["egress_proxy_ip"] + "/32",
                    )
                )
                endpoint = await manager.provision_http_credential(
                    handle,
                    credential=b"offline-fixture-secret",
                    binding=HttpCredentialBinding(
                        name="offline",
                        target_origin="https://unused.invalid",
                        allowed_methods=("POST",),
                        allowed_path_prefixes=("/check",),
                    ),
                )
                handles[-1] = replace(handle, metadata={**handle.metadata, "credential_broker_ip": endpoint.network_ip})
            a, b = handles
            before = (runner.admin_calls, runner.batch_calls)
            await manager.acquire(
                SandboxSpec(
                    a.tenant_id, "conversation", sandbox_image, network=egress.internal_network, disk_limit=None
                )
            )
            assert runner.admin_calls == before[0]
            assert runner.batch_calls - before[1] == 2  # verified egress + broker, no per-rule Docker invocations
            assert await connect(a, a.metadata["egress_proxy_ip"], 3128)
            assert await connect(a, a.metadata["credential_broker_ip"], 8080)
            assert not await connect(a, b.metadata["credential_broker_ip"], 8080)
            await docker("exec", "-d", b.id, "python3", "-m", "http.server", "8080")
            for _ in range(30):
                if await connect(b, "127.0.0.1", 8080):
                    break
                await asyncio.sleep(0.05)
            assert await connect(b, "127.0.0.1", 8080)
            b_ip = await docker("inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", b.id)
            assert not await connect(a, b_ip, 8080)
            # A known listening service on the public bridge is denied as well.
            witness = await docker(
                "run",
                "-d",
                "--network",
                egress.public_network,
                "--entrypoint",
                "python",
                broker.image,
                "-m",
                "http.server",
                "8080",
            )
            witness_ip = await docker(
                "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", witness
            )
            await docker(
                "exec",
                witness,
                "python",
                "-c",
                "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080',timeout=3).read()",
            )
            assert not await connect(a, witness_ip, 8080)
            # The capability cannot cross the destination interface's tenant boundary.
            broker_manager = manager._credential_broker_manager
            assert broker_manager is not None and broker_manager._container_id is not None
            endpoint_a = await manager.provision_http_credential(
                a,
                credential=b"rotated-fixture-secret",
                binding=HttpCredentialBinding(
                    name="offline",
                    target_origin="https://unused.invalid",
                    allowed_methods=("POST",),
                    allowed_path_prefixes=("/check",),
                ),
            )
            path = urlsplit(endpoint_a.base_url).path + "/check"
            response = await docker(
                "exec",
                b.id,
                "python3",
                "-c",
                "import http.client,sys; c=http.client.HTTPConnection(sys.argv[1],8080,timeout=3); "
                "c.request('POST',sys.argv[2],body='{}'); print(c.getresponse().status)",
                b.metadata["credential_broker_ip"],
                path,
            )
            assert response == "404"  # capability is absent from this interface's tenant registry
            # Drift repair preserves a foreign exact rule and restores the missing proxy allow.
            foreign_created = await manager._ensure_iptables_rule(foreign)
            await manager._remove_iptables_rule(policies[0].allow_rule())
            await manager.acquire(
                SandboxSpec(
                    a.tenant_id, "conversation", sandbox_image, network=egress.internal_network, disk_limit=None
                )
            )
            assert await connect(a, a.metadata["egress_proxy_ip"], 3128)
            checked = await runner.run(manager._iptables_helper_command(["-C", *foreign]))
            assert checked.returncode == 0
            # Same Docker ID with a new server process must restore every active tenant.
            before_restart = runner.admin_calls
            await docker("restart", broker_manager._container_id)
            await manager.acquire(
                SandboxSpec(
                    a.tenant_id, "conversation", sandbox_image, network=egress.internal_network, disk_limit=None
                )
            )
            assert runner.admin_calls == before_restart + 2
            assert await connect(b, b.metadata["credential_broker_ip"], 8080)
        finally:
            if witness:
                await docker("rm", "-f", witness)
            for handle in reversed(handles):
                await manager.destroy(handle)
            for p in policies:
                await manager._remove_iptables_rule(p.drop_rule())
                await manager._remove_iptables_rule(p.host_input_drop_rule())
            if foreign_created:
                await manager._remove_iptables_rule(foreign)
            await docker("rm", "-f", egress.container_name)
            await docker("network", "rm", egress.public_network)
            await runtime.shutdown()

    asyncio.run(run())
