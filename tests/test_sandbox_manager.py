import asyncio
import base64
import hashlib
import json
import sys
from dataclasses import replace
from types import SimpleNamespace

import pytest

import soveren_agent_platform.sandbox as sandbox_api
import soveren_agent_platform.sessions as sessions_api
from soveren_agent_platform.sandbox import (
    CommandResult,
    CredentialBindingScope,
    CredentialBrokerCapability,
    CredentialBrokerEndpoint,
    CredentialBrokerPolicy,
    DockerCredentialBrokerSpec,
    DockerEgressSpec,
    DockerSandboxManager,
    HttpCredentialBinding,
    SandboxHandle,
    SandboxSpec,
    SubprocessDockerCommandRunner,
)
from soveren_agent_platform.sandbox import docker as docker_module
from soveren_agent_platform.sessions import (
    CodexApiKeyCredentials,
    CodexAuthFileCredentials,
    CodexCollaborationMode,
    CodexThreadInspector,
    ConversationScope,
    ExistingCodexCredentials,
    OpenSpec,
    RuntimeSession,
    SandboxedCodexAppServerBackend,
    SessionBackendRegistry,
    TenantBoundaryError,
    create_sandbox_manager,
    create_sandboxed_codex_backend,
    ensure_conversation_boundary,
    ensure_tenant_boundary,
)


def _sandbox_open_spec(
    backend: SandboxedCodexAppServerBackend,
    *,
    cwd: str = "/ignored",
    metadata: dict | None = None,
) -> OpenSpec:
    return OpenSpec(
        kind="codex_cli",
        cwd=cwd,
        metadata=metadata,
        conversation_scope=ConversationScope(
            tenant_id=backend.tenant_id,
            source_id=backend.source_id,
        ),
    )


def test_subprocess_docker_runner_times_out_and_reaps_process():
    async def run() -> float:
        runner = SubprocessDockerCommandRunner(timeout_s=0.05, terminate_grace_s=0.1)
        started = asyncio.get_running_loop().time()
        with pytest.raises(TimeoutError, match="Docker command timed out"):
            await runner.run([sys.executable, "-c", "import time; time.sleep(60)"])
        return asyncio.get_running_loop().time() - started

    assert asyncio.run(run()) < 1.0


def test_public_sandbox_api_uses_one_manager_vocabulary():
    assert sandbox_api.SandboxManager
    assert sandbox_api.DockerSandboxManager
    assert sandbox_api.CredentialBrokerProvisioner
    assert sandbox_api.HttpCredentialBrokerProvisioner
    assert sessions_api.create_sandbox_manager
    assert not hasattr(sandbox_api, "SandboxRuntime")
    assert not hasattr(sandbox_api, "DockerSandboxRuntime")
    assert not hasattr(sessions_api, "create_sandbox_pool")


class FakeDockerRunner:
    def __init__(self, results: list[CommandResult]) -> None:
        self.results = results
        self.calls: list[list[str]] = []
        self.inputs: list[bytes | None] = []

    async def run(self, args: list[str], *, input_data: bytes | None = None) -> CommandResult:
        self.calls.append(args)
        self.inputs.append(input_data)
        if not self.results:
            return CommandResult(returncode=0)
        return self.results.pop(0)


def test_docker_sandbox_manager_creates_container_with_hard_limits_without_raw_tenant_label():
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0, stdout=""),
            CommandResult(returncode=0, stdout="container-123\n"),
        ]
    )
    manager = DockerSandboxManager(runner=runner)

    handle = asyncio.run(
        manager.acquire(
            SandboxSpec(
                tenant_id="telegram-chat-123",
                conversation_id="chat-123",
                image="soveren-codex-sandbox:latest",
                memory="384m",
                cpus="0.5",
                pids_limit=96,
                network="soveren-sandbox-egress",
            )
        )
    )

    assert handle.id == "container-123"
    run = runner.calls[1]
    assert run[:3] == ["docker", "run", "-d"]
    assert "--memory" in run
    assert run[run.index("--memory") + 1] == "384m"
    assert "--cpus" in run
    assert run[run.index("--cpus") + 1] == "0.5"
    assert "--pids-limit" in run
    assert run[run.index("--pids-limit") + 1] == "96"
    assert "--network" in run
    assert run[run.index("--network") + 1] == "soveren-sandbox-egress"
    assert run[run.index("--storage-opt") + 1] == "size=1g"
    assert run[run.index("--user") + 1] == "10001:10001"
    assert run[run.index("--cap-drop") + 1] == "ALL"
    assert "no-new-privileges:true" in run
    assert "telegram-chat-123" not in " ".join(run)
    assert "chat-123" not in " ".join(run)
    assert "soveren.tenant_key=" in " ".join(run)
    assert "soveren.conversation_key=" in " ".join(run)
    assert "soveren.spec_hash=" in " ".join(run)


def test_docker_sandbox_manager_reuses_existing_stopped_container():
    spec = SandboxSpec(tenant_id="tenant-a", conversation_id="chat-1", image="soveren-codex-sandbox:latest")
    spec_hash = _expected_spec_hash(spec)
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0, stdout="container-123\n"),
            CommandResult(returncode=0, stdout=f"{spec_hash}\n"),
            CommandResult(returncode=0, stdout="false\n"),
            CommandResult(returncode=0, stdout="container-123\n"),
        ]
    )
    manager = DockerSandboxManager(runner=runner)

    handle = asyncio.run(manager.acquire(spec))

    assert handle.id == "container-123"
    assert runner.calls[0][1:4] == ["ps", "-aq", "--no-trunc"]
    assert runner.calls[3] == ["docker", "start", "container-123"]


def test_docker_sandbox_manager_retains_existing_state_when_only_image_changes():
    requested_spec = SandboxSpec(
        tenant_id="tenant-a",
        conversation_id="chat-1",
        image="soveren-codex-sandbox:new",
    )
    existing_spec = replace(requested_spec, image="soveren-codex-sandbox:old")
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0, stdout="container-123\n"),
            CommandResult(returncode=0, stdout=f"{_expected_spec_hash(existing_spec)}\n"),
            CommandResult(returncode=0, stdout=f"{existing_spec.image}\n"),
            CommandResult(returncode=0, stdout="false\n"),
            CommandResult(returncode=0, stdout="container-123\n"),
        ]
    )
    manager = DockerSandboxManager(runner=runner)

    handle = asyncio.run(manager.acquire(requested_spec))

    assert handle.id == "container-123"
    assert handle.metadata["image"] == existing_spec.image
    assert handle.metadata["configured_image"] == requested_spec.image
    assert handle.metadata["image_update_state"] == "deferred_until_destroy"
    assert handle.metadata["spec_hash"] == _expected_spec_hash(existing_spec)
    assert runner.calls[4] == ["docker", "start", "container-123"]


def test_docker_sandbox_manager_rejects_existing_container_with_different_spec():
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0, stdout="container-123\n"),
            CommandResult(returncode=0, stdout="old-spec-hash\n"),
            CommandResult(returncode=0, stdout="soveren-codex-sandbox:latest\n"),
        ]
    )
    manager = DockerSandboxManager(runner=runner)

    with pytest.raises(RuntimeError, match="config changed"):
        asyncio.run(
            manager.acquire(
                SandboxSpec(tenant_id="tenant-a", conversation_id="chat-1", image="soveren-codex-sandbox:latest")
            )
        )

    assert all("start" not in call for call in runner.calls)


def test_docker_sandbox_manager_reports_missing_disk_quota_support():
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0, stdout=""),
            CommandResult(returncode=1, stderr="overlay2: storage-opt size is not supported"),
            CommandResult(returncode=0, stdout=""),
        ]
    )
    manager = DockerSandboxManager(runner=runner)

    with pytest.raises(RuntimeError, match="XFS with pquota"):
        asyncio.run(
            manager.acquire(
                SandboxSpec(tenant_id="tenant-a", conversation_id="chat-1", image="soveren-codex-sandbox:latest")
            )
        )


def test_docker_sandbox_manager_recovers_from_cross_process_create_race():
    spec = SandboxSpec(tenant_id="tenant-a", conversation_id="chat-1", image="soveren-codex-sandbox:latest")
    spec_hash = _expected_spec_hash(spec)
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0, stdout=""),
            CommandResult(returncode=1, stderr="container name is already in use"),
            CommandResult(returncode=0, stdout="container-winner\n"),
            CommandResult(returncode=0, stdout=f"{spec_hash}\n"),
            CommandResult(returncode=0, stdout="true\n"),
        ]
    )
    manager = DockerSandboxManager(runner=runner)

    handle = asyncio.run(manager.acquire(spec))

    assert handle.id == "container-winner"


def test_docker_sandbox_manager_provisions_shared_broker_without_key_metadata():
    tenant_id = "tenant-a"
    conversation_id = "chat-1"
    tenant_key = hashlib.sha256(tenant_id.encode()).hexdigest()
    conversation_key = hashlib.sha256(f"{tenant_id}\0{conversation_id}".encode()).hexdigest()
    network = f"soveren-sandbox-egress-{conversation_key[:12]}"
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0, stdout=f"{network}\n"),
            CommandResult(returncode=0, stdout=""),
            CommandResult(returncode=0, stdout="broker-123\n"),
            CommandResult(returncode=0, stdout="healthy\n"),
            CommandResult(returncode=0, stdout=json.dumps({network: {}})),
            CommandResult(returncode=0, stdout="false\n"),
            CommandResult(returncode=0, stdout="172.30.0.0/16\n"),
            CommandResult(returncode=0, stdout="172.30.0.4\n"),
            CommandResult(returncode=1),
            CommandResult(returncode=0),
            CommandResult(returncode=1),
            CommandResult(returncode=0),
            CommandResult(returncode=0),
        ]
    )
    manager = DockerSandboxManager(
        runner=runner,
        egress=DockerEgressSpec(image="soveren-sandbox-egress:test"),
        credential_broker=DockerCredentialBrokerSpec(image="soveren-credential-broker:test"),
    )
    handle = SandboxHandle(
        id="sandbox-123",
        name="sandbox",
        tenant_id=tenant_id,
        conversation_id=conversation_id,
        workspace_root="/workspace",
        codex_home="/codex-home",
        metadata={
            "runtime": "docker",
            "tenant_key": tenant_key,
            "conversation_key": conversation_key,
            "network": network,
        },
    )

    endpoint = asyncio.run(
        manager.provision_credential_broker(
            handle,
            api_key=b"sk-provider-secret",
            policy=CredentialBrokerPolicy(),
        )
    )

    assert endpoint == CredentialBrokerEndpoint(
        base_url="http://soveren-credential-broker:8080/v1",
        network_ip="172.30.0.4",
    )
    registry_inputs = [value for value in runner.inputs if value is not None]
    assert len(registry_inputs) == 1
    registry = json.loads(registry_inputs[0])
    assert registry["version"] == 2
    assert registry["operation"] == "replace_tenant"
    assert registry["tenant_key"] == tenant_key
    assert len(registry["bindings"]) == 1
    assert base64.b64decode(registry["bindings"][0]["secret"]) == b"sk-provider-secret"
    assert all("sk-provider-secret" not in " ".join(call) for call in runner.calls)
    broker_run = runner.calls[2]
    assert broker_run[broker_run.index("--network") + 1] == network
    assert broker_run[broker_run.index("--network-alias") + 1] == "soveren-credential-broker"
    assert "--read-only" in broker_run
    assert "no-new-privileges:true" in broker_run
    assert f"soveren.tenant_key={tenant_key}" not in broker_run
    assert broker_run[broker_run.index("--name") + 1] == "soveren-credential-broker"
    assert all("OPENAI_API_KEY" not in argument for argument in broker_run)
    assert "SOVEREN_BROKER_EGRESS_PROXY=http://soveren-sandbox-egress:3128" in broker_run
    assert all("SOVEREN_BROKER_MAX_CONCURRENT" not in argument for argument in broker_run)
    assert runner.calls[-1][-3:] == ["python", "/opt/soveren/credential_broker.py", "admin"]
    assert any(
        "--sport" in call and "8080" in call and "--ctstate" in call and "ESTABLISHED,RELATED" in call
        for call in runner.calls
    )


def test_docker_sandbox_manager_removes_tenant_broker_when_last_active_sandbox_stops():
    tenant_id = "tenant-a"
    conversation_id = "chat-1"
    tenant_key = hashlib.sha256(tenant_id.encode()).hexdigest()
    conversation_key = hashlib.sha256(f"{tenant_id}\0{conversation_id}".encode()).hexdigest()
    network = f"soveren-sandbox-egress-{conversation_key[:12]}"
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0),
            CommandResult(returncode=0, stdout=""),
            CommandResult(returncode=0, stdout="broker-123\n"),
            CommandResult(
                returncode=0,
                stdout=json.dumps({network: {}}),
            ),
            CommandResult(returncode=0, stdout="false\n"),
            CommandResult(returncode=0, stdout="172.30.0.0/16\n"),
            CommandResult(returncode=0, stdout="172.30.0.4\n"),
            CommandResult(returncode=0),
            CommandResult(returncode=0),
            CommandResult(returncode=0),
            CommandResult(returncode=0),
            CommandResult(returncode=0),
        ]
    )
    manager = DockerSandboxManager(
        runner=runner,
        egress=DockerEgressSpec(image="soveren-sandbox-egress:test"),
        credential_broker=DockerCredentialBrokerSpec(image="soveren-credential-broker:test"),
    )
    handle = SandboxHandle(
        id="sandbox-123",
        name="sandbox",
        tenant_id=tenant_id,
        conversation_id=conversation_id,
        workspace_root="/workspace",
        codex_home="/codex-home",
        metadata={
            "runtime": "docker",
            "tenant_key": tenant_key,
            "conversation_key": conversation_key,
            "network": network,
        },
    )

    asyncio.run(manager.stop(handle))

    assert runner.calls[0] == ["docker", "stop", "sandbox-123"]
    assert ["docker", "rm", "-f", "broker-123"] in runner.calls


def test_docker_sandbox_manager_reconciles_tenant_broker_after_destroy():
    events: list[str] = []

    class RecordingBrokerManager:
        async def cleanup_network(self, handle, *, tenant_key, network, network_subnet):
            events.append("broker-network-cleaned")

        async def remove_unused(self, tenant_key):
            events.append("broker-unused-checked")

        async def remove_inactive(self, tenant_key):
            events.append("broker-activity-reconciled")

    class RecordingDockerSandboxManager(DockerSandboxManager):
        async def _cleanup_network_policy(self, policy):
            events.append("sandbox-network-cleaned")

    tenant_id = "tenant-a"
    conversation_id = "chat-1"
    tenant_key = hashlib.sha256(tenant_id.encode()).hexdigest()
    conversation_key = hashlib.sha256(f"{tenant_id}\0{conversation_id}".encode()).hexdigest()
    network = f"soveren-sandbox-egress-{conversation_key[:12]}"
    manager = RecordingDockerSandboxManager(
        runner=FakeDockerRunner([CommandResult(returncode=0)]),
        egress=DockerEgressSpec(image="soveren-sandbox-egress:test"),
        credential_broker=DockerCredentialBrokerSpec(image="soveren-credential-broker:test"),
    )
    manager._credential_broker_manager = RecordingBrokerManager()
    handle = SandboxHandle(
        id="sandbox-123",
        name="sandbox",
        tenant_id=tenant_id,
        conversation_id=conversation_id,
        workspace_root="/workspace",
        codex_home="/codex-home",
        metadata={
            "runtime": "docker",
            "tenant_key": tenant_key,
            "conversation_key": conversation_key,
            "network": network,
            "network_subnet": "172.30.0.0/16",
            "egress_proxy_ip": "172.30.0.2",
        },
    )

    asyncio.run(manager.destroy(handle))

    assert events == [
        "broker-network-cleaned",
        "sandbox-network-cleaned",
        "broker-unused-checked",
        "broker-activity-reconciled",
    ]


def test_docker_sandbox_manager_provisions_shared_egress_before_tenant_container():
    egress_image = "soveren-sandbox-egress:test"
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=1, stderr="not found"),
            CommandResult(returncode=0, stdout="internal-network\n"),
            CommandResult(returncode=0, stdout="false\n"),
            CommandResult(returncode=0, stdout="172.30.0.0/16\n"),
            CommandResult(returncode=1, stderr="not found"),
            CommandResult(returncode=0, stdout="public-network\n"),
            CommandResult(returncode=0, stdout=""),
            CommandResult(returncode=0, stdout="egress-123\n"),
            CommandResult(
                returncode=0,
                stdout=json.dumps(
                    {
                        "Image": egress_image,
                        "Labels": {"soveren.egress_policy": "1"},
                    }
                ),
            ),
            CommandResult(returncode=0, stdout="true\n"),
            CommandResult(returncode=0, stdout='{"soveren-sandbox-public-egress": {}}\n'),
            CommandResult(returncode=0),
            CommandResult(returncode=0, stdout="healthy\n"),
            CommandResult(returncode=0, stdout="172.30.0.2\n"),
            CommandResult(returncode=1),
            CommandResult(returncode=0),
            CommandResult(returncode=1),
            CommandResult(returncode=0),
            CommandResult(returncode=1),
            CommandResult(returncode=0),
            CommandResult(returncode=1),
            CommandResult(returncode=0),
            CommandResult(returncode=1),
            CommandResult(returncode=0, stdout=""),
            CommandResult(returncode=0, stdout="tenant-123\n"),
        ]
    )
    manager = DockerSandboxManager(
        runner=runner,
        egress=DockerEgressSpec(image=egress_image),
    )

    handle = asyncio.run(
        manager.acquire(
            SandboxSpec(
                tenant_id="tenant-a",
                conversation_id="chat-1",
                image="soveren-codex-sandbox:test",
                network="soveren-sandbox-egress",
            )
        )
    )

    assert handle.id == "tenant-123"
    tenant_network = "soveren-sandbox-egress-fee3e8259204"
    assert runner.calls[1] == [
        "docker",
        "network",
        "create",
        "--driver",
        "bridge",
        "--internal",
        "--opt",
        "com.docker.network.bridge.enable_icc=false",
        "--label",
        "soveren.conversation_key=fee3e8259204e2e38c2671473c3c65c128845906d625257c5a11478ffb15979c",
        "--label",
        "soveren.managed=true",
        "--label",
        "soveren.tenant_key=80a707af7dc77ee1228f9127180f3964835e5beb4c4ab0d812f0fe7593579b3a",
        tenant_network,
    ]
    assert runner.calls[5] == ["docker", "network", "create", "soveren-sandbox-public-egress"]
    egress_run = runner.calls[7]
    assert egress_run[:3] == ["docker", "run", "-d"]
    assert egress_image in egress_run
    assert "--read-only" in egress_run
    assert "soveren.egress_policy=1" in egress_run
    assert runner.calls[11][-3:] == [
        "soveren-sandbox-egress",
        tenant_network,
        "egress-123",
    ]
    firewall_calls = [call for call in runner.calls if "--entrypoint" in call]
    assert len(firewall_calls) == 9
    assert any("172.30.0.0/16" in call and "DROP" in call for call in firewall_calls)
    assert any("172.30.0.2/32" in call and "3128" in call and "ACCEPT" in call for call in firewall_calls)
    installed_firewall_rules = [call for call in firewall_calls if "-I" in call]
    assert any(
        "172.30.0.2/32" in call
        and "172.30.0.0/16" in call
        and "--sport" in call
        and "3128" in call
        and "ESTABLISHED,RELATED" in call
        for call in installed_firewall_rules
    )
    assert not any(
        "172.30.0.2/32" in call and "ACCEPT" in call and "--sport" not in call and "--dport" not in call
        for call in installed_firewall_rules
    )
    assert any("INPUT" in call and "172.30.0.0/16" in call and "DROP" in call for call in firewall_calls)
    assert any(call[1:3] == ["run", "-d"] and "soveren-codex-sandbox:test" in call for call in runner.calls)
    assert handle.metadata["network_subnet"] == "172.30.0.0/16"


def test_docker_sandbox_manager_replaces_an_outdated_egress_container():
    class TrackingManager(DockerSandboxManager):
        def __init__(self) -> None:
            super().__init__(
                runner=FakeDockerRunner([]),
                egress=DockerEgressSpec(image="soveren-sandbox-egress:new"),
            )
            self.inspected: list[str] = []
            self.replaced: list[str] = []

        async def _find_egress_container_id(self) -> str | None:
            return "egress-old"

        async def _inspect_egress_container(
            self,
            container_id: str,
        ) -> docker_module._DockerEgressConfiguration:
            self.inspected.append(container_id)
            image = "soveren-sandbox-egress:new" if container_id == "egress-new" else "soveren-sandbox-egress:old"
            return docker_module._DockerEgressConfiguration(
                image=image,
                policy_version="1",
            )

        async def _replace_outdated_egress_image(self, container_id: str) -> str:
            self.replaced.append(container_id)
            return "egress-new"

    manager = TrackingManager()

    container_id = asyncio.run(manager._ensure_current_egress_container())

    assert container_id == "egress-new"
    assert manager.inspected == ["egress-old", "egress-new"]
    assert manager.replaced == ["egress-old"]


def test_docker_sandbox_manager_requires_explicit_egress_policy_migration():
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0, stdout="egress-old\n"),
            CommandResult(
                returncode=0,
                stdout=json.dumps(
                    {
                        "Image": "soveren-sandbox-egress:old",
                        "Labels": {"soveren.egress_policy": "0"},
                    }
                ),
            ),
        ]
    )
    manager = DockerSandboxManager(
        runner=runner,
        egress=DockerEgressSpec(image="soveren-sandbox-egress:new"),
    )

    with pytest.raises(RuntimeError, match="apply the matching policy migration"):
        asyncio.run(manager._ensure_current_egress_container())

    assert all(call[1:3] != ["rm", "-f"] for call in runner.calls)


def test_docker_sandbox_manager_rotates_egress_without_removing_fail_closed_rules():
    tenant_network = "soveren-sandbox-egress-conversation"
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0, stdout=""),
            CommandResult(
                returncode=0,
                stdout=json.dumps(
                    {
                        "soveren-sandbox-public-egress": {},
                        tenant_network: {},
                    }
                ),
            ),
            CommandResult(returncode=0, stdout="false\n"),
            CommandResult(returncode=0, stdout="172.30.0.0/16\n"),
            CommandResult(returncode=0, stdout="172.30.0.2\n"),
            CommandResult(returncode=1),
            CommandResult(returncode=1),
            CommandResult(returncode=1),
            CommandResult(returncode=0),
            CommandResult(returncode=0, stdout="egress-new\n"),
        ]
    )
    manager = DockerSandboxManager(
        runner=runner,
        egress=DockerEgressSpec(image="soveren-sandbox-egress:new"),
    )

    container_id = asyncio.run(manager._replace_outdated_egress_image("egress-old"))

    assert container_id == "egress-new"
    assert ["docker", "rm", "-f", "egress-old"] in runner.calls
    firewall_calls = [call for call in runner.calls if "--entrypoint" in call]
    assert len(firewall_calls) == 3
    assert all("-C" in call for call in firewall_calls)
    assert not any("DROP" in call for call in firewall_calls)


def test_docker_sandbox_manager_refuses_egress_rotation_while_a_sandbox_is_running():
    runner = FakeDockerRunner([CommandResult(returncode=0, stdout="sandbox-123\n")])
    manager = DockerSandboxManager(
        runner=runner,
        egress=DockerEgressSpec(image="soveren-sandbox-egress:new"),
    )

    with pytest.raises(RuntimeError, match="conversation sandboxes are running"):
        asyncio.run(manager._replace_outdated_egress_image("egress-old"))

    assert all(call[1:3] != ["rm", "-f"] for call in runner.calls)


def test_docker_sandbox_manager_replaces_legacy_broad_proxy_rule():
    runner = FakeDockerRunner([CommandResult(returncode=0) for _ in range(10)])
    manager = DockerSandboxManager(
        runner=runner,
        egress=DockerEgressSpec(image="soveren-sandbox-egress:test"),
    )
    policy = docker_module._DockerNetworkPolicy(
        network="soveren-sandbox-egress-tenant",
        source="172.30.0.0/16",
        destination="172.30.0.2/32",
    )

    asyncio.run(manager._ensure_network_policy(policy=policy))

    firewall_calls = [" ".join(call) for call in runner.calls]
    assert any(
        "-I DOCKER-USER 1 -s 172.30.0.2/32 -d 172.30.0.0/16" in call
        and "--sport 3128" in call
        and "--ctstate ESTABLISHED,RELATED" in call
        for call in firewall_calls
    )
    assert any("-D DOCKER-USER -s 172.30.0.2/32 -j ACCEPT" in call for call in firewall_calls)


def test_docker_sandbox_manager_rejects_existing_network_owned_by_another_conversation():
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0, stdout="true\n"),
            CommandResult(
                returncode=0,
                stdout=json.dumps(
                    {
                        "soveren.managed": "true",
                        "soveren.tenant_key": "another-tenant",
                        "soveren.conversation_key": "another-conversation",
                    }
                ),
            ),
        ]
    )
    manager = DockerSandboxManager(runner=runner)

    with pytest.raises(RuntimeError, match="not owned by the requested tenant conversation"):
        asyncio.run(
            manager._ensure_network(
                "soveren-sandbox-egress-collision",
                internal=True,
                labels={
                    "soveren.managed": "true",
                    "soveren.tenant_key": "expected-tenant",
                    "soveren.conversation_key": "expected-conversation",
                },
            )
        )


def test_docker_sandbox_manager_rejects_existing_network_with_peer_connectivity():
    labels = {
        "soveren.managed": "true",
        "soveren.tenant_key": "expected-tenant",
        "soveren.conversation_key": "expected-conversation",
    }
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0, stdout="true\n"),
            CommandResult(returncode=0, stdout=json.dumps(labels)),
            CommandResult(returncode=0, stdout="bridge\ntrue\n"),
        ]
    )
    manager = DockerSandboxManager(runner=runner)

    with pytest.raises(RuntimeError, match="does not disable inter-container connectivity"):
        asyncio.run(
            manager._ensure_network(
                "soveren-sandbox-egress-stale",
                internal=True,
                labels=labels,
            )
        )


def test_docker_sandbox_manager_rejects_ipv6_tenant_network_without_dual_stack_firewall():
    runner = FakeDockerRunner([CommandResult(returncode=0, stdout="true\n")])
    manager = DockerSandboxManager(
        runner=runner,
        egress=DockerEgressSpec(image="soveren-sandbox-egress:test"),
    )

    with pytest.raises(RuntimeError, match="IPv6 disabled"):
        asyncio.run(manager._tenant_network_subnet("soveren-sandbox-egress-tenant"))

    assert runner.calls == [
        [
            "docker",
            "network",
            "inspect",
            "-f",
            "{{.EnableIPv6}}",
            "soveren-sandbox-egress-tenant",
        ]
    ]


def test_docker_sandbox_manager_rolls_back_network_policy_when_container_create_fails():
    policy = docker_module._DockerNetworkPolicy(
        network="soveren-sandbox-egress-tenant",
        source="172.30.0.0/16",
        destination="172.30.0.2/32",
    )

    class FailingManager(DockerSandboxManager):
        def __init__(self):
            super().__init__(
                runner=FakeDockerRunner([]),
                egress=DockerEgressSpec(image="soveren-sandbox-egress:test"),
            )
            self.cleaned: list[object] = []

        async def _ensure_egress(
            self,
            *,
            internal_network: str,
            tenant_key: str,
            conversation_key: str,
        ):
            return policy

        async def _acquire_locked(self, spec, *, tenant_key, conversation_key):
            raise RuntimeError("tenant image missing")

        async def _find_container_id(self, tenant_key: str, conversation_key: str):
            return None

        async def _cleanup_network_policy(self, value):
            self.cleaned.append(value)

    manager = FailingManager()

    with pytest.raises(RuntimeError, match="tenant image missing"):
        asyncio.run(
            manager.acquire(
                SandboxSpec(
                    tenant_id="tenant-a",
                    conversation_id="chat-1",
                    image="missing:test",
                    network="soveren-sandbox-egress",
                )
            )
        )

    assert manager.cleaned == [policy]
    assert manager._active_conversation_keys == set()


def test_docker_sandbox_manager_rolls_back_broker_before_failed_acquire_network_cleanup():
    events: list[str] = []

    class RecordingBrokerManager:
        async def prepare_tenant_network(self, tenant_key: str, network: str):
            events.append(f"broker-prepared:{network}")
            return SimpleNamespace(network=network)

        async def rollback_prepared_network(
            self,
            *,
            preparation,
            network_subnet: str,
        ):
            events.append(f"broker-rolled-back:{preparation.network}")

    class FailingManager(DockerSandboxManager):
        def __init__(self):
            super().__init__(
                runner=FakeDockerRunner([]),
                egress=DockerEgressSpec(image="soveren-sandbox-egress:test"),
                credential_broker=DockerCredentialBrokerSpec(image="soveren-credential-broker:test"),
            )
            self._credential_broker_manager = RecordingBrokerManager()

        async def _ensure_egress(
            self,
            *,
            internal_network: str,
            tenant_key: str,
            conversation_key: str,
        ):
            return docker_module._DockerNetworkPolicy(
                network=internal_network,
                source="172.30.0.0/16",
                destination="172.30.0.2/32",
            )

        async def _acquire_locked(self, spec, *, tenant_key, conversation_key):
            raise RuntimeError("tenant image missing")

        async def _find_container_id(self, tenant_key: str, conversation_key: str):
            return None

        async def _cleanup_network_policy(self, policy):
            events.append(f"network-cleaned:{policy.network}")

    manager = FailingManager()

    with pytest.raises(RuntimeError, match="tenant image missing"):
        asyncio.run(
            manager.acquire(
                SandboxSpec(
                    tenant_id="tenant-a",
                    conversation_id="chat-1",
                    image="missing:test",
                    network="soveren-sandbox-egress",
                )
            )
        )

    network = events[0].split(":", 1)[1]
    assert events == [
        f"broker-prepared:{network}",
        f"broker-rolled-back:{network}",
        f"network-cleaned:{network}",
    ]
    assert manager._active_conversation_keys == set()


def test_docker_sandbox_manager_serializes_tenant_stop_with_sandbox_start():
    async def run():
        tenant_id = "tenant-a"
        conversation_a = "chat-a"
        conversation_b = "chat-b"
        tenant_key = hashlib.sha256(tenant_id.encode()).hexdigest()
        conversation_key_a = hashlib.sha256(f"{tenant_id}\0{conversation_a}".encode()).hexdigest()
        conversation_key_b = hashlib.sha256(f"{tenant_id}\0{conversation_b}".encode()).hexdigest()
        between_prepare_and_start = asyncio.Event()
        allow_start = asyncio.Event()

        class CoordinatedBrokerManager:
            def __init__(self):
                self.running = True
                self.remove_calls = 0

            async def prepare_tenant_network(self, requested_tenant: str, network: str):
                assert requested_tenant == tenant_key
                return SimpleNamespace(network=network)

            async def remove_inactive(self, requested_tenant: str):
                assert requested_tenant == tenant_key
                self.remove_calls += 1
                if not manager.sandbox_b_running:
                    self.running = False

        class CoordinatedManager(DockerSandboxManager):
            def __init__(self):
                super().__init__(
                    runner=FakeDockerRunner([]),
                    max_active_sandboxes=2,
                    egress=DockerEgressSpec(image="soveren-sandbox-egress:test"),
                    credential_broker=DockerCredentialBrokerSpec(image="soveren-credential-broker:test"),
                )
                self.sandbox_b_running = False

            async def _ensure_egress(
                self,
                *,
                internal_network: str,
                tenant_key: str,
                conversation_key: str,
            ):
                return docker_module._DockerNetworkPolicy(
                    network=internal_network,
                    source="172.30.0.0/16",
                    destination="172.30.0.2/32",
                )

            async def _acquire_locked(self, spec, *, tenant_key, conversation_key):
                between_prepare_and_start.set()
                await allow_start.wait()
                self.sandbox_b_running = True
                return SandboxHandle(
                    id="sandbox-b",
                    name="sandbox-b",
                    tenant_id=spec.tenant_id,
                    conversation_id=spec.conversation_id,
                    workspace_root=spec.workspace_root,
                    codex_home=spec.codex_home,
                    metadata={
                        "runtime": "docker",
                        "tenant_key": tenant_key,
                        "conversation_key": conversation_key,
                        "network": spec.network,
                    },
                )

        manager = CoordinatedManager()
        broker = CoordinatedBrokerManager()
        manager._credential_broker_manager = broker
        manager._active_conversation_keys.add(conversation_key_a)
        handle_a = SandboxHandle(
            id="sandbox-a",
            name="sandbox-a",
            tenant_id=tenant_id,
            conversation_id=conversation_a,
            workspace_root="/workspace",
            codex_home="/codex-home",
            metadata={
                "runtime": "docker",
                "tenant_key": tenant_key,
                "conversation_key": conversation_key_a,
                "network": f"soveren-sandbox-egress-{conversation_key_a[:12]}",
            },
        )
        acquire_task = asyncio.create_task(
            manager.acquire(
                SandboxSpec(
                    tenant_id=tenant_id,
                    conversation_id=conversation_b,
                    image="soveren-codex-sandbox:test",
                    network="soveren-sandbox-egress",
                )
            )
        )
        await asyncio.wait_for(between_prepare_and_start.wait(), timeout=1)
        stop_task = asyncio.create_task(manager.stop(handle_a))
        await asyncio.sleep(0)
        assert not stop_task.done()

        allow_start.set()
        handle_b, _ = await asyncio.gather(acquire_task, stop_task)
        return manager, broker, handle_b, conversation_key_b

    manager, broker, handle_b, conversation_key_b = asyncio.run(run())

    assert handle_b.id == "sandbox-b"
    assert broker.running
    assert broker.remove_calls == 1
    assert manager._active_conversation_keys == {conversation_key_b}


def test_docker_sandbox_destroy_cleans_policy_when_egress_container_is_missing():
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0),
            CommandResult(returncode=0, stdout=""),
            CommandResult(returncode=1),
            CommandResult(returncode=1),
            CommandResult(returncode=1),
            CommandResult(returncode=0, stdout=""),
            CommandResult(returncode=0),
            CommandResult(returncode=1),
            CommandResult(returncode=1),
        ]
    )
    manager = DockerSandboxManager(
        runner=runner,
        egress=DockerEgressSpec(image="soveren-sandbox-egress:test"),
    )
    handle = SandboxHandle(
        id="tenant-123",
        name="soveren-sandbox-tenant",
        tenant_id="tenant-a",
        conversation_id="chat-1",
        workspace_root="/workspace",
        codex_home="/codex-home",
        metadata={
            "tenant_key": "tenant-key",
            "network": "soveren-sandbox-egress-tenant",
            "network_subnet": "172.30.0.0/16",
            "egress_proxy_ip": "172.30.0.2",
        },
    )

    asyncio.run(manager.destroy(handle))

    assert runner.calls[0] == ["docker", "rm", "-f", "tenant-123"]
    assert runner.calls[6] == ["docker", "network", "rm", "soveren-sandbox-egress-tenant"]
    assert all(call[1] != "inspect" for call in runner.calls if len(call) > 1)


def test_docker_sandbox_destroy_cleans_policy_when_egress_container_is_stopped():
    tenant_network = "soveren-sandbox-egress-tenant"
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0),
            CommandResult(returncode=0, stdout="egress-123\n"),
            CommandResult(returncode=0, stdout="false\n"),
            CommandResult(returncode=0),
            CommandResult(returncode=0),
            CommandResult(returncode=0),
            CommandResult(returncode=0),
            CommandResult(returncode=0),
            CommandResult(returncode=0),
            CommandResult(returncode=0, stdout="egress-123\n"),
            CommandResult(returncode=0, stdout=json.dumps({tenant_network: {}})),
            CommandResult(returncode=0),
            CommandResult(returncode=0),
            CommandResult(returncode=0),
            CommandResult(returncode=0),
            CommandResult(returncode=0),
            CommandResult(returncode=0),
        ]
    )
    manager = DockerSandboxManager(
        runner=runner,
        egress=DockerEgressSpec(image="soveren-sandbox-egress:test"),
    )
    handle = SandboxHandle(
        id="tenant-123",
        name="soveren-sandbox-tenant",
        tenant_id="tenant-a",
        conversation_id="chat-1",
        workspace_root="/workspace",
        codex_home="/codex-home",
        metadata={
            "tenant_key": "tenant-key",
            "network": tenant_network,
            "network_subnet": "172.30.0.0/16",
            "egress_proxy_ip": "172.30.0.2",
        },
    )

    asyncio.run(manager.destroy(handle))

    assert ["docker", "inspect", "-f", "{{.State.Running}}", "egress-123"] in runner.calls
    assert ["docker", "network", "disconnect", "-f", tenant_network, "egress-123"] in runner.calls
    assert ["docker", "network", "rm", tenant_network] in runner.calls
    assert all(".IPAddress" not in " ".join(call) for call in runner.calls)


def test_docker_sandbox_cleanup_rejects_invalid_policy_from_running_egress():
    tenant_network = "soveren-sandbox-egress-tenant"
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0, stdout="egress-123\n"),
            CommandResult(returncode=0, stdout="true\n"),
            CommandResult(returncode=0, stdout=json.dumps({tenant_network: {}})),
            CommandResult(returncode=0, stdout="not-an-ip\n"),
            CommandResult(returncode=0, stdout="true\n"),
        ]
    )
    manager = DockerSandboxManager(
        runner=runner,
        egress=DockerEgressSpec(image="soveren-sandbox-egress:test"),
    )

    with pytest.raises(RuntimeError, match="invalid tenant network policy metadata"):
        asyncio.run(
            manager._current_network_policy(
                tenant_network,
                network_subnet="172.30.0.0/16",
            )
        )


def test_docker_sandbox_cleanup_treats_egress_removed_before_state_inspect_as_missing():
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0, stdout="egress-123\n"),
            CommandResult(returncode=1, stderr="Error: No such object: egress-123"),
        ]
    )
    manager = DockerSandboxManager(
        runner=runner,
        egress=DockerEgressSpec(image="soveren-sandbox-egress:test"),
    )

    policy = asyncio.run(
        manager._current_network_policy(
            "soveren-sandbox-egress-tenant",
            network_subnet="172.30.0.0/16",
        )
    )

    assert policy is None


@pytest.mark.parametrize(
    "results",
    [
        [
            CommandResult(returncode=0, stdout="egress-123\n"),
            CommandResult(returncode=1, stderr="Error: No such object: egress-123"),
        ],
        [
            CommandResult(returncode=0, stdout="egress-123\n"),
            CommandResult(
                returncode=0,
                stdout=json.dumps({"soveren-sandbox-egress-tenant": {}}),
            ),
            CommandResult(returncode=1, stderr="Error: No such container: egress-123"),
        ],
    ],
)
def test_docker_sandbox_cleanup_treats_egress_removed_during_disconnect_as_missing(results):
    manager = DockerSandboxManager(
        runner=FakeDockerRunner(results),
        egress=DockerEgressSpec(image="soveren-sandbox-egress:test"),
    )

    asyncio.run(manager._disconnect_current_egress("soveren-sandbox-egress-tenant"))


def test_docker_sandbox_destroy_cleans_policy_when_tenant_container_is_already_missing():
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=1, stderr="Error: No such container: tenant-123"),
            CommandResult(returncode=0, stdout=""),
            CommandResult(returncode=1),
            CommandResult(returncode=1),
            CommandResult(returncode=1),
            CommandResult(returncode=0, stdout=""),
            CommandResult(returncode=0),
            CommandResult(returncode=1),
            CommandResult(returncode=1),
        ]
    )
    manager = DockerSandboxManager(
        runner=runner,
        egress=DockerEgressSpec(image="soveren-sandbox-egress:test"),
    )
    handle = SandboxHandle(
        id="tenant-123",
        name="soveren-sandbox-tenant",
        tenant_id="tenant-a",
        conversation_id="chat-1",
        workspace_root="/workspace",
        codex_home="/codex-home",
        metadata={
            "tenant_key": "tenant-key",
            "network": "soveren-sandbox-egress-tenant",
            "network_subnet": "172.30.0.0/16",
            "egress_proxy_ip": "172.30.0.2",
        },
    )

    asyncio.run(manager.destroy(handle))

    assert runner.calls[0] == ["docker", "rm", "-f", "tenant-123"]
    assert runner.calls[6] == ["docker", "network", "rm", "soveren-sandbox-egress-tenant"]


def test_docker_sandbox_restores_existing_firewall_rule_when_reordering_fails():
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0),
            CommandResult(returncode=0),
            CommandResult(returncode=1, stderr="insert failed"),
            CommandResult(returncode=0),
        ]
    )
    manager = DockerSandboxManager(
        runner=runner,
        egress=DockerEgressSpec(image="soveren-sandbox-egress:test"),
    )
    rule = ["DOCKER-USER", "-s", "172.30.0.0/16", "-j", "DROP"]

    with pytest.raises(RuntimeError, match="could not be installed"):
        asyncio.run(manager._ensure_iptables_rule(rule, force_first=True))

    assert runner.calls[-1][-6:] == ["-A", *rule]


def test_docker_sandbox_manager_recovers_running_orphans_once_after_process_restart():
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0, stdout="orphan-a\norphan-b\n"),
            CommandResult(returncode=0, stdout="orphan-a\n"),
            CommandResult(returncode=0, stdout="orphan-b\n"),
            CommandResult(returncode=0, stdout=""),
            CommandResult(returncode=0, stdout="tenant-a\n"),
            CommandResult(returncode=0, stdout=""),
            CommandResult(returncode=0, stdout="tenant-b\n"),
        ]
    )
    manager = DockerSandboxManager(
        runner=runner,
        max_active_sandboxes=2,
        recover_orphaned_sandboxes=True,
    )

    async def run():
        first = await manager.acquire(
            SandboxSpec(tenant_id="tenant-a", conversation_id="chat-1", image="soveren-codex-sandbox:latest")
        )
        second = await manager.acquire(
            SandboxSpec(tenant_id="tenant-b", conversation_id="chat-1", image="soveren-codex-sandbox:latest")
        )
        return first, second

    first, second = asyncio.run(run())

    assert (first.id, second.id) == ("tenant-a", "tenant-b")
    assert runner.calls[0][1:3] == ["ps", "-q"]
    assert runner.calls[1] == ["docker", "stop", "orphan-a"]
    assert runner.calls[2] == ["docker", "stop", "orphan-b"]
    assert sum(call[1:3] == ["ps", "-q"] for call in runner.calls) == 1


def test_docker_sandbox_manager_limits_active_conversation_capacity():
    class FastDockerSandboxManager(DockerSandboxManager):
        async def _acquire_locked(self, spec, *, tenant_key, conversation_key):
            return SandboxHandle(
                id=f"container-{conversation_key[:8]}",
                name=f"sandbox-{conversation_key[:8]}",
                tenant_id=spec.tenant_id,
                conversation_id=spec.conversation_id,
                workspace_root=spec.workspace_root,
                codex_home=spec.codex_home,
                metadata={"tenant_key": tenant_key, "conversation_key": conversation_key},
            )

    async def run():
        manager = FastDockerSandboxManager(
            runner=FakeDockerRunner([]),
            max_active_sandboxes=1,
        )
        first = await manager.acquire(
            SandboxSpec(tenant_id="tenant-a", conversation_id="chat-1", image="soveren-codex-sandbox:latest")
        )
        second_task = asyncio.create_task(
            manager.acquire(
                SandboxSpec(tenant_id="tenant-b", conversation_id="chat-1", image="soveren-codex-sandbox:latest")
            )
        )
        await asyncio.sleep(0)
        assert not second_task.done()
        await manager.stop(first)
        second = await asyncio.wait_for(second_task, timeout=1)
        await manager.destroy(second)
        return second

    second = asyncio.run(run())

    assert second.tenant_id == "tenant-b"


def test_docker_sandbox_manager_releases_capacity_when_cancelled_waiting_for_tenant_lifecycle():
    class WaitingDockerSandboxManager(DockerSandboxManager):
        def __init__(self):
            super().__init__(runner=FakeDockerRunner([]), max_active_sandboxes=1)
            self.capacity_reserved = asyncio.Event()

        async def _reserve_capacity(self, conversation_key: str) -> bool:
            reserved = await super()._reserve_capacity(conversation_key)
            self.capacity_reserved.set()
            return reserved

    async def run():
        manager = WaitingDockerSandboxManager()
        tenant_id = "tenant-a"
        conversation_id = "chat-1"
        tenant_key = hashlib.sha256(tenant_id.encode()).hexdigest()
        tenant_lock = manager._tenant_lifecycle_locks.setdefault(tenant_key, asyncio.Lock())
        await tenant_lock.acquire()
        task = asyncio.create_task(
            manager.acquire(
                SandboxSpec(
                    tenant_id=tenant_id,
                    conversation_id=conversation_id,
                    image="soveren-codex-sandbox:latest",
                )
            )
        )
        try:
            await asyncio.wait_for(manager.capacity_reserved.wait(), timeout=1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            tenant_lock.release()
        return manager

    manager = asyncio.run(run())

    assert manager._active_conversation_keys == set()


def test_docker_sandbox_manager_separates_conversations_in_one_tenant():
    class CapturingDockerSandboxManager(DockerSandboxManager):
        def __init__(self) -> None:
            super().__init__(
                runner=FakeDockerRunner([]),
                max_active_sandboxes=2,
            )
            self.keys: list[str] = []

        async def _acquire_locked(self, spec, *, tenant_key, conversation_key):
            self.keys.append(conversation_key)
            return SandboxHandle(
                id=f"container-{conversation_key[:8]}",
                name=f"sandbox-{conversation_key[:8]}",
                tenant_id=spec.tenant_id,
                conversation_id=spec.conversation_id,
                workspace_root=spec.workspace_root,
                codex_home=spec.codex_home,
                metadata={
                    "tenant_key": tenant_key,
                    "conversation_key": conversation_key,
                },
            )

    async def run():
        manager = CapturingDockerSandboxManager()
        first = await manager.acquire(
            SandboxSpec(
                tenant_id="tenant-a",
                conversation_id="chat-a",
                image="soveren-codex-sandbox:latest",
            )
        )
        second = await manager.acquire(
            SandboxSpec(
                tenant_id="tenant-a",
                conversation_id="chat-b",
                image="soveren-codex-sandbox:latest",
            )
        )
        return manager, first, second

    manager, first, second = asyncio.run(run())

    assert first.tenant_id == second.tenant_id == "tenant-a"
    assert first.conversation_id == "chat-a"
    assert second.conversation_id == "chat-b"
    assert first.id != second.id
    assert len(set(manager.keys)) == 2


def test_docker_sandbox_manager_keeps_capacity_reserved_when_stop_fails():
    class FastDockerSandboxManager(DockerSandboxManager):
        async def _acquire_locked(self, spec, *, tenant_key, conversation_key):
            return SandboxHandle(
                id=f"container-{conversation_key[:8]}",
                name=f"sandbox-{conversation_key[:8]}",
                tenant_id=spec.tenant_id,
                conversation_id=spec.conversation_id,
                workspace_root=spec.workspace_root,
                codex_home=spec.codex_home,
                metadata={"tenant_key": tenant_key, "conversation_key": conversation_key},
            )

    async def run():
        manager = FastDockerSandboxManager(
            runner=FakeDockerRunner([CommandResult(returncode=1, stderr="stop failed")]),
            max_active_sandboxes=1,
        )
        first = await manager.acquire(
            SandboxSpec(tenant_id="tenant-a", conversation_id="chat-1", image="soveren-codex-sandbox:latest")
        )
        with pytest.raises(RuntimeError, match="stop failed"):
            await manager.stop(first)
        second_task = asyncio.create_task(
            manager.acquire(
                SandboxSpec(tenant_id="tenant-b", conversation_id="chat-1", image="soveren-codex-sandbox:latest")
            )
        )
        await asyncio.sleep(0)
        assert not second_task.done()
        second_task.cancel()
        await asyncio.gather(second_task, return_exceptions=True)

    asyncio.run(run())


def test_docker_sandbox_manager_releases_capacity_when_container_is_already_gone():
    class FastDockerSandboxManager(DockerSandboxManager):
        async def _acquire_locked(self, spec, *, tenant_key, conversation_key):
            return SandboxHandle(
                id=f"container-{conversation_key[:8]}",
                name=f"sandbox-{conversation_key[:8]}",
                tenant_id=spec.tenant_id,
                conversation_id=spec.conversation_id,
                workspace_root=spec.workspace_root,
                codex_home=spec.codex_home,
                metadata={"tenant_key": tenant_key, "conversation_key": conversation_key},
            )

    async def run():
        manager = FastDockerSandboxManager(
            runner=FakeDockerRunner([CommandResult(returncode=1, stderr="Error: No such container: missing")]),
            max_active_sandboxes=1,
        )
        first = await manager.acquire(
            SandboxSpec(tenant_id="tenant-a", conversation_id="chat-1", image="soveren-codex-sandbox:latest")
        )
        await manager.stop(first)
        second = await asyncio.wait_for(
            manager.acquire(
                SandboxSpec(tenant_id="tenant-b", conversation_id="chat-1", image="soveren-codex-sandbox:latest")
            ),
            timeout=1,
        )
        await manager.destroy(second)
        return second

    second = asyncio.run(run())

    assert second.tenant_id == "tenant-b"


def test_docker_sandbox_manager_rejects_host_network():
    manager = DockerSandboxManager(runner=FakeDockerRunner([]))

    with pytest.raises(ValueError, match="network"):
        asyncio.run(
            manager.acquire(
                SandboxSpec(
                    tenant_id="tenant-a",
                    conversation_id="chat-1",
                    image="soveren-codex-sandbox:latest",
                    network="host",
                )
            )
        )


def test_docker_sandbox_manager_respects_explicit_empty_network_allowlist():
    manager = DockerSandboxManager(
        runner=FakeDockerRunner([]),
        allowed_networks=frozenset(),
    )

    with pytest.raises(ValueError, match="network"):
        asyncio.run(
            manager.acquire(
                SandboxSpec(
                    tenant_id="tenant-a",
                    conversation_id="chat-1",
                    image="soveren-codex-sandbox:latest",
                    network="none",
                )
            )
        )


def test_docker_sandbox_manager_builds_interactive_exec_command():
    manager = DockerSandboxManager()
    handle = SandboxHandle(
        id="container-123",
        name="soveren-sandbox",
        tenant_id="tenant-a",
        conversation_id="chat-1",
        workspace_root="/workspace",
        codex_home="/codex-home",
    )

    command = manager.exec_command(
        handle,
        ["codex", "app-server", "--listen", "stdio://"],
        env={"CODEX_HOME": "/codex-home"},
        workdir="/workspace",
    )

    assert command == [
        "docker",
        "exec",
        "-i",
        "-w",
        "/workspace",
        "-e",
        "CODEX_HOME=/codex-home",
        "container-123",
        "codex",
        "app-server",
        "--listen",
        "stdio://",
    ]


class FakeSandboxManager:
    def __init__(self) -> None:
        self.handle = SandboxHandle(
            id="container-123",
            name="soveren-sandbox-abc",
            tenant_id="tenant-a",
            conversation_id="chat-1",
            workspace_root="/workspace",
            codex_home="/codex-home",
            metadata={
                "runtime": "docker",
                "tenant_key": "abc",
                "credential_broker_host": "soveren-credential-broker",
            },
        )
        self.acquired: list[SandboxSpec] = []
        self.directories: list[str] = []
        self.commands: list[list[str]] = []
        self.command_inputs: list[bytes | None] = []
        self.exec_envs: list[dict[str, str]] = []
        self.stopped: list[SandboxHandle] = []
        self.destroyed: list[SandboxHandle] = []
        self.broker_calls: list[tuple[bytes, CredentialBrokerPolicy]] = []
        self.http_broker_calls: list[tuple[bytes, HttpCredentialBinding]] = []
        self.http_broker_revocations: list[tuple[str, CredentialBindingScope]] = []

    async def acquire(self, spec: SandboxSpec) -> SandboxHandle:
        self.acquired.append(spec)
        return self.handle

    async def destroy(self, handle: SandboxHandle) -> None:
        self.destroyed.append(handle)

    async def stop(self, handle: SandboxHandle) -> None:
        self.stopped.append(handle)

    async def ensure_directory(self, handle: SandboxHandle, path: str) -> None:
        self.directories.append(path)

    async def provision_credential_broker(
        self,
        handle: SandboxHandle,
        *,
        api_key: bytes,
        policy: CredentialBrokerPolicy,
    ) -> CredentialBrokerEndpoint:
        self.broker_calls.append((api_key, policy))
        return CredentialBrokerEndpoint(
            base_url="http://soveren-credential-broker:8080/v1",
            network_ip="172.30.0.4",
        )

    async def provision_http_credential(
        self,
        handle: SandboxHandle,
        *,
        credential: bytes,
        binding: HttpCredentialBinding,
    ) -> CredentialBrokerCapability:
        self.http_broker_calls.append((credential, binding))
        return CredentialBrokerCapability(
            base_url="http://soveren-credential-broker:8080/bindings/capability",
            network_ip="172.30.0.4",
        )

    async def revoke_http_credential(
        self,
        handle: SandboxHandle,
        *,
        name: str,
        scope: CredentialBindingScope = "conversation",
    ) -> None:
        self.http_broker_revocations.append((name, scope))

    async def run_command(
        self,
        handle: SandboxHandle,
        command: list[str],
        *,
        input_data: bytes | None = None,
        env: dict[str, str] | None = None,
        workdir: str | None = None,
    ) -> None:
        self.commands.append(["run", handle.id, *command])
        self.command_inputs.append(input_data)

    def exec_command(
        self,
        handle: SandboxHandle,
        command: list[str],
        *,
        env: dict[str, str] | None = None,
        workdir: str | None = None,
        interactive: bool = True,
    ) -> list[str]:
        built = ["docker", "exec", "-i", handle.id, *command]
        self.commands.append(built)
        self.exec_envs.append(dict(env or {}))
        return built


class FakeCodexClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.last_turns: dict[str, object] = {}
        self.released_turns: list[tuple[str, str]] = []
        self.released_threads: list[str] = []

    async def request(self, method: str, params: dict):
        self.calls.append((method, params))
        if method == "thread/start":
            return {"thread": {"id": "thread-1"}, "modelProvider": "openai", "cwd": params["cwd"]}
        if method == "turn/start":
            return {"turn": {"id": "turn-1"}}
        if method == "thread/archive":
            return {}
        if method == "thread/read":
            return {"thread": {"items": [{"role": "assistant", "text": "sandbox summary"}]}}
        return {}

    async def close(self) -> None:
        return None

    def set_last_turn(self, thread_id: str, turn_id: str):
        state = SimpleNamespace(turn_id=turn_id)
        self.last_turns[thread_id] = state
        return state

    def last_turn(self, thread_id: str):
        return self.last_turns.get(thread_id)

    def release_turn(self, thread_id: str, turn_id: str) -> None:
        self.released_turns.append((thread_id, turn_id))
        state = self.last_turns.get(thread_id)
        if state is not None and getattr(state, "turn_id", None) == turn_id:
            self.last_turns.pop(thread_id, None)

    def release_thread(self, thread_id: str) -> None:
        self.released_threads.append(thread_id)
        self.last_turns.pop(thread_id, None)


def test_sandboxed_codex_backend_opens_thread_inside_sandbox():
    manager = FakeSandboxManager()
    client = FakeCodexClient()
    backend = SandboxedCodexAppServerBackend(
        sandbox_manager=manager,
        sandbox_spec=SandboxSpec(tenant_id="tenant-a", conversation_id="chat-1", image="soveren-codex-sandbox:latest"),
        client=client,
    )

    opened = asyncio.run(
        backend.open(
            _sandbox_open_spec(
                backend,
                cwd="/host/path/ignored",
                metadata={"sandbox_cwd": "/workspace/chat-a"},
            )
        )
    )

    assert opened.backend_session_id == "thread-1"
    assert manager.directories == ["/workspace", "/codex-home", "/workspace/chat-a"]
    assert manager.commands == [
        ["docker", "exec", "-i", "container-123", "codex", "app-server", "--listen", "stdio://"]
    ]
    assert client.calls[0][0] == "thread/start"
    assert client.calls[0][1]["cwd"] == "/workspace/chat-a"
    assert opened.metadata["runtime"] == "codex"
    assert opened.metadata["isolation"] == "docker"
    assert "sandbox_name" not in opened.metadata
    assert "sandbox_tenant_key" not in opened.metadata


@pytest.mark.parametrize(
    "scope",
    [
        None,
        ConversationScope(tenant_id="tenant-a", source_id="chat-2"),
        ConversationScope(tenant_id="tenant-b", source_id="chat-1"),
    ],
)
def test_sandboxed_codex_backend_rejects_untrusted_scope_before_acquire(scope):
    manager = FakeSandboxManager()
    backend = SandboxedCodexAppServerBackend(
        sandbox_manager=manager,
        sandbox_spec=SandboxSpec(
            tenant_id="tenant-a",
            conversation_id="chat-1",
            image="soveren-codex-sandbox:latest",
        ),
        client=FakeCodexClient(),
    )

    with pytest.raises(TenantBoundaryError):
        asyncio.run(
            backend.open(
                OpenSpec(
                    kind="codex_cli",
                    cwd="/ignored",
                    conversation_scope=scope,
                )
            )
        )

    assert manager.acquired == []


def test_sandboxed_codex_backend_launches_with_non_secret_broker_provider_overrides():
    async def run():
        manager = FakeSandboxManager()
        backend = SandboxedCodexAppServerBackend(
            sandbox_manager=manager,
            sandbox_spec=SandboxSpec(
                tenant_id="tenant-a",
                conversation_id="chat-1",
                image="soveren-codex-sandbox:latest",
            ),
            credentials=CodexApiKeyCredentials("sk-provider-secret"),
            client=FakeCodexClient(),
        )
        await backend.open(_sandbox_open_spec(backend))
        await backend.destroy_sandbox()
        return manager

    manager = asyncio.run(run())

    command = " ".join(manager.commands[0])
    assert 'model_provider="soveren_credential_broker"' in command
    assert "model_providers.soveren_credential_broker.requires_openai_auth=false" in command
    assert "model_providers.soveren_credential_broker.supports_websockets=false" in command
    assert "sk-provider-secret" not in command
    assert manager.exec_envs == [
        {
            "CODEX_HOME": "/codex-home",
            "NO_PROXY": "soveren-credential-broker,172.30.0.4",
            "no_proxy": "soveren-credential-broker,172.30.0.4",
        }
    ]
    assert manager.destroyed[0].metadata["credential_broker_ip"] == "172.30.0.4"


def test_sandboxed_codex_backend_provisions_and_revokes_protected_http_credentials():
    async def run():
        manager = FakeSandboxManager()
        backend = SandboxedCodexAppServerBackend(
            sandbox_manager=manager,
            sandbox_spec=SandboxSpec(
                tenant_id="tenant-a",
                conversation_id="chat-1",
                image="soveren-codex-sandbox:latest",
            ),
            client=FakeCodexClient(),
        )
        binding = HttpCredentialBinding(
            name="clickup",
            target_origin="https://api.clickup.com",
            credential_header="Authorization",
            credential_prefix="",
            allowed_methods=("GET", "POST"),
            allowed_path_prefixes=("/api/v2",),
        )
        capability = await backend.provision_http_credential(b"protected-token", binding)
        await backend.revoke_http_credential("clickup")
        await backend.destroy_sandbox()
        return manager, binding, capability

    manager, binding, capability = asyncio.run(run())

    assert capability.network_ip == "172.30.0.4"
    assert manager.http_broker_calls == [(b"protected-token", binding)]
    assert manager.http_broker_revocations == [("clickup", "conversation")]
    assert manager.exec_envs == [
        {
            "CODEX_HOME": "/codex-home",
            "NO_PROXY": "soveren-credential-broker",
            "no_proxy": "soveren-credential-broker",
        }
    ]
    assert manager.destroyed == [manager.handle]


def test_sandboxed_codex_backend_revokes_after_idle_stop_without_reacquiring_sandbox():
    async def run():
        manager = FakeSandboxManager()
        backend = SandboxedCodexAppServerBackend(
            sandbox_manager=manager,
            sandbox_spec=SandboxSpec(
                tenant_id="tenant-a",
                conversation_id="chat-1",
                image="soveren-codex-sandbox:latest",
            ),
            client=FakeCodexClient(),
            idle_stop_after_s=0,
        )
        await backend.provision_http_credential(
            b"protected-token",
            HttpCredentialBinding(
                name="clickup",
                target_origin="https://api.clickup.com",
                allowed_methods=("GET",),
                allowed_path_prefixes=("/api/v2",),
            ),
        )
        idle_stop = backend._idle_stop_task
        assert idle_stop is not None
        await idle_stop

        await backend.revoke_http_credential("clickup")
        return manager

    manager = asyncio.run(run())

    assert len(manager.acquired) == 1
    assert manager.stopped == [manager.handle]
    assert manager.http_broker_revocations == [("clickup", "conversation")]


def test_sandboxed_codex_backend_rejects_unsupported_credential_manager_before_acquire():
    class ExecutionOnlyManager:
        def __init__(self) -> None:
            self.acquired = False

        async def acquire(self, spec: SandboxSpec) -> SandboxHandle:
            self.acquired = True
            raise AssertionError("credential capability check must run before sandbox acquisition")

    manager = ExecutionOnlyManager()
    backend = SandboxedCodexAppServerBackend(
        sandbox_manager=manager,
        sandbox_spec=SandboxSpec(
            tenant_id="tenant-a",
            conversation_id="chat-1",
            image="soveren-codex-sandbox:latest",
        ),
        client=FakeCodexClient(),
    )
    binding = HttpCredentialBinding(
        name="clickup",
        target_origin="https://api.clickup.com",
        allowed_methods=("GET",),
        allowed_path_prefixes=("/api/v2",),
    )

    with pytest.raises(RuntimeError, match="does not support"):
        asyncio.run(backend.provision_http_credential(b"protected-token", binding))

    assert manager.acquired is False


def test_sandboxed_codex_backend_single_flights_concurrent_open_and_stops_on_shutdown():
    async def run():
        manager = FakeSandboxManager()
        backend = SandboxedCodexAppServerBackend(
            sandbox_manager=manager,
            sandbox_spec=SandboxSpec(
                tenant_id="tenant-a", conversation_id="chat-1", image="soveren-codex-sandbox:latest"
            ),
            client=FakeCodexClient(),
        )
        opened = await asyncio.gather(*(backend.open(_sandbox_open_spec(backend)) for _ in range(10)))
        await backend.shutdown()
        return manager, opened

    manager, opened = asyncio.run(run())

    assert len(manager.acquired) == 1
    assert len(manager.commands) == 1
    assert len(opened) == 10
    assert manager.stopped == [manager.handle]
    assert manager.destroyed == []


def test_sandboxed_codex_backend_stops_container_when_app_server_shutdown_fails():
    class FailingCloseCodexClient(FakeCodexClient):
        async def close(self) -> None:
            raise RuntimeError("app-server close failed")

    async def run():
        manager = FakeSandboxManager()
        backend = SandboxedCodexAppServerBackend(
            sandbox_manager=manager,
            sandbox_spec=SandboxSpec(
                tenant_id="tenant-a", conversation_id="chat-1", image="soveren-codex-sandbox:latest"
            ),
            client=FailingCloseCodexClient(),
        )
        await backend.open(_sandbox_open_spec(backend))
        with pytest.raises(ExceptionGroup, match="sandboxed Codex shutdown failed"):
            await backend.shutdown()
        return manager

    manager = asyncio.run(run())

    assert manager.stopped == [manager.handle]


def test_sandboxed_codex_backend_cleans_up_and_discards_backend_on_cancelled_shutdown():
    class CancelledCloseCodexClient(FakeCodexClient):
        async def close(self) -> None:
            raise asyncio.CancelledError()

    async def run():
        manager = FakeSandboxManager()
        backend = SandboxedCodexAppServerBackend(
            sandbox_manager=manager,
            sandbox_spec=SandboxSpec(
                tenant_id="tenant-a", conversation_id="chat-1", image="soveren-codex-sandbox:latest"
            ),
            client=CancelledCloseCodexClient(),
        )
        await backend.open(_sandbox_open_spec(backend))
        with pytest.raises(BaseExceptionGroup, match="sandboxed Codex shutdown failed"):
            await backend.shutdown()
        return manager, backend

    manager, backend = asyncio.run(run())

    assert manager.stopped == [manager.handle]
    assert backend._backend is None
    assert backend._handle is manager.handle


def test_sandboxed_codex_backend_resumes_persisted_thread_after_process_restart():
    async def run():
        manager = FakeSandboxManager()
        client = FakeCodexClient()
        backend = SandboxedCodexAppServerBackend(
            sandbox_manager=manager,
            sandbox_spec=SandboxSpec(
                tenant_id="tenant-a", conversation_id="chat-1", image="soveren-codex-sandbox:latest"
            ),
            client=client,
        )
        await backend.send("thread-existing", "continue")
        return manager, client

    manager, client = asyncio.run(run())

    assert len(manager.acquired) == 1
    assert client.calls == [
        (
            "thread/resume",
            {
                "threadId": "thread-existing",
                "approvalPolicy": "never",
                "sandbox": "workspace-write",
            },
        ),
        (
            "turn/start",
            {"threadId": "thread-existing", "input": [{"type": "text", "text": "continue"}]},
        ),
    ]


def test_sandboxed_codex_backend_reacquires_runtime_before_a_new_turn():
    async def run():
        manager = FakeSandboxManager()
        client = FakeCodexClient()
        backend = SandboxedCodexAppServerBackend(
            sandbox_manager=manager,
            sandbox_spec=SandboxSpec(
                tenant_id="tenant-a", conversation_id="chat-1", image="soveren-codex-sandbox:latest"
            ),
            client=client,
        )
        opened = await backend.open(_sandbox_open_spec(backend))
        first_backend = backend._backend

        await backend.send(opened.backend_session_id, "continue")
        return manager, client, backend, first_backend

    manager, client, backend, first_backend = asyncio.run(run())

    assert len(manager.acquired) == 2
    assert backend._backend is first_backend
    assert client.calls[-1] == (
        "turn/start",
        {"threadId": "thread-1", "input": [{"type": "text", "text": "continue"}]},
    )


def test_sandboxed_codex_backend_does_not_start_turn_when_runtime_reacquire_fails():
    class FailingReacquireManager(FakeSandboxManager):
        async def acquire(self, spec: SandboxSpec) -> SandboxHandle:
            self.acquired.append(spec)
            if len(self.acquired) == 2:
                raise RuntimeError("broker recovery failed")
            return self.handle

    async def run():
        manager = FailingReacquireManager()
        client = FakeCodexClient()
        backend = SandboxedCodexAppServerBackend(
            sandbox_manager=manager,
            sandbox_spec=SandboxSpec(
                tenant_id="tenant-a", conversation_id="chat-1", image="soveren-codex-sandbox:latest"
            ),
            client=client,
        )
        opened = await backend.open(_sandbox_open_spec(backend))
        with pytest.raises(RuntimeError, match="broker recovery failed"):
            await backend.send(opened.backend_session_id, "must not start")
        return manager, client

    manager, client = asyncio.run(run())

    assert len(manager.acquired) == 2
    assert [method for method, _ in client.calls] == ["thread/start"]


def test_sandboxed_codex_backend_rebuilds_app_server_for_a_replaced_container():
    async def run():
        manager = FakeSandboxManager()
        client = FakeCodexClient()
        backend = SandboxedCodexAppServerBackend(
            sandbox_manager=manager,
            sandbox_spec=SandboxSpec(
                tenant_id="tenant-a", conversation_id="chat-1", image="soveren-codex-sandbox:latest"
            ),
            client=client,
        )
        opened = await backend.open(_sandbox_open_spec(backend))
        first_backend = backend._backend
        manager.handle = replace(manager.handle, id="container-456")

        await backend.send(opened.backend_session_id, "continue")
        return manager, backend, first_backend

    manager, backend, first_backend = asyncio.run(run())

    assert backend._backend is not first_backend
    assert len(manager.acquired) == 2
    assert manager.commands[-1][:4] == ["docker", "exec", "-i", "container-456"]


def test_sandboxed_codex_inspector_preserves_tenant_boundary():
    async def run():
        manager = FakeSandboxManager()
        backend = SandboxedCodexAppServerBackend(
            sandbox_manager=manager,
            sandbox_spec=SandboxSpec(
                tenant_id="tenant-a", conversation_id="chat-1", image="soveren-codex-sandbox:latest"
            ),
            client=FakeCodexClient(),
            idle_stop_after_s=None,
        )
        inspector = CodexThreadInspector(backend)
        ensure_tenant_boundary(inspector, "tenant-a", resource_name="Codex inspector")
        ensure_conversation_boundary(
            inspector,
            "tenant-a",
            "chat-1",
            resource_name="Codex inspector",
        )
        with pytest.raises(TenantBoundaryError, match="tenant-a.*tenant-b"):
            ensure_tenant_boundary(inspector, "tenant-b", resource_name="Codex inspector")
        with pytest.raises(TenantBoundaryError, match="chat-1.*chat-2"):
            ensure_conversation_boundary(
                inspector,
                "tenant-a",
                "chat-2",
                resource_name="Codex inspector",
            )
        inspection = await inspector.inspect(
            RuntimeSession(
                id="rs-1",
                tenant_id="tenant-a",
                source_id="chat-1",
                kind="codex_cli",
                backend="codex",
                backend_session_id="thread-existing",
                status="idle",
            )
        )
        await backend.shutdown()
        return manager, inspection

    manager, inspection = asyncio.run(run())

    assert inspection is not None
    assert inspection.payload_text == "sandbox summary"
    assert manager.stopped == [manager.handle]


def test_sandboxed_codex_backend_stops_sandbox_when_credential_provisioning_fails():
    class FailingCredentials:
        async def provision(self, manager, handle):
            raise RuntimeError("credentials unavailable")

    async def run():
        manager = FakeSandboxManager()
        backend = SandboxedCodexAppServerBackend(
            sandbox_manager=manager,
            sandbox_spec=SandboxSpec(
                tenant_id="tenant-a", conversation_id="chat-1", image="soveren-codex-sandbox:latest"
            ),
            credentials=FailingCredentials(),
            client=FakeCodexClient(),
        )
        with pytest.raises(RuntimeError, match="credentials unavailable"):
            await backend.send("thread-existing", "continue")
        return manager

    manager = asyncio.run(run())

    assert manager.stopped == [manager.handle]


def test_sandboxed_codex_backend_stops_after_failed_thread_start():
    class FailingThreadStartClient(FakeCodexClient):
        async def request(self, method: str, params: dict):
            if method == "thread/start":
                raise RuntimeError("thread start failed")
            return await super().request(method, params)

    async def run():
        manager = FakeSandboxManager()
        backend = SandboxedCodexAppServerBackend(
            sandbox_manager=manager,
            sandbox_spec=SandboxSpec(
                tenant_id="tenant-a", conversation_id="chat-1", image="soveren-codex-sandbox:latest"
            ),
            client=FailingThreadStartClient(),
            idle_stop_after_s=0,
        )
        with pytest.raises(RuntimeError, match="thread start failed"):
            await backend.open(_sandbox_open_spec(backend))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        return manager

    manager = asyncio.run(run())

    assert manager.stopped == [manager.handle]


def test_sandboxed_codex_backend_stops_after_last_thread_becomes_idle():
    async def run():
        manager = FakeSandboxManager()
        backend = SandboxedCodexAppServerBackend(
            sandbox_manager=manager,
            sandbox_spec=SandboxSpec(
                tenant_id="tenant-a", conversation_id="chat-1", image="soveren-codex-sandbox:latest"
            ),
            client=FakeCodexClient(),
            idle_stop_after_s=0,
        )
        opened = await backend.open(_sandbox_open_spec(backend))
        await backend.close(opened.backend_session_id)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        return manager

    manager = asyncio.run(run())

    assert manager.stopped == [manager.handle]


def test_sandboxed_codex_abort_releases_thread_and_capacity_when_interrupt_fails():
    class InterruptFailingClient(FakeCodexClient):
        async def request(self, method: str, params: dict):
            if method == "turn/interrupt":
                self.calls.append((method, params))
                raise RuntimeError("interrupt failed")
            return await super().request(method, params)

    async def run():
        manager = FakeSandboxManager()
        client = InterruptFailingClient()
        backend = SandboxedCodexAppServerBackend(
            sandbox_manager=manager,
            sandbox_spec=SandboxSpec(
                tenant_id="tenant-a",
                conversation_id="chat-1",
                image="soveren-codex-sandbox:latest",
            ),
            client=client,
            idle_stop_after_s=0,
        )
        opened = await backend.open(_sandbox_open_spec(backend))
        receipt = await backend.send(opened.backend_session_id, "long task")

        with pytest.raises(RuntimeError, match="interrupt failed"):
            await backend.abort_delivery(opened.backend_session_id, receipt)

        assert opened.backend_session_id not in backend._active_thread_ids
        idle_stop = backend._idle_stop_task
        assert idle_stop is not None
        await idle_stop
        return manager, client

    manager, client = asyncio.run(run())

    assert client.calls[-2:] == [
        ("turn/interrupt", {"threadId": "thread-1", "turnId": "turn-1"}),
        ("thread/archive", {"threadId": "thread-1"}),
    ]
    assert client.released_threads == ["thread-1"]
    assert manager.stopped == [manager.handle]


def test_sandboxed_codex_backend_does_not_idle_stop_during_inflight_open():
    class BlockingSecondThreadStartClient(FakeCodexClient):
        def __init__(self) -> None:
            super().__init__()
            self.thread_start_count = 0
            self.second_start_entered = asyncio.Event()
            self.allow_second_start = asyncio.Event()
            self.closed = asyncio.Event()

        async def request(self, method: str, params: dict):
            if method == "thread/start":
                self.thread_start_count += 1
                if self.thread_start_count == 1:
                    return {"thread": {"id": "thread-1"}, "cwd": params["cwd"]}
                self.second_start_entered.set()
                await self.allow_second_start.wait()
                if self.closed.is_set():
                    raise RuntimeError("client closed during thread/start")
                return {"thread": {"id": "thread-2"}, "cwd": params["cwd"]}
            return await super().request(method, params)

        async def close(self) -> None:
            self.closed.set()

    async def run():
        manager = FakeSandboxManager()
        client = BlockingSecondThreadStartClient()
        backend = SandboxedCodexAppServerBackend(
            sandbox_manager=manager,
            sandbox_spec=SandboxSpec(
                tenant_id="tenant-a", conversation_id="chat-1", image="soveren-codex-sandbox:latest"
            ),
            client=client,
            idle_stop_after_s=0,
        )
        first = await backend.open(_sandbox_open_spec(backend))
        second_open = asyncio.create_task(backend.open(_sandbox_open_spec(backend)))
        await client.second_start_entered.wait()

        await backend.close(first.backend_session_id)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert manager.stopped == []
        assert not client.closed.is_set()

        client.allow_second_start.set()
        second = await second_open
        assert second.backend_session_id == "thread-2"
        await backend.close(second.backend_session_id)
        idle_stop = backend._idle_stop_task
        assert idle_stop is not None
        await idle_stop
        assert client.closed.is_set()
        return manager

    manager = asyncio.run(run())

    assert manager.stopped == [manager.handle]


def test_sandboxed_codex_backend_waits_for_idle_shutdown_before_reactivation():
    class BlockingCloseCodexClient(FakeCodexClient):
        def __init__(self) -> None:
            super().__init__()
            self.close_started = asyncio.Event()
            self.allow_close = asyncio.Event()

        async def close(self) -> None:
            self.close_started.set()
            await self.allow_close.wait()

    async def run():
        manager = FakeSandboxManager()
        client = BlockingCloseCodexClient()
        backend = SandboxedCodexAppServerBackend(
            sandbox_manager=manager,
            sandbox_spec=SandboxSpec(
                tenant_id="tenant-a", conversation_id="chat-1", image="soveren-codex-sandbox:latest"
            ),
            client=client,
            idle_stop_after_s=0,
        )
        opened = await backend.open(_sandbox_open_spec(backend))
        first_backend = backend._backend
        await backend.close(opened.backend_session_id)
        await client.close_started.wait()

        send_task = asyncio.create_task(backend.send("thread-existing", "continue"))
        await asyncio.sleep(0)
        assert not send_task.done()

        client.allow_close.set()
        receipt = await send_task
        assert receipt is not None
        assert backend._backend is not first_backend
        await backend.shutdown()
        return manager

    manager = asyncio.run(run())

    assert len(manager.acquired) == 2
    assert manager.stopped == [manager.handle, manager.handle]


def test_codex_api_key_is_brokered_without_entering_the_sandbox(tmp_path):
    auth_path = tmp_path / "auth.json"
    auth_path.write_text('{"tokens":{"access_token":"secret"}}')

    async def run():
        manager = FakeSandboxManager()
        await CodexAuthFileCredentials(auth_path).provision(manager, manager.handle)
        api_credentials = CodexApiKeyCredentials("sk-secret")
        provisioning = await api_credentials.provision(manager, manager.handle)
        return manager, api_credentials, provisioning

    manager, api_credentials, provisioning = asyncio.run(run())

    assert manager.command_inputs == [auth_path.read_bytes()]
    assert manager.broker_calls == [(b"sk-secret", CredentialBrokerPolicy())]
    assert "secret" not in repr(api_credentials)
    assert all("secret" not in " ".join(command) for command in manager.commands)
    assert 'test -s "$CODEX_HOME/auth.json"' in " ".join(manager.commands[0])
    assert provisioning.sandbox_metadata == (("credential_broker_ip", "172.30.0.4"),)
    assert provisioning.launch_env == (
        ("NO_PROXY", "soveren-credential-broker,172.30.0.4"),
        ("no_proxy", "soveren-credential-broker,172.30.0.4"),
    )
    overrides = " ".join(provisioning.config_overrides)
    assert "soveren_credential_broker" in overrides
    assert "http://soveren-credential-broker:8080/v1" in overrides
    assert "sk-secret" not in overrides


def test_create_sandboxed_codex_backend_uses_profile_and_registers_backend():
    manager = FakeSandboxManager()
    registry = SessionBackendRegistry()
    collaboration_mode = CodexCollaborationMode(mode="default", model="gpt-5.4")

    backend = create_sandboxed_codex_backend(
        tenant_id="tenant-a",
        source_id="chat-a",
        credentials=ExistingCodexCredentials(),
        resources="small",
        collaboration_mode=collaboration_mode,
        session_backends=registry,
        sandbox_manager=manager,
    )
    second_backend = create_sandboxed_codex_backend(
        tenant_id="tenant-b",
        source_id="chat-b",
        credentials=ExistingCodexCredentials(),
        resources="small",
        session_backends=registry,
        sandbox_manager=manager,
    )

    assert registry.require(backend.name) is backend
    assert backend.sandbox_manager is manager
    assert backend.collaboration_mode is collaboration_mode
    assert backend.name == "codex:af127ba918ceb498557e652e"
    assert registry.require(second_backend.name) is second_backend
    assert second_backend.sandbox_manager is manager
    assert second_backend.name == "codex:6d6331b1db2787e391acecb5"
    assert backend.sandbox_spec.memory == "512m"
    assert backend.sandbox_spec.disk_limit == "1g"
    assert backend.sandbox_spec.network == "soveren-sandbox-egress"
    assert backend.sandbox_spec.env["HTTPS_PROXY"] == "http://soveren-sandbox-egress:3128"
    assert backend.sandbox_spec.env["NO_PROXY"] == "soveren-credential-broker"


def test_create_sandboxed_codex_backend_rejects_duplicate_conversation():
    manager = FakeSandboxManager()
    registry = SessionBackendRegistry()
    kwargs = {
        "tenant_id": "tenant-a",
        "source_id": "chat-a",
        "credentials": ExistingCodexCredentials(),
        "session_backends": registry,
        "sandbox_manager": manager,
    }

    create_sandboxed_codex_backend(**kwargs)

    with pytest.raises(ValueError, match="session backend already registered"):
        create_sandboxed_codex_backend(**kwargs)


def test_create_sandboxed_codex_backend_requires_process_manager():
    with pytest.raises(TypeError, match="sandbox_manager"):
        create_sandboxed_codex_backend(
            tenant_id="tenant-a",
            source_id="chat-a",
            credentials=ExistingCodexCredentials(),
        )


def test_create_sandboxed_codex_backend_requires_session_registry():
    with pytest.raises(TypeError, match="session_backends"):
        create_sandboxed_codex_backend(
            tenant_id="tenant-a",
            source_id="chat-a",
            credentials=ExistingCodexCredentials(),
            sandbox_manager=FakeSandboxManager(),
        )


def test_create_sandbox_manager_owns_shared_capacity_and_managed_egress():
    manager = create_sandbox_manager(max_active_sandboxes=2)

    assert manager.max_active_sandboxes == 2
    assert manager.recover_orphaned_sandboxes is True
    assert manager.egress is not None
    assert manager.egress.image == "ghcr.io/neureca/soveren-sandbox-egress:0.5.0"
    assert manager.credential_broker is not None
    assert manager.credential_broker.image == "ghcr.io/neureca/soveren-credential-broker:0.5.0"


def _expected_spec_hash(spec: SandboxSpec) -> str:
    payload = {
        "policy_version": "6",
        "conversation_id": spec.conversation_id,
        "image": spec.image,
        "memory": spec.memory,
        "cpus": spec.cpus,
        "pids_limit": spec.pids_limit,
        "disk_limit": spec.disk_limit,
        "tmpfs_size": spec.tmpfs_size,
        "network": spec.network,
        "user": spec.user,
        "workspace_root": spec.workspace_root,
        "codex_home": spec.codex_home,
        "command": list(spec.command),
        "env": dict(sorted(spec.env.items())),
        "labels": dict(sorted(spec.labels.items())),
        "name": spec.name,
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@pytest.mark.parametrize("sandbox_cwd", ["/", "/codex-home", "/workspace/../codex-home"])
def test_sandboxed_codex_backend_rejects_cwd_outside_workspace(sandbox_cwd):
    manager = FakeSandboxManager()
    backend = SandboxedCodexAppServerBackend(
        sandbox_manager=manager,
        sandbox_spec=SandboxSpec(tenant_id="tenant-a", conversation_id="chat-1", image="soveren-codex-sandbox:latest"),
        client=FakeCodexClient(),
    )

    with pytest.raises(ValueError, match="workspace root"):
        asyncio.run(
            backend.open(
                _sandbox_open_spec(
                    backend,
                    cwd="/host/path/ignored",
                    metadata={"sandbox_cwd": sandbox_cwd},
                )
            )
        )
