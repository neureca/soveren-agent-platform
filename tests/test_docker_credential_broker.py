import asyncio
import base64
import json
from types import SimpleNamespace

import pytest

from soveren_agent_platform.sandbox import (
    CredentialUsagePolicy,
    HttpCredentialBinding,
    SandboxHandle,
)
from soveren_agent_platform.sandbox.docker_broker import (
    DockerCredentialBrokerManager,
    DockerCredentialBrokerSpec,
)
from soveren_agent_platform.sandbox.docker_commands import CommandResult


class FakeBrokerHost:
    docker_command = ("docker",)

    def __init__(self, networks: dict[str, tuple[str, str]]) -> None:
        self.egress = SimpleNamespace(
            container_name="soveren-sandbox-egress",
            internal_network="soveren-sandbox-egress",
        )
        self.networks = networks
        self.container_id: str | None = None
        self.container_networks: set[str] = set()
        self.sandbox_running = False
        self.labels: dict[str, str] = {}
        self.calls: list[list[str]] = []
        self.inputs: list[bytes | None] = []
        self.registry_payloads: list[dict[str, object]] = []
        self.firewall_rules: set[tuple[str, ...]] = set()
        self.removed_ids: list[str] = []
        self.fail_next_admin = False
        self.fail_next_rule_removal = False

    async def _run_docker(
        self,
        args: list[str],
        *,
        input_data: bytes | None = None,
    ) -> CommandResult:
        self.calls.append(args)
        self.inputs.append(input_data)
        if args[1:3] == ["network", "ls"]:
            return CommandResult(returncode=0, stdout="\n".join(sorted(self.networks)) + "\n")
        if args[1] == "ps" and "-aq" in args:
            return CommandResult(returncode=0, stdout=f"{self.container_id}\n" if self.container_id else "")
        if args[1] == "ps":
            return CommandResult(returncode=0, stdout="sandbox-123\n" if self.sandbox_running else "")
        if args[1] == "run" and "-d" in args:
            self.container_id = "broker-123"
            self.container_networks = {args[args.index("--network") + 1]}
            for index, value in enumerate(args):
                if value == "--label":
                    key, label_value = args[index + 1].split("=", 1)
                    self.labels[key] = label_value
            return CommandResult(returncode=0, stdout="broker-123\n")
        if args[1] == "inspect":
            template = args[args.index("-f") + 1]
            if template == "{{json .NetworkSettings.Networks}}":
                return CommandResult(
                    returncode=0,
                    stdout=json.dumps({network: {} for network in self.container_networks}),
                )
            if ".State.Health" in template:
                return CommandResult(returncode=0, stdout="healthy\n")
            if ".NetworkSettings.Networks" in template:
                for network, (_, address) in self.networks.items():
                    if json.dumps(network) in template and network in self.container_networks:
                        return CommandResult(returncode=0, stdout=f"{address}\n")
                return CommandResult(returncode=0, stdout="\n")
        if args[1:3] == ["network", "connect"]:
            self.container_networks.add(args[-2])
            return CommandResult(returncode=0)
        if args[1:3] == ["network", "disconnect"]:
            self.container_networks.discard(args[-2])
            return CommandResult(returncode=0)
        if args[1] == "exec" and args[-1] == "admin":
            assert input_data is not None
            if self.fail_next_admin:
                self.fail_next_admin = False
                return CommandResult(returncode=1, stderr="registry update failed")
            self.registry_payloads.append(json.loads(input_data))
            return CommandResult(returncode=0)
        if args[1:3] == ["rm", "-f"]:
            self.removed_ids.append(args[-1])
            if self.container_id == args[-1]:
                self.container_id = None
                self.container_networks.clear()
            return CommandResult(returncode=0)
        return CommandResult(returncode=0)

    async def _run_checked(self, args: list[str]) -> CommandResult:
        result = await self._run_docker(args)
        if result.returncode != 0:
            self._raise_command_error(result)
        return result

    async def _is_running(self, container_id: str) -> bool:
        return self.container_id == container_id

    async def _inspect_label(self, container_id: str, label: str) -> str | None:
        return self.labels.get(label)

    async def _tenant_network_subnet(self, internal_network: str) -> str:
        return self.networks[internal_network][0]

    async def _ensure_iptables_rule(self, rule: list[str], *, force_first: bool = False) -> bool:
        value = tuple(rule)
        created = value not in self.firewall_rules
        self.firewall_rules.add(value)
        return created

    async def _remove_iptables_rule(self, rule: list[str]) -> None:
        if self.fail_next_rule_removal:
            self.fail_next_rule_removal = False
            raise RuntimeError("firewall cleanup failed")
        self.firewall_rules.discard(tuple(rule))

    @staticmethod
    def _raise_command_error(result: CommandResult) -> None:
        raise RuntimeError(result.stderr or result.stdout or "Docker command failed")

    @staticmethod
    def _is_missing_container_result(result: CommandResult) -> bool:
        return "not found" in (result.stderr + result.stdout).lower()


def _handle(*, conversation_key: str, network: str) -> SandboxHandle:
    tenant_key = "a" * 64
    return SandboxHandle(
        id=f"sandbox-{conversation_key[:8]}",
        name="sandbox",
        tenant_id="tenant-a",
        conversation_id=conversation_key,
        workspace_root="/workspace",
        codex_home="/codex-home",
        metadata={
            "runtime": "docker",
            "tenant_key": tenant_key,
            "conversation_key": conversation_key,
            "network": network,
        },
    )


def _binding_by_capability(payload: dict[str, object], capability: str) -> dict[str, object]:
    bindings = payload["bindings"]
    assert isinstance(bindings, list)
    return next(
        binding for binding in bindings if isinstance(binding, dict) and binding.get("capability") == capability
    )


async def _provision_test_http_binding(
    manager: DockerCredentialBrokerManager,
    *,
    network: str,
) -> SandboxHandle:
    handle = _handle(conversation_key="b" * 64, network=network)
    await manager.provision_http(
        handle,
        tenant_key="a" * 64,
        conversation_key="b" * 64,
        credential=b"test-secret",
        binding=HttpCredentialBinding(
            name="github",
            target_origin="https://api.github.com",
            allowed_methods=("GET",),
            allowed_path_prefixes=("/repos",),
        ),
    )
    return handle


def test_docker_broker_keeps_one_tenant_container_and_scopes_http_capabilities():
    async def run():
        network_a = "soveren-sandbox-egress-conversation-a"
        network_b = "soveren-sandbox-egress-conversation-b"
        host = FakeBrokerHost(
            {
                network_a: ("172.30.1.0/24", "172.30.1.4"),
                network_b: ("172.30.2.0/24", "172.30.2.4"),
            }
        )
        manager = DockerCredentialBrokerManager(
            host=host,
            spec=DockerCredentialBrokerSpec(image="soveren-credential-broker:test"),
        )
        handle_a = _handle(conversation_key="b" * 64, network=network_a)
        handle_b = _handle(conversation_key="c" * 64, network=network_b)
        conversation_binding = HttpCredentialBinding(
            name="github",
            target_origin="https://api.github.com",
            credential_prefix="Bearer ",
            allowed_methods=("GET", "POST"),
            allowed_path_prefixes=("/repos",),
        )
        tenant_binding = HttpCredentialBinding(
            name="clickup",
            target_origin="https://api.clickup.com",
            scope="tenant",
            credential_prefix="",
            allowed_methods=("GET", "POST"),
            allowed_path_prefixes=("/api/v2",),
        )

        github_a = await manager.provision_http(
            handle_a,
            tenant_key="a" * 64,
            conversation_key="b" * 64,
            credential=b"github-secret-a",
            binding=conversation_binding,
        )
        clickup = await manager.provision_http(
            handle_a,
            tenant_key="a" * 64,
            conversation_key="b" * 64,
            credential=b"clickup-secret",
            binding=tenant_binding,
        )
        rotated_github_a = await manager.provision_http(
            handle_a,
            tenant_key="a" * 64,
            conversation_key="b" * 64,
            credential=b"github-secret-a-rotated",
            binding=conversation_binding,
        )
        github_b = await manager.provision_http(
            handle_b,
            tenant_key="a" * 64,
            conversation_key="c" * 64,
            credential=b"github-secret-b",
            binding=conversation_binding,
        )
        await manager.revoke_http(
            handle_a,
            tenant_key="a" * 64,
            conversation_key="b" * 64,
            name="github",
            scope="conversation",
        )
        return host, github_a, clickup, rotated_github_a, github_b

    host, github_a, clickup, rotated_github_a, github_b = asyncio.run(run())

    assert github_a.base_url == rotated_github_a.base_url
    assert github_a.base_url != github_b.base_url
    assert github_a.base_url != clickup.base_url
    broker_runs = [call for call in host.calls if call[1] == "run"]
    assert len(broker_runs) == 1
    broker_lookups = [call for call in host.calls if call[1] == "ps" and "-aq" in call]
    assert broker_lookups
    assert all("--no-trunc" in call for call in broker_lookups)
    assert all(
        secret not in " ".join(argument for call in host.calls for argument in call)
        for secret in ("github-secret-a", "github-secret-a-rotated", "github-secret-b", "clickup-secret")
    )
    latest = host.registry_payloads[-1]
    github_a_capability = github_a.base_url.rsplit("/", 1)[-1]
    github_b_capability = github_b.base_url.rsplit("/", 1)[-1]
    clickup_capability = clickup.base_url.rsplit("/", 1)[-1]
    with pytest.raises(StopIteration):
        _binding_by_capability(latest, github_a_capability)
    github_b_binding = _binding_by_capability(latest, github_b_capability)
    clickup_binding = _binding_by_capability(latest, clickup_capability)
    assert github_b_binding["allowed_local_ips"] == ["172.30.2.4"]
    assert clickup_binding["allowed_local_ips"] == ["172.30.1.4", "172.30.2.4"]
    assert base64.b64decode(github_b_binding["secret"]) == b"github-secret-b"
    assert base64.b64decode(clickup_binding["secret"]) == b"clickup-secret"


def test_docker_broker_decommissions_on_uncertain_registry_update():
    async def run():
        network = "soveren-sandbox-egress-conversation-a"
        host = FakeBrokerHost({network: ("172.30.1.0/24", "172.30.1.4")})
        manager = DockerCredentialBrokerManager(
            host=host,
            spec=DockerCredentialBrokerSpec(image="soveren-credential-broker:test"),
        )
        handle = _handle(conversation_key="b" * 64, network=network)
        binding = HttpCredentialBinding(
            name="github",
            target_origin="https://api.github.com",
            allowed_methods=("GET",),
            allowed_path_prefixes=("/repos",),
        )
        await manager.provision_http(
            handle,
            tenant_key="a" * 64,
            conversation_key="b" * 64,
            credential=b"first-secret",
            binding=binding,
        )
        host.fail_next_admin = True
        with pytest.raises(RuntimeError, match="registry update failed"):
            await manager.provision_http(
                handle,
                tenant_key="a" * 64,
                conversation_key="b" * 64,
                credential=b"rotated-secret",
                binding=binding,
            )
        return host

    host = asyncio.run(run())

    assert host.container_id is None
    assert host.removed_ids == ["broker-123"]
    assert not host.firewall_rules


def test_docker_broker_revocation_falls_back_to_container_removal():
    async def run():
        network = "soveren-sandbox-egress-conversation-a"
        host = FakeBrokerHost({network: ("172.30.1.0/24", "172.30.1.4")})
        manager = DockerCredentialBrokerManager(
            host=host,
            spec=DockerCredentialBrokerSpec(image="soveren-credential-broker:test"),
        )
        handle = _handle(conversation_key="b" * 64, network=network)
        binding = HttpCredentialBinding(
            name="github",
            target_origin="https://api.github.com",
            allowed_methods=("GET",),
            allowed_path_prefixes=("/repos",),
        )
        await manager.provision_http(
            handle,
            tenant_key="a" * 64,
            conversation_key="b" * 64,
            credential=b"first-secret",
            binding=binding,
        )
        host.fail_next_admin = True
        await manager.revoke_http(
            handle,
            tenant_key="a" * 64,
            conversation_key="b" * 64,
            name="github",
            scope="conversation",
        )
        return host

    host = asyncio.run(run())

    assert host.container_id is None
    assert host.removed_ids == ["broker-123"]
    assert not host.firewall_rules


def test_docker_broker_revocation_retry_finishes_firewall_cleanup_after_removal_failure():
    async def run():
        network = "soveren-sandbox-egress-conversation-a"
        host = FakeBrokerHost({network: ("172.30.1.0/24", "172.30.1.4")})
        manager = DockerCredentialBrokerManager(
            host=host,
            spec=DockerCredentialBrokerSpec(image="soveren-credential-broker:test"),
        )
        handle = _handle(conversation_key="b" * 64, network=network)
        await manager.provision_http(
            handle,
            tenant_key="a" * 64,
            conversation_key="b" * 64,
            credential=b"first-secret",
            binding=HttpCredentialBinding(
                name="github",
                target_origin="https://api.github.com",
                allowed_methods=("GET",),
                allowed_path_prefixes=("/repos",),
            ),
        )

        host.fail_next_rule_removal = True
        with pytest.raises(RuntimeError, match="firewall cleanup failed"):
            await manager.revoke_http(
                handle,
                tenant_key="a" * 64,
                conversation_key="b" * 64,
                name="github",
                scope="conversation",
            )

        assert host.container_id is None
        assert host.firewall_rules
        await manager.revoke_http(
            handle,
            tenant_key="a" * 64,
            conversation_key="b" * 64,
            name="github",
            scope="conversation",
        )
        return host

    host = asyncio.run(run())

    assert host.removed_ids == ["broker-123"]
    assert not host.firewall_rules


def test_docker_broker_inactive_cleanup_retry_removes_stale_firewall_rules():
    async def run():
        network = "soveren-sandbox-egress-conversation-a"
        host = FakeBrokerHost({network: ("172.30.1.0/24", "172.30.1.4")})
        manager = DockerCredentialBrokerManager(
            host=host,
            spec=DockerCredentialBrokerSpec(image="soveren-credential-broker:test"),
        )
        await _provision_test_http_binding(manager, network=network)

        host.fail_next_rule_removal = True
        with pytest.raises(RuntimeError, match="firewall cleanup failed"):
            await manager.remove_inactive("a" * 64)
        assert host.container_id is None
        assert host.firewall_rules

        host.sandbox_running = True
        await manager.remove_inactive("a" * 64)
        return host, manager, network

    host, manager, network = asyncio.run(run())

    assert not host.firewall_rules
    assert "a" * 64 not in manager._container_ids
    assert network not in manager._network_ips


def test_docker_broker_reconciles_stale_firewall_rules_before_restart():
    async def run():
        network = "soveren-sandbox-egress-conversation-a"
        host = FakeBrokerHost({network: ("172.30.1.0/24", "172.30.1.4")})
        manager = DockerCredentialBrokerManager(
            host=host,
            spec=DockerCredentialBrokerSpec(image="soveren-credential-broker:test"),
        )
        await _provision_test_http_binding(manager, network=network)

        host.fail_next_rule_removal = True
        with pytest.raises(RuntimeError, match="firewall cleanup failed"):
            await manager.remove_inactive("a" * 64)
        host.networks[network] = ("172.30.1.0/24", "172.30.1.5")

        await manager.prepare_tenant_network("a" * 64, network)
        return host, manager, network

    host, manager, network = asyncio.run(run())

    rule_values = {value for rule in host.firewall_rules for value in rule}
    assert "172.30.1.4/32" not in rule_values
    assert "172.30.1.5/32" in rule_values
    assert manager._network_ips[network] == "172.30.1.5"


def test_docker_broker_unowned_cleanup_retry_uses_discovered_network_policy():
    async def run():
        network = "soveren-sandbox-egress-conversation-a"
        host = FakeBrokerHost({network: ("172.30.1.0/24", "172.30.1.4")})
        owner = DockerCredentialBrokerManager(
            host=host,
            spec=DockerCredentialBrokerSpec(image="soveren-credential-broker:test"),
        )
        await _provision_test_http_binding(owner, network=network)
        recovering = DockerCredentialBrokerManager(
            host=host,
            spec=DockerCredentialBrokerSpec(image="soveren-credential-broker:test"),
        )

        host.fail_next_rule_removal = True
        with pytest.raises(RuntimeError, match="firewall cleanup failed"):
            await recovering.remove_unowned("a" * 64)
        assert host.container_id is None
        assert recovering._network_ips[network] == "172.30.1.4"

        await recovering.remove_unowned("a" * 64)
        return host, recovering, network

    host, recovering, network = asyncio.run(run())

    assert not host.firewall_rules
    assert network not in recovering._network_ips


def test_docker_broker_removes_registry_owned_by_a_previous_control_plane():
    async def run():
        network = "soveren-sandbox-egress-conversation-a"
        host = FakeBrokerHost({network: ("172.30.1.0/24", "172.30.1.4")})
        host.container_id = "broker-orphaned"
        host.container_networks = {network}
        manager = DockerCredentialBrokerManager(
            host=host,
            spec=DockerCredentialBrokerSpec(image="soveren-credential-broker:test"),
        )

        await manager.remove_unowned("a" * 64)
        return host

    host = asyncio.run(run())

    assert host.container_id is None
    assert host.removed_ids == ["broker-orphaned"]


def test_docker_broker_extends_tenant_binding_to_a_new_conversation_network():
    async def run():
        network_a = "soveren-sandbox-egress-conversation-a"
        network_b = "soveren-sandbox-egress-conversation-b"
        host = FakeBrokerHost({network_a: ("172.30.1.0/24", "172.30.1.4")})
        manager = DockerCredentialBrokerManager(
            host=host,
            spec=DockerCredentialBrokerSpec(image="soveren-credential-broker:test"),
        )
        capability = await manager.provision_http(
            _handle(conversation_key="b" * 64, network=network_a),
            tenant_key="a" * 64,
            conversation_key="b" * 64,
            credential=b"tenant-secret",
            binding=HttpCredentialBinding(
                name="clickup",
                target_origin="https://api.clickup.com",
                allowed_methods=("GET",),
                scope="tenant",
                allowed_path_prefixes=("/api/v2",),
            ),
        )

        host.networks[network_b] = ("172.30.2.0/24", "172.30.2.4")
        await manager.prepare_tenant_network("a" * 64, network_b)
        return host, capability

    host, capability = asyncio.run(run())

    binding = _binding_by_capability(
        host.registry_payloads[-1],
        capability.base_url.rsplit("/", 1)[-1],
    )
    assert binding["allowed_local_ips"] == ["172.30.1.4", "172.30.2.4"]
    assert host.container_networks == {
        "soveren-sandbox-egress-conversation-a",
        "soveren-sandbox-egress-conversation-b",
    }


def test_docker_broker_rolls_back_a_network_prepared_for_a_failed_sandbox():
    async def run():
        network_a = "soveren-sandbox-egress-conversation-a"
        network_b = "soveren-sandbox-egress-conversation-b"
        host = FakeBrokerHost({network_a: ("172.30.1.0/24", "172.30.1.4")})
        manager = DockerCredentialBrokerManager(
            host=host,
            spec=DockerCredentialBrokerSpec(image="soveren-credential-broker:test"),
        )
        capability = await manager.provision_http(
            _handle(conversation_key="b" * 64, network=network_a),
            tenant_key="a" * 64,
            conversation_key="b" * 64,
            credential=b"tenant-secret",
            binding=HttpCredentialBinding(
                name="clickup",
                target_origin="https://api.clickup.com",
                allowed_methods=("GET",),
                scope="tenant",
                allowed_path_prefixes=("/api/v2",),
            ),
        )

        host.networks[network_b] = ("172.30.2.0/24", "172.30.2.4")
        preparation = await manager.prepare_tenant_network("a" * 64, network_b)
        await manager.rollback_prepared_network(
            preparation=preparation,
            network_subnet="172.30.2.0/24",
        )
        return host, capability

    host, capability = asyncio.run(run())

    binding = _binding_by_capability(
        host.registry_payloads[-1],
        capability.base_url.rsplit("/", 1)[-1],
    )
    assert binding["allowed_local_ips"] == ["172.30.1.4"]
    assert host.container_networks == {"soveren-sandbox-egress-conversation-a"}
    assert all("172.30.2.0/24" not in rule for rule in host.firewall_rules)


def test_docker_broker_rollback_retains_bindings_when_no_network_remains():
    async def run():
        network = "soveren-sandbox-egress-conversation-a"
        host = FakeBrokerHost({network: ("172.30.1.0/24", "172.30.1.4")})
        manager = DockerCredentialBrokerManager(
            host=host,
            spec=DockerCredentialBrokerSpec(image="soveren-credential-broker:test"),
        )
        capability = await manager.provision_http(
            _handle(conversation_key="b" * 64, network=network),
            tenant_key="a" * 64,
            conversation_key="b" * 64,
            credential=b"tenant-secret",
            binding=HttpCredentialBinding(
                name="clickup",
                target_origin="https://api.clickup.com",
                allowed_methods=("GET",),
                scope="tenant",
                allowed_path_prefixes=("/api/v2",),
            ),
        )
        await manager.remove_inactive("a" * 64)

        preparation = await manager.prepare_tenant_network("a" * 64, network)
        await manager.rollback_prepared_network(
            preparation=preparation,
            network_subnet="172.30.1.0/24",
        )
        return host, manager, capability

    host, manager, capability = asyncio.run(run())

    assert host.container_id is None
    retained = manager._bindings["a" * 64]
    binding = next(iter(retained.values()))
    assert binding.capability == capability.base_url.rsplit("/", 1)[-1]
    assert binding.secret == b"tenant-secret"


def test_docker_broker_restores_bindings_after_the_last_sandbox_idles():
    async def run():
        network = "soveren-sandbox-egress-conversation-a"
        host = FakeBrokerHost({network: ("172.30.1.0/24", "172.30.1.4")})
        manager = DockerCredentialBrokerManager(
            host=host,
            spec=DockerCredentialBrokerSpec(image="soveren-credential-broker:test"),
        )
        capability = await manager.provision_http(
            _handle(conversation_key="b" * 64, network=network),
            tenant_key="a" * 64,
            conversation_key="b" * 64,
            credential=b"conversation-secret",
            binding=HttpCredentialBinding(
                name="github",
                target_origin="https://api.github.com",
                allowed_methods=("GET",),
                allowed_path_prefixes=("/repos",),
            ),
        )

        await manager.remove_inactive("a" * 64)
        assert host.container_id is None
        await manager.prepare_tenant_network("a" * 64, network)
        return host, capability

    host, capability = asyncio.run(run())

    broker_runs = [call for call in host.calls if call[1] == "run"]
    assert len(broker_runs) == 2
    restored = _binding_by_capability(
        host.registry_payloads[-1],
        capability.base_url.rsplit("/", 1)[-1],
    )
    assert base64.b64decode(restored["secret"]) == b"conversation-secret"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_concurrent_requests", True),
        ("requests_per_minute", 1.5),
        ("max_request_bytes", 64 * 1024 * 1024 + 1),
        ("queue_timeout_s", float("nan")),
        ("request_read_timeout_s", float("inf")),
    ],
)
def test_credential_usage_policy_rejects_values_the_broker_cannot_enforce(field, value):
    with pytest.raises(ValueError):
        CredentialUsagePolicy(**{field: value})


def test_http_binding_rejects_policy_that_managed_egress_cannot_honor():
    with pytest.raises(ValueError, match="port 443"):
        HttpCredentialBinding(
            name="custom-port",
            target_origin="https://api.example.com:8443",
            allowed_methods=("GET",),
            allowed_path_prefixes=("/v1",),
        )

    with pytest.raises(ValueError, match="unsafe header"):
        HttpCredentialBinding(
            name="client-auth",
            target_origin="https://api.example.com",
            allowed_methods=("GET",),
            allowed_path_prefixes=("/v1",),
            allowed_request_headers=("Authorization",),
        )

    with pytest.raises(ValueError, match="unsafe header"):
        HttpCredentialBinding(
            name="method-override",
            target_origin="https://api.example.com",
            allowed_methods=("GET",),
            allowed_path_prefixes=("/v1",),
            allowed_request_headers=("X-HTTP-Method-Override",),
        )

    with pytest.raises(ValueError, match="unsafe"):
        HttpCredentialBinding(
            name="provider-scope-header",
            target_origin="https://api.example.com",
            allowed_methods=("GET",),
            allowed_path_prefixes=("/v1",),
            credential_header="OpenAI-Project",
        )


def test_docker_broker_rejects_an_oversized_registry_before_starting_a_container():
    async def run():
        network = "soveren-sandbox-egress-conversation-a"
        host = FakeBrokerHost({network: ("172.30.1.0/24", "172.30.1.4")})
        manager = DockerCredentialBrokerManager(
            host=host,
            spec=DockerCredentialBrokerSpec(image="soveren-credential-broker:test"),
        )
        with pytest.raises(RuntimeError, match="1 MiB"):
            await manager.provision_http(
                _handle(conversation_key="b" * 64, network=network),
                tenant_key="a" * 64,
                conversation_key="b" * 64,
                credential=b"secret",
                binding=HttpCredentialBinding(
                    name="oversized",
                    target_origin="https://api.example.com",
                    allowed_methods=("GET",),
                    allowed_path_prefixes=(f"/{'x' * (1024 * 1024)}",),
                ),
            )
        return host

    host = asyncio.run(run())

    assert host.container_id is None
    assert all(call[1] != "run" for call in host.calls)


def test_docker_broker_spec_rejects_an_unimplemented_listener_port():
    with pytest.raises(ValueError, match="port 8080"):
        DockerCredentialBrokerSpec(
            image="soveren-credential-broker:test",
            port=8443,
        )
