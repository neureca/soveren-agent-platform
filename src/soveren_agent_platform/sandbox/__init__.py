"""Sandbox lifecycle ports and the bundled Docker manager."""

from soveren_agent_platform.sandbox.contracts import (
    SANDBOX_RESOURCE_PROFILES,
    CredentialBindingScope,
    CredentialBrokerCapability,
    CredentialBrokerEndpoint,
    CredentialBrokerPolicy,
    CredentialBrokerProvisioner,
    CredentialUsagePolicy,
    HttpCredentialBinding,
    HttpCredentialBrokerProvisioner,
    SandboxHandle,
    SandboxManager,
    SandboxResourceProfile,
    SandboxSpec,
    resolve_sandbox_resource_profile,
)
from soveren_agent_platform.sandbox.docker import DockerEgressSpec, DockerSandboxManager
from soveren_agent_platform.sandbox.docker_broker import DockerCredentialBrokerSpec
from soveren_agent_platform.sandbox.docker_commands import (
    CommandResult,
    DockerCommandRunner,
    SubprocessDockerCommandRunner,
)

__all__ = [
    "CommandResult",
    "CredentialBindingScope",
    "CredentialBrokerCapability",
    "CredentialBrokerEndpoint",
    "CredentialBrokerPolicy",
    "CredentialBrokerProvisioner",
    "CredentialUsagePolicy",
    "DockerCommandRunner",
    "DockerCredentialBrokerSpec",
    "DockerEgressSpec",
    "DockerSandboxManager",
    "HttpCredentialBinding",
    "HttpCredentialBrokerProvisioner",
    "SANDBOX_RESOURCE_PROFILES",
    "SandboxHandle",
    "SandboxResourceProfile",
    "SandboxManager",
    "SandboxSpec",
    "SubprocessDockerCommandRunner",
    "resolve_sandbox_resource_profile",
]
