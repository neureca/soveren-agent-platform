from pathlib import Path

import pytest

from soveren_agent_platform.app_api import AgentPlatformApp
from soveren_agent_platform.sandbox import SandboxEgressUpstream
from soveren_agent_platform.sandbox.egress import (
    UPSTREAM_DESTINATIONS_ENV,
    UPSTREAM_PROXY_ENV,
    render_squid_config,
    upstream_from_environment,
)
from soveren_agent_platform.sessions import ExistingCodexCredentials


def test_egress_upstream_normalizes_config_and_round_trips_environment():
    policy = SandboxEgressUpstream(
        proxy_url="http://HOST.DOCKER.INTERNAL:10810/",
        destination_hosts=("API.Provider.Example", "api.provider.example.", "chat.provider.example"),
    )
    assert policy.proxy_url == "http://host.docker.internal:10810"
    assert policy.destination_hosts == ("api.provider.example", "chat.provider.example")
    assert upstream_from_environment(policy.environment()) == policy
    assert upstream_from_environment({}) is None
    assert upstream_from_environment({UPSTREAM_PROXY_ENV: "", UPSTREAM_DESTINATIONS_ENV: ""}) is None


@pytest.mark.parametrize("url", [
    "http://127.0.0.1:10809", "http://localhost:10809", "http://[::1]:10809", "http://0.0.0.0:10809",
    "https://host:10809", "socks5://host:10809", "http://host", "http://host:0", "http://host:65536",
    "http://user:secret@host:10809", "http://host:10809/path", "http://host:10809?foo=bar",
    "http://host:10809#fragment", "http://host:10809\ncache_peer bad parent 1 0", "http://host:bad",
])
def test_egress_upstream_rejects_unusable_or_unsafe_urls(url):
    with pytest.raises(ValueError):
        SandboxEgressUpstream(proxy_url=url, destination_hosts=("api.provider.example",))


@pytest.mark.parametrize("hosts", [
    (), "api.provider.example", ("",), (".provider.example",), ("*.provider.example",),
    ("https://provider.example/v1",), ("127.0.0.1",), ("::1",), ("provider.example\nhttp_access allow all",),
    ("provider.example other.example",), ("a" * 64 + ".example",), (None,), tuple(["a.example"] * 257),
])
def test_egress_upstream_rejects_non_hostname_or_unbounded_destinations(hosts):
    with pytest.raises(ValueError):
        SandboxEgressUpstream(proxy_url="http://172.17.0.1:10810", destination_hosts=hosts)


@pytest.mark.parametrize("environment", [
    {UPSTREAM_PROXY_ENV: "http://host:10810"},
    {UPSTREAM_DESTINATIONS_ENV: '["api.provider.example"]'},
    {UPSTREAM_PROXY_ENV: "http://host:10810", UPSTREAM_DESTINATIONS_ENV: "[]"},
    {UPSTREAM_PROXY_ENV: "http://host:10810", UPSTREAM_DESTINATIONS_ENV: "not-json"},
    {UPSTREAM_PROXY_ENV: "http://host:10810", UPSTREAM_DESTINATIONS_ENV: '"api.provider.example"'},
])
def test_partial_or_invalid_egress_environment_cannot_disable_routing(environment):
    with pytest.raises(ValueError):
        upstream_from_environment(environment)


def test_rendered_policy_retains_private_denies_and_direct_default():
    base = Path("deploy/sandbox/squid.conf").read_text()
    assert render_squid_config(base, None) == base
    rendered = render_squid_config(base, SandboxEgressUpstream(
        proxy_url="http://host.docker.internal:10810", destination_hosts=("api.provider.example",),
    ))
    assert "http_access deny blocked_destination" in rendered
    assert "http_access deny upstream_destination !resolved_destination" in rendered
    assert "acl upstream_destination dstdomain -n api.provider.example" in rendered
    assert "always_direct deny upstream_destination\nalways_direct allow all" in rendered
    assert "never_direct allow upstream_destination" in rendered
    assert "cache_peer_access selected_upstream deny all" in rendered


def test_upstream_cannot_silently_omit_destination_guards_from_changed_template():
    policy = SandboxEgressUpstream(proxy_url="http://parent.example:10810", destination_hosts=("provider.example",))
    with pytest.raises(ValueError, match="Squid base configuration"):
        render_squid_config("http_access allow localnet\n", policy)


def test_public_bootstrap_wires_host_wide_policy_without_changing_capacity_or_resources(tmp_path):
    import asyncio

    from soveren_agent_platform.llm.backends.sandboxed_codex import _DefaultSandboxedCodexRuntime
    from soveren_agent_platform.sandbox import resolve_sandbox_resource_profile

    async def credentials(tenant_id: str) -> ExistingCodexCredentials:
        return ExistingCodexCredentials()

    policy = SandboxEgressUpstream(
        proxy_url="http://host.docker.internal:10810", destination_hosts=("other-provider.example",),
    )
    app = AgentPlatformApp(db_path=tmp_path / "app.db", bootstrap_storage=False)
    runtime = app.configure_sandboxed_codex(
        credentials_for_tenant=credentials, model="test-model", resources="small",
        max_active_sandboxes=3, egress_upstream=policy,
    )
    try:
        assert isinstance(runtime, _DefaultSandboxedCodexRuntime)
        manager = runtime._sandbox_manager
        assert manager.max_active_sandboxes == 3
        assert manager.egress.upstream == policy
        assert resolve_sandbox_resource_profile("small").memory == "512m"
    finally:
        asyncio.run(app.stop())
