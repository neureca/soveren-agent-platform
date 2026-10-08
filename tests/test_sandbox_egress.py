from pathlib import Path

import pytest

from soveren_agent_platform.app_api import AgentPlatformApp
from soveren_agent_platform.sandbox import SandboxEgressUpstream
from soveren_agent_platform.sandbox.egress import (
    UPSTREAM_ROUTES_ENV,
    normalize_upstreams,
    render_squid_config,
    upstreams_environment,
    upstreams_from_environment,
)
from soveren_agent_platform.sessions import ExistingCodexCredentials


def test_egress_upstream_normalizes_config_and_round_trips_environment():
    policy = SandboxEgressUpstream(
        proxy_url="http://HOST.DOCKER.INTERNAL:10810/",
        destination_hosts=("API.Provider.Example.", "chat.provider.example"),
    )
    assert policy.proxy_url == "http://host.docker.internal:10810"
    assert policy.destination_hosts == ("api.provider.example", "chat.provider.example")
    assert upstreams_from_environment(upstreams_environment((policy,))) == (policy,)
    assert upstreams_from_environment({}) == ()
    assert upstreams_from_environment({UPSTREAM_ROUTES_ENV: ""}) == ()
    assert upstreams_from_environment({UPSTREAM_ROUTES_ENV: "[]"}) == ()


def test_multiple_groups_round_trip_and_combine_same_proxy_without_order_drift():
    first = SandboxEgressUpstream(proxy_url="http://parent-a:8080", destination_hosts=("a.example",))
    second = SandboxEgressUpstream(proxy_url="http://parent-b:8080", destination_hosts=("b.example",))
    shared = SandboxEgressUpstream(proxy_url="http://parent-a:8080", destination_hosts=("c.example",))
    expected = (
        SandboxEgressUpstream(proxy_url=first.proxy_url, destination_hosts=("a.example", "c.example")), second,
    )
    assert normalize_upstreams((second, shared, first)) == expected
    assert upstreams_environment((second, shared, first)) == upstreams_environment((first, shared, second))
    assert upstreams_from_environment(upstreams_environment((first, shared, second))) == expected


@pytest.mark.parametrize("second_proxy", ["http://parent-a:8080", "http://parent-b:8080"])
def test_duplicate_hostname_across_groups_is_rejected_even_for_same_proxy(second_proxy):
    groups = (
        SandboxEgressUpstream(proxy_url="http://parent-a:8080", destination_hosts=("API.Provider.Example.",)),
        SandboxEgressUpstream(proxy_url=second_proxy, destination_hosts=("api.provider.example",)),
    )
    with pytest.raises(ValueError, match="assigned more than once"):
        normalize_upstreams(groups)


@pytest.mark.parametrize("hosts", [
    ("a.example", "a.example"), ("API.Provider.Example.", "api.provider.example"),
])
def test_duplicate_hostname_within_group_is_rejected(hosts):
    with pytest.raises(ValueError, match="duplicated"):
        SandboxEgressUpstream(proxy_url="http://parent:8080", destination_hosts=hosts)


def test_total_policy_budgets_apply_across_groups():
    with pytest.raises(ValueError, match="16 groups"):
        normalize_upstreams(tuple(
            SandboxEgressUpstream(proxy_url=f"http://parent-{i}:8080", destination_hosts=(f"host-{i}.example",))
            for i in range(17)
        ))
    with pytest.raises(ValueError, match="256 total"):
        normalize_upstreams(tuple(
            SandboxEgressUpstream(
                proxy_url=f"http://parent-{i}:8080",
                destination_hosts=tuple(f"host-{i}-{n}.example" for n in range(129)),
            )
            for i in range(2)
        ))


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
    {"SOVEREN_EGRESS_UPSTREAM_PROXY": "http://host:10810"},
    {"SOVEREN_EGRESS_UPSTREAM_DESTINATIONS": '["api.provider.example"]'},
    {UPSTREAM_ROUTES_ENV: "not-json"},
    {UPSTREAM_ROUTES_ENV: '"api.provider.example"'},
    {UPSTREAM_ROUTES_ENV: '[{"proxy_url":"http://parent:8080"}]'},
    {UPSTREAM_ROUTES_ENV: '[{"destination_hosts":["a.example"]}]'},
    {UPSTREAM_ROUTES_ENV: '[{"proxy_url":"http://parent:8080","destination_hosts":[]}]'},
    {UPSTREAM_ROUTES_ENV: '[{"proxy_url":"http://parent:8080","destination_hosts":"a.example"}]'},
    {UPSTREAM_ROUTES_ENV: '[{"proxy_url":"http://parent:8080","destination_hosts":["a.example"],"extra":true}]'},
])
def test_partial_or_invalid_egress_environment_cannot_disable_routing(environment):
    with pytest.raises(ValueError):
        upstreams_from_environment(environment)


def test_rendered_policy_retains_private_denies_and_direct_default():
    base = Path("deploy/sandbox/squid.conf").read_text()
    assert render_squid_config(base, ()) == base
    rendered = render_squid_config(base, (SandboxEgressUpstream(
        proxy_url="http://host.docker.internal:10810", destination_hosts=("api.provider.example",),
    ), SandboxEgressUpstream(proxy_url="http://other-parent:8080", destination_hosts=("other.provider.example",))))
    assert "http_access deny blocked_destination" in rendered
    assert "http_access deny upstream_destination !resolved_destination" in rendered
    assert "acl upstream_destination dstdomain -n api.provider.example other.provider.example" in rendered
    assert "always_direct deny upstream_destination\nalways_direct allow all" in rendered
    assert "never_direct allow upstream_destination" in rendered
    for index in range(2):
        assert f"cache_peer_access selected_upstream_{index} allow upstream_group_{index}" in rendered
        assert f"cache_peer_access selected_upstream_{index} deny all" in rendered


def test_upstream_cannot_silently_omit_destination_guards_from_changed_template():
    policy = SandboxEgressUpstream(proxy_url="http://parent.example:10810", destination_hosts=("provider.example",))
    with pytest.raises(ValueError, match="Squid base configuration"):
        render_squid_config("http_access allow localnet\n", (policy,))


def test_public_bootstrap_wires_host_wide_policy_without_changing_capacity_or_resources(tmp_path):
    import asyncio

    from soveren_agent_platform.llm.backends.sandboxed_codex import _DefaultSandboxedCodexRuntime
    from soveren_agent_platform.sandbox import resolve_sandbox_resource_profile

    async def credentials(tenant_id: str) -> ExistingCodexCredentials:
        return ExistingCodexCredentials()

    policy = (SandboxEgressUpstream(
        proxy_url="http://host.docker.internal:10810", destination_hosts=("other-provider.example",),
    ), SandboxEgressUpstream(proxy_url="http://parent-b:8080", destination_hosts=("another-provider.example",)))
    app = AgentPlatformApp(db_path=tmp_path / "app.db", bootstrap_storage=False)
    runtime = app.configure_sandboxed_codex(
        credentials_for_tenant=credentials, model="test-model", resources="small",
        max_active_sandboxes=3, egress_upstreams=policy,
    )
    try:
        assert isinstance(runtime, _DefaultSandboxedCodexRuntime)
        manager = runtime._sandbox_manager
        assert manager.max_active_sandboxes == 3
        assert manager.egress.upstreams == policy
        assert resolve_sandbox_resource_profile("small").memory == "512m"
    finally:
        asyncio.run(app.stop())


def test_public_bootstrap_rejects_conflicting_groups_before_claiming_runtime(tmp_path):
    import asyncio

    async def credentials(tenant_id: str) -> ExistingCodexCredentials:
        return ExistingCodexCredentials()

    first = SandboxEgressUpstream(proxy_url="http://parent-a:8080", destination_hosts=("provider.example",))
    conflicting = SandboxEgressUpstream(proxy_url="http://parent-b:8080", destination_hosts=("PROVIDER.EXAMPLE.",))
    app = AgentPlatformApp(db_path=tmp_path / "app.db", bootstrap_storage=False)
    try:
        with pytest.raises(ValueError, match="assigned more than once"):
            app.configure_sandboxed_codex(
                credentials_for_tenant=credentials, model="test-model", egress_upstreams=(first, conflicting),
            )
        app.configure_sandboxed_codex(
            credentials_for_tenant=credentials, model="test-model", egress_upstreams=(first,),
        )
    finally:
        asyncio.run(app.stop())
