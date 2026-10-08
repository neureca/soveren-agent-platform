from __future__ import annotations

import asyncio
from pathlib import Path
from typing import cast

import pytest

import soveren_agent_platform.llm as llm_api
import soveren_agent_platform.llm.backends as llm_backends_api
from soveren_agent_platform.app_api import AgentPlatformApp
from soveren_agent_platform.app_api import runtime as app_runtime_module
from soveren_agent_platform.conversation import ConversationScope
from soveren_agent_platform.llm import (
    CodexSessionOpenRequest,
    CodexSessionOpenResult,
    CodexSessionPrompt,
)
from soveren_agent_platform.llm.backends import sandboxed_codex as runtime_module
from soveren_agent_platform.llm.contracts import LlmRequest
from soveren_agent_platform.sandbox import (
    CredentialBrokerCapability,
    HttpCredentialBinding,
    SandboxHandle,
    SandboxManager,
)
from soveren_agent_platform.sessions import (
    CaptureResult,
    DynamicToolRegistry,
    ExistingCodexCredentials,
    OpenResult,
    SessionBackendRegistry,
    SessionLifecyclePolicy,
    TenantBoundaryError,
)
from soveren_agent_platform.sessions.codex_credentials import (
    CodexCredentialProvider,
)
from soveren_agent_platform.sessions.mailbox import enqueue_prompt as enqueue_mailbox_prompt
from soveren_agent_platform.sessions.store import insert_session
from soveren_agent_platform.storage import bootstrap_platform_storage
from soveren_agent_platform.storage.sqlite import open_sqlite


async def credentials_for_tenant(tenant_id: str) -> ExistingCodexCredentials:
    assert tenant_id
    return ExistingCodexCredentials()


class FakeConversationBackend:
    def __init__(self, *, tenant_id: str, source_id: str, name: str) -> None:
        self.tenant_id = tenant_id
        self.source_id = source_id
        self.name = name
        self.opens = 0
        self.closes = 0
        self.shutdowns = 0
        self.prompts: list[str] = []
        self.structured_prompts: list[tuple[str, dict[str, object]]] = []
        self.provisioned: list[tuple[bytes, HttpCredentialBinding]] = []
        self.revoked: list[tuple[str, str]] = []

    async def open(self, spec) -> OpenResult:
        assert spec.conversation_scope == ConversationScope(
            tenant_id=self.tenant_id,
            source_id=self.source_id,
        )
        self.opens += 1
        return OpenResult(backend_session_id=f"thread-{self.opens}")

    async def send(self, backend_session_id: str, prompt: str):
        self.prompts.append(prompt)
        return None

    async def send_with_output_schema(
        self,
        backend_session_id: str,
        prompt: str,
        output_schema: dict[str, object],
    ):
        self.structured_prompts.append((prompt, output_schema))
        return None

    async def capture(self, backend_session_id: str) -> CaptureResult:
        return CaptureResult(text='{"kind":"reply","text":"ok"}', timed_out=False)

    async def close(self, backend_session_id: str) -> None:
        self.closes += 1

    async def shutdown(self) -> None:
        self.shutdowns += 1

    async def provision_http_credential(
        self,
        credential: bytes,
        binding: HttpCredentialBinding,
    ) -> CredentialBrokerCapability:
        self.provisioned.append((credential, binding))
        return CredentialBrokerCapability(
            base_url="http://broker/bindings/capability",
            network_ip="172.30.0.4",
        )

    async def revoke_http_credential(
        self,
        name: str,
        *,
        scope: str = "conversation",
    ) -> None:
        self.revoked.append((name, scope))


def test_public_llm_api_hides_codex_runtime_construction() -> None:
    assert llm_api.SandboxedCodexRuntime
    assert llm_api.CodexSessionOpenRequest
    assert llm_api.CodexSessionOpenResult
    assert llm_api.CodexSessionPrompt
    assert llm_api.CodexSessionPromptReceipt
    assert llm_api.TenantCodexCredentialResolver
    assert tuple(llm_api.CodexSessionOpenResult.__dataclass_fields__) == (
        "session_id",
    )
    assert not hasattr(llm_api.SandboxedCodexRuntime, "_mailbox_backends")
    assert not hasattr(llm_api.SandboxedCodexRuntime, "_restore_sessions")
    assert not hasattr(llm_api, "create_sandboxed_codex_runtime")
    assert not hasattr(llm_api, "CodexAppServerLlmBackend")
    assert not hasattr(llm_api, "SessionLlmBackend")
    assert not hasattr(llm_backends_api, "SessionLlmBackend")


def test_sandboxed_codex_runtime_routes_and_caches_by_trusted_conversation(
    tmp_path,
    monkeypatch,
) -> None:
    created: list[tuple[FakeConversationBackend, DynamicToolRegistry | None]] = []
    tool_scopes: list[ConversationScope] = []
    manager = object()

    def create_manager(*, max_active_sandboxes: int, egress_upstreams=()):
        assert max_active_sandboxes == 2
        return manager

    def create_backend(**kwargs: object) -> FakeConversationBackend:
        tenant_id = kwargs["tenant_id"]
        source_id = kwargs["source_id"]
        registry = kwargs["session_backends"]
        dynamic_tools = kwargs["dynamic_tools"]
        assert isinstance(tenant_id, str)
        assert isinstance(source_id, str)
        assert isinstance(registry, SessionBackendRegistry)
        assert dynamic_tools is None or isinstance(dynamic_tools, DynamicToolRegistry)
        assert kwargs["sandbox_manager"] is manager
        backend = FakeConversationBackend(
            tenant_id=tenant_id,
            source_id=source_id,
            name=f"codex:{tenant_id}:{source_id}",
        )
        registry.register(backend.name, backend)
        created.append((backend, dynamic_tools))
        return backend

    def tools_for(scope: ConversationScope) -> DynamicToolRegistry:
        tool_scopes.append(scope)
        return DynamicToolRegistry()

    monkeypatch.setattr(runtime_module, "_create_sandbox_manager", create_manager)
    monkeypatch.setattr(
        runtime_module,
        "_create_sandboxed_codex_backend",
        create_backend,
    )
    app = AgentPlatformApp(db_path=tmp_path / "app.db", bootstrap_storage=False)
    runtime = app.configure_sandboxed_codex(
        credentials_for_tenant=credentials_for_tenant,
        model="gpt-5.4",
        resources="small",
        max_active_sandboxes=2,
        tool_registry_factory=tools_for,
        output_schema={"type": "object"},
    )

    async def run() -> None:
        first, replay = await asyncio.gather(
            runtime.run(_request(tenant_id="tenant-a", source_id="chat-1")),
            runtime.run(_request(tenant_id="tenant-a", source_id="chat-1")),
        )
        other = await runtime.run(_request(tenant_id="tenant-a", source_id="chat-2"))
        binding = HttpCredentialBinding(
            name="clickup",
            target_origin="https://api.clickup.com",
            allowed_methods=("GET",),
            allowed_path_prefixes=("/api/v2",),
        )
        capability = await runtime.provision_http_credential(
            tenant_id="tenant-a",
            source_id="chat-1",
            credential=b"secret",
            binding=binding,
        )
        await runtime.revoke_http_credential(
            tenant_id="tenant-a",
            source_id="chat-1",
            name="clickup",
        )
        assert first.text == replay.text == other.text
        assert capability.base_url == "http://broker/bindings/capability"
        await runtime.shutdown()
        await runtime.shutdown()

    asyncio.run(run())

    assert len(created) == 2
    assert tool_scopes == [
        ConversationScope(tenant_id="tenant-a", source_id="chat-1"),
        ConversationScope(tenant_id="tenant-a", source_id="chat-2"),
    ]
    assert all(tools is not None for _, tools in created)
    assert created[0][0].opens == 2
    assert created[0][0].closes == 2
    assert created[1][0].opens == 1
    assert created[1][0].closes == 1
    assert created[0][0].prompts == []
    assert len(created[0][0].structured_prompts) == 2
    assert created[0][0].structured_prompts[0][1] == {"type": "object"}
    assert created[1][0].prompts == []
    assert created[1][0].structured_prompts[0][1] == {"type": "object"}
    assert created[0][0].provisioned[0][0] == b"secret"
    assert created[0][0].revoked == [("clickup", "conversation")]
    assert [backend.shutdowns for backend, _ in created] == [1, 1]


def test_sandboxed_codex_runtime_fails_before_backend_creation_for_invalid_request(
    tmp_path,
    monkeypatch,
) -> None:
    created = 0

    def create_manager(*, max_active_sandboxes: int, egress_upstreams=()):
        assert max_active_sandboxes == 4
        return object()

    def create_backend(**kwargs: object) -> FakeConversationBackend:
        nonlocal created
        created += 1
        raise AssertionError("invalid requests must not create a conversation backend")

    monkeypatch.setattr(runtime_module, "_create_sandbox_manager", create_manager)
    monkeypatch.setattr(
        runtime_module,
        "_create_sandboxed_codex_backend",
        create_backend,
    )
    app = AgentPlatformApp(db_path=tmp_path / "app.db", bootstrap_storage=False)
    runtime = app.configure_sandboxed_codex(
        credentials_for_tenant=credentials_for_tenant,
        model="gpt-5.4",
    )
    with pytest.raises(TypeError, match="unexpected keyword"):
        app.use_codex_session_mailbox(
            tenant_id="tenant-a",
            session_backends=SessionBackendRegistry(),
        )
    with pytest.raises(ValueError, match="non-negative integer"):
        app.use_codex_session_mailbox(
            tenant_id="tenant-a",
            stale_sending_s=-1,
        )
    with pytest.raises(ValueError, match="capture_pending_timeout_s"):
        app.use_codex_session_mailbox(
            tenant_id="tenant-a",
            capture_pending_timeout_s=0,
        )
    with pytest.raises(ValueError, match="max_consecutive_failures"):
        app.use_codex_session_mailbox(
            tenant_id="tenant-a",
            max_consecutive_failures=0,
        )
    assert app.worker_names == ()

    async def run() -> None:
        with pytest.raises(TenantBoundaryError, match="trusted conversation scope"):
            await runtime.run(_request(scope=False))
        with pytest.raises(ValueError, match="does not match configured"):
            await runtime.run(
                _request(
                    tenant_id="tenant-a",
                    source_id="chat-1",
                    model="gpt-5.3",
                )
            )
        with pytest.raises(ValueError, match="inside /workspace"):
            await runtime.open_session(
                CodexSessionOpenRequest(
                    tenant_id="tenant-a",
                    source_id="chat-1",
                    workspace_path="/etc",
                )
            )
        await runtime.shutdown()
        with pytest.raises(RuntimeError, match="shutting down"):
            await runtime.run(_request(tenant_id="tenant-a", source_id="chat-1"))

    asyncio.run(run())
    assert created == 0


@pytest.mark.parametrize(
    "value",
    [True, float("nan"), float("inf"), -1],
)
def test_agent_platform_rejects_invalid_codex_idle_stop_timeout(
    tmp_path,
    monkeypatch,
    value,
) -> None:
    manager_creations = 0

    def create_manager(*, max_active_sandboxes: int, egress_upstreams=()):
        nonlocal manager_creations
        manager_creations += 1
        return object()

    monkeypatch.setattr(runtime_module, "_create_sandbox_manager", create_manager)
    app = AgentPlatformApp(
        db_path=tmp_path / "app.db",
        bootstrap_storage=False,
    )

    with pytest.raises(ValueError, match="finite non-negative"):
        app.configure_sandboxed_codex(
            credentials_for_tenant=credentials_for_tenant,
            model="gpt-5.4",
            idle_stop_after_s=value,
        )

    assert manager_creations == 0


def test_agent_platform_composes_codex_and_scoped_custom_mailboxes(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        runtime_module,
        "_create_sandbox_manager",
        lambda *, max_active_sandboxes, egress_upstreams=(): object(),
    )
    app = AgentPlatformApp(db_path=tmp_path / "app.db", bootstrap_storage=False)
    app.configure_sandboxed_codex(
        credentials_for_tenant=credentials_for_tenant,
        model="gpt-5.4",
    )

    app.use_session_mailbox(
        tenant_id="tenant-a",
        session_backends=SessionBackendRegistry(),
        backend_prefix="custom:",
    )
    app.use_codex_session_mailbox(tenant_id="tenant-a")

    assert app.worker_names == (
        "session_mailbox:custom:tenant-a:custom:",
        "session_mailbox:codex:tenant-a",
    )
    with pytest.raises(ValueError, match="backend ownership overlaps"):
        app.use_session_mailbox(
            tenant_id="tenant-a",
            session_backends=SessionBackendRegistry(),
            backend_prefix="codex:custom:",
        )
    asyncio.run(app.stop())


def test_agent_platform_rejects_codex_with_unscoped_mailbox_in_both_orders(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        runtime_module,
        "_create_sandbox_manager",
        lambda *, max_active_sandboxes, egress_upstreams=(): object(),
    )
    codex_first = AgentPlatformApp(
        db_path=tmp_path / "codex-first.db",
        bootstrap_storage=False,
    )
    codex_first.configure_sandboxed_codex(
        credentials_for_tenant=credentials_for_tenant,
        model="gpt-5.4",
    )
    codex_first.use_codex_session_mailbox(tenant_id="tenant-a")

    with pytest.raises(ValueError, match="backend ownership overlaps"):
        codex_first.use_session_mailbox(
            tenant_id="tenant-a",
            session_backends=SessionBackendRegistry(),
        )
    asyncio.run(codex_first.stop())

    generic_first = AgentPlatformApp(
        db_path=tmp_path / "generic-first.db",
        bootstrap_storage=False,
    )
    generic_first.use_session_mailbox(
        tenant_id="tenant-a",
        session_backends=SessionBackendRegistry(),
    )
    generic_first.configure_sandboxed_codex(
        credentials_for_tenant=credentials_for_tenant,
        model="gpt-5.4",
    )

    with pytest.raises(ValueError, match="backend ownership overlaps"):
        generic_first.use_codex_session_mailbox(tenant_id="tenant-a")
    asyncio.run(generic_first.stop())


def test_agent_platform_app_owns_sandboxed_codex_runtime_shutdown(
    tmp_path,
    monkeypatch,
) -> None:
    backends: list[FakeConversationBackend] = []

    monkeypatch.setattr(
        runtime_module,
        "_create_sandbox_manager",
        lambda *, max_active_sandboxes, egress_upstreams=(): object(),
    )

    def create_backend(**kwargs: object) -> FakeConversationBackend:
        tenant_id = kwargs["tenant_id"]
        source_id = kwargs["source_id"]
        registry = kwargs["session_backends"]
        assert isinstance(tenant_id, str)
        assert isinstance(source_id, str)
        assert isinstance(registry, SessionBackendRegistry)
        backend = FakeConversationBackend(
            tenant_id=tenant_id,
            source_id=source_id,
            name=f"codex:{tenant_id}:{source_id}",
        )
        registry.register(backend.name, backend)
        backends.append(backend)
        return backend

    monkeypatch.setattr(
        runtime_module,
        "_create_sandboxed_codex_backend",
        create_backend,
    )
    app = AgentPlatformApp(db_path=tmp_path / "app.db")
    runtime = app.configure_sandboxed_codex(
        credentials_for_tenant=credentials_for_tenant,
        model="gpt-5.4",
    )

    async def run() -> None:
        await app.start()
        await runtime.run(_request(tenant_id="tenant-a", source_id="chat-1"))
        await app.stop()

    asyncio.run(run())
    assert len(backends) == 1
    assert backends[0].shutdowns == 1


def test_agent_platform_app_rejects_second_process_runtime(
    tmp_path,
    monkeypatch,
) -> None:
    managers: list[object] = []

    def create_manager(*, max_active_sandboxes: int, egress_upstreams=()) -> object:
        manager = object()
        managers.append(manager)
        return manager

    monkeypatch.setattr(runtime_module, "_create_sandbox_manager", create_manager)
    first_app = AgentPlatformApp(
        db_path=tmp_path / "first.db",
        bootstrap_storage=False,
    )
    second_app = AgentPlatformApp(
        db_path=tmp_path / "second.db",
        bootstrap_storage=False,
    )
    runtime = first_app.configure_sandboxed_codex(
        credentials_for_tenant=credentials_for_tenant,
        model="gpt-5.4",
    )

    with pytest.raises(RuntimeError, match="already configured"):
        first_app.configure_sandboxed_codex(
            credentials_for_tenant=credentials_for_tenant,
            model="gpt-5.4",
        )
    with pytest.raises(RuntimeError, match="already configured in this process"):
        second_app.configure_sandboxed_codex(
            credentials_for_tenant=credentials_for_tenant,
            model="gpt-5.4",
        )

    asyncio.run(runtime.shutdown())
    second_runtime = second_app.configure_sandboxed_codex(
        credentials_for_tenant=credentials_for_tenant,
        model="gpt-5.4",
    )
    asyncio.run(second_runtime.shutdown())
    assert len(managers) == 3


def test_sandboxed_codex_runtime_resolves_credentials_per_tenant(
    tmp_path,
    monkeypatch,
) -> None:
    providers: list[CodexCredentialProvider] = []
    resolved_tenants: list[str] = []

    async def resolve_credentials(tenant_id: str) -> ExistingCodexCredentials:
        resolved_tenants.append(tenant_id)
        return ExistingCodexCredentials()

    monkeypatch.setattr(
        runtime_module,
        "_create_sandbox_manager",
        lambda *, max_active_sandboxes, egress_upstreams=(): object(),
    )

    def create_backend(**kwargs: object) -> FakeConversationBackend:
        tenant_id = kwargs["tenant_id"]
        source_id = kwargs["source_id"]
        registry = kwargs["session_backends"]
        credentials = kwargs["credentials"]
        assert isinstance(tenant_id, str)
        assert isinstance(source_id, str)
        assert isinstance(registry, SessionBackendRegistry)
        assert isinstance(credentials, CodexCredentialProvider)
        backend = FakeConversationBackend(
            tenant_id=tenant_id,
            source_id=source_id,
            name=f"codex:{tenant_id}:{source_id}",
        )
        registry.register(backend.name, backend)
        providers.append(credentials)
        return backend

    monkeypatch.setattr(
        runtime_module,
        "_create_sandboxed_codex_backend",
        create_backend,
    )
    app = AgentPlatformApp(db_path=tmp_path / "app.db", bootstrap_storage=False)
    runtime = app.configure_sandboxed_codex(
        credentials_for_tenant=resolve_credentials,
        model="gpt-5.4",
    )

    async def run() -> None:
        await runtime.run(_request(tenant_id="tenant-a", source_id="chat-1"))
        await runtime.run(_request(tenant_id="tenant-b", source_id="chat-2"))
        manager = cast(SandboxManager, object())
        for provider, tenant_id, source_id in (
            (providers[0], "tenant-a", "chat-1"),
            (providers[1], "tenant-b", "chat-2"),
        ):
            await provider.provision(
                manager,
                SandboxHandle(
                    id=f"sandbox-{tenant_id}",
                    name=f"sandbox-{tenant_id}",
                    tenant_id=tenant_id,
                    conversation_id=source_id,
                    workspace_root="/workspace",
                    codex_home="/codex-home",
                ),
            )
        with pytest.raises(TenantBoundaryError, match="another tenant"):
            await providers[0].provision(
                manager,
                SandboxHandle(
                    id="sandbox-b",
                    name="sandbox-b",
                    tenant_id="tenant-b",
                    conversation_id="chat-2",
                    workspace_root="/workspace",
                    codex_home="/codex-home",
                ),
            )
        await runtime.shutdown()

    asyncio.run(run())
    assert resolved_tenants == ["tenant-a", "tenant-b"]


def test_sandboxed_codex_runtime_owns_durable_sessions_and_mailbox(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "app.db"
    backends: list[FakeConversationBackend] = []
    monkeypatch.setattr(
        runtime_module,
        "_create_sandbox_manager",
        lambda *, max_active_sandboxes, egress_upstreams=(): object(),
    )

    def create_backend(**kwargs: object) -> FakeConversationBackend:
        tenant_id = kwargs["tenant_id"]
        source_id = kwargs["source_id"]
        registry = kwargs["session_backends"]
        assert isinstance(tenant_id, str)
        assert isinstance(source_id, str)
        assert isinstance(registry, SessionBackendRegistry)
        backend = FakeConversationBackend(
            tenant_id=tenant_id,
            source_id=source_id,
            name=f"codex:{tenant_id}:{source_id}",
        )
        registry.register(backend.name, backend)
        backends.append(backend)
        return backend

    monkeypatch.setattr(
        runtime_module,
        "_create_sandboxed_codex_backend",
        create_backend,
    )
    app = AgentPlatformApp(db_path=db_path, bootstrap_storage=False)
    runtime = app.configure_sandboxed_codex(
        credentials_for_tenant=credentials_for_tenant,
        model="gpt-5.4",
        output_schema={"type": "object"},
    )
    app.use_codex_session_mailbox(tenant_id="tenant-a")

    async def run() -> None:
        await bootstrap_platform_storage(db_path)
        conn = open_sqlite(db_path)
        foreign_session_id = insert_session(
            conn,
            tenant_id="tenant-a",
            source_id="chat-1",
            kind="custom",
            backend="other",
            backend_session_id="other-thread",
        )
        conn.close()
        assert (
            await runtime.close_idle_sessions(
                tenant_id="tenant-a",
                policy=SessionLifecyclePolicy(idle_ttl_s=0),
            )
            == []
        )
        conn = open_sqlite(db_path)
        foreign_mailbox_id, _ = enqueue_mailbox_prompt(
            conn,
            session_id=foreign_session_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            prompt="must remain owned by the custom worker",
        )
        conn.close()
        with pytest.raises(RuntimeError, match="not owned"):
            await runtime.enqueue_prompt(
                CodexSessionPrompt(
                    session_id=foreign_session_id,
                    tenant_id="tenant-a",
                    source_id="chat-1",
                    prompt="must not be accepted",
                )
            )
        with pytest.raises(RuntimeError, match="not owned"):
            await runtime.close_session(
                foreign_session_id,
                tenant_id="tenant-a",
                source_id="chat-1",
            )
        planner_result = await runtime.run(
            _request(tenant_id="tenant-a", source_id="chat-1")
        )
        opened = await runtime.open_session(
            CodexSessionOpenRequest(
                tenant_id="tenant-a",
                source_id="chat-1",
                owner_id="user-1",
                title="Primary session",
            )
        )
        assert opened == CodexSessionOpenResult(session_id=opened.session_id)
        assert not hasattr(opened, "backend_session_id")
        assert not hasattr(opened, "session_handle")
        first = await runtime.enqueue_prompt(
            CodexSessionPrompt(
                session_id=opened.session_id,
                tenant_id="tenant-a",
                source_id="chat-1",
                prompt="continue",
                idempotency_key="prompt-1",
            )
        )
        replay = await runtime.enqueue_prompt(
            CodexSessionPrompt(
                session_id=opened.session_id,
                tenant_id="tenant-a",
                source_id="chat-1",
                prompt="continue",
                idempotency_key="prompt-1",
            )
        )
        await app.start()
        for _ in range(100):
            if backends[0].prompts:
                break
            await asyncio.sleep(0.01)
        assert len(backends[0].structured_prompts) == 1
        assert backends[0].structured_prompts[0][1] == {"type": "object"}
        assert backends[0].prompts == ["continue"]
        conn = open_sqlite(db_path)
        foreign_mailbox = conn.execute(
            "SELECT status, last_error FROM session_mailbox WHERE id = ?",
            (foreign_mailbox_id,),
        ).fetchone()
        foreign_session = conn.execute(
            "SELECT status FROM runtime_sessions WHERE id = ?",
            (foreign_session_id,),
        ).fetchone()
        conn.close()
        assert foreign_mailbox["status"] == "queued"
        assert foreign_mailbox["last_error"] is None
        assert foreign_session["status"] == "idle"
        closed = await runtime.close_session(
            opened.session_id,
            tenant_id="tenant-a",
            source_id="chat-1",
            force=True,
        )
        assert first.created is True
        assert replay == type(replay)(mailbox_id=first.mailbox_id, created=False)
        assert closed.closed is True
        assert closed.cancelled_mailbox_count == 0
        assert planner_result.text == '{"kind":"reply","text":"ok"}'
        await app.stop()

    asyncio.run(run())
    assert app.worker_names == ("session_mailbox:codex:tenant-a",)
    assert len(backends) == 1
    assert backends[0].opens == 2
    assert backends[0].closes == 2
    assert backends[0].shutdowns == 1


def test_agent_platform_restores_persistent_codex_backends_before_mailbox_start(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "app.db"
    backends: list[FakeConversationBackend] = []
    monkeypatch.setattr(
        runtime_module,
        "_create_sandbox_manager",
        lambda *, max_active_sandboxes, egress_upstreams=(): object(),
    )

    def create_backend(**kwargs: object) -> FakeConversationBackend:
        tenant_id = kwargs["tenant_id"]
        source_id = kwargs["source_id"]
        registry = kwargs["session_backends"]
        assert isinstance(tenant_id, str)
        assert isinstance(source_id, str)
        assert isinstance(registry, SessionBackendRegistry)
        backend = FakeConversationBackend(
            tenant_id=tenant_id,
            source_id=source_id,
            name=f"codex:{tenant_id}:{source_id}",
        )
        registry.register(backend.name, backend)
        backends.append(backend)
        return backend

    async def mailbox_worker(db_path, stop_event, **kwargs) -> None:
        await stop_event.wait()

    monkeypatch.setattr(
        runtime_module,
        "_create_sandboxed_codex_backend",
        create_backend,
    )
    monkeypatch.setattr(
        app_runtime_module,
        "run_session_mailbox_worker",
        mailbox_worker,
    )

    async def run() -> None:
        await bootstrap_platform_storage(db_path)
        first_app = AgentPlatformApp(db_path=db_path, bootstrap_storage=False)
        first_runtime = first_app.configure_sandboxed_codex(
            credentials_for_tenant=credentials_for_tenant,
            model="gpt-5.4",
        )
        await first_runtime.open_session(
            CodexSessionOpenRequest(
                tenant_id="tenant-a",
                source_id="chat-1",
                title="Persistent session",
            )
        )
        await first_app.stop()

        second_app = AgentPlatformApp(db_path=db_path, bootstrap_storage=False)
        second_app.configure_sandboxed_codex(
            credentials_for_tenant=credentials_for_tenant,
            model="gpt-5.4",
        )
        second_app.use_codex_session_mailbox(tenant_id="tenant-a")
        await second_app.start()
        assert len(backends) == 2
        assert backends[1].opens == 0
        await second_app.stop()

    asyncio.run(run())
    assert [backend.shutdowns for backend in backends] == [1, 1]


def _request(
    *,
    tenant_id: str = "tenant-a",
    source_id: str = "chat-1",
    model: str = "gpt-5.4",
    scope: bool = True,
) -> LlmRequest:
    return LlmRequest(
        prompt="hello",
        system_prompt="system",
        cwd=Path("/host/work"),
        env_home=Path("/host/codex-home"),
        model=model,
        conversation_scope=(
            ConversationScope(tenant_id=tenant_id, source_id=source_id)
            if scope
            else None
        ),
    )
