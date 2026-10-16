"""The SIWC provider and latency must ship and compose together."""

import asyncio
import json
import logging
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from test_sandbox_manager import CompletedFakeCodexClient, FakeSandboxManager
from test_telegram_ptb_adapter import FakeApplicationBuilder, FakeHandler, FakeUpdate
from test_turn_latency import Handler, Prompts, Reply, summaries, until

from soveren_agent_platform.app_api import AgentPlatformApp
from soveren_agent_platform.decisions import DecisionDispatcher, DecisionRegistry
from soveren_agent_platform.llm.backends import sandboxed_codex as runtime_module
from soveren_agent_platform.runtime.planner import PlannerRuntimeConfig, run_planner_dispatch_turn
from soveren_agent_platform.sessions import (
    ChatGPTPlanAccessToken,
    CodexChatGPTPlanCredentials,
    CodexClientInfo,
    CodexCredentialUnavailable,
)
from soveren_agent_platform.telegram import create_telegram_agent_app


@pytest.mark.parametrize("denied", [False, True])
def test_siwc_provider_through_real_telegram_latency_composition(tmp_path, monkeypatch, caplog, denied):
    caplog.set_level(logging.INFO)
    bearer = "siwc-fixture-bearer-secret"
    manager = FakeSandboxManager()
    manager.handle = replace(manager.handle, tenant_id="tenant", conversation_id="456")

    class Client(CompletedFakeCodexClient):
        def set_last_turn(self, thread_id, turn_id):
            state = super().set_last_turn(thread_id, turn_id)
            state.text_parts = [json.dumps({"kind": "reply", "text": "accepted"})]
            return state

    client = Client()
    created_backends = []
    original_factory = runtime_module._create_sandboxed_codex_backend

    def create_backend(**kwargs):
        backend = original_factory(**kwargs)
        backend.client = client
        created_backends.append(backend)
        return backend

    monkeypatch.setattr(runtime_module, "_create_sandbox_manager", lambda **kwargs: manager)
    monkeypatch.setattr(runtime_module, "_create_sandboxed_codex_backend", create_backend)

    async def run():
        lookups = 0

        async def get_token():
            nonlocal lookups
            lookups += 1
            if denied:
                raise CodexCredentialUnavailable()
            return ChatGPTPlanAccessToken(bearer, datetime.now(UTC) + timedelta(hours=1))

        provider = CodexChatGPTPlanCredentials("tenant", get_token, CodexClientInfo("Pulsy", "0.1.0", "Pulsy"))

        async def credentials_for_tenant(tenant_id):
            assert tenant_id == "tenant"
            return provider

        path = tmp_path / "app.db"
        platform = AgentPlatformApp(db_path=path)
        codex = platform.configure_sandboxed_codex(
            credentials_for_tenant=credentials_for_tenant,
            model="fixture",
            max_active_sandboxes=3,
        )

        class SiwcHandler(Handler):
            async def handle(self, event):
                self.event = event
                registry = DecisionRegistry()
                registry.register("reply", Reply)
                dispatcher = DecisionDispatcher()
                dispatcher.register("reply", self)
                self.result = await run_planner_dispatch_turn(
                    self.conn,
                    event=event,
                    prompt_builder=Prompts(),
                    llm_backend=codex,
                    decision_parser=registry,
                    dispatcher=dispatcher,
                    config=PlannerRuntimeConfig(model="fixture", prompt_version="v1", cwd=tmp_path, env_home=tmp_path),
                )

        handler = SiwcHandler(path)
        builder = FakeApplicationBuilder()
        app = await create_telegram_agent_app(
            token="fixture",
            db_path=path,
            tenant_id="tenant",
            handler=handler,
            platform=platform,
            latency_logging=True,
            agent_idle_max_s=0.02,
            outbound_idle_max_s=0.02,
            quiet_window_s=0,
            allowed_chat_ids=[456],
            application_builder=builder,
            message_handler_cls=FakeHandler,
            callback_query_handler_cls=FakeHandler,
            message_filter="all",
        )
        try:
            await builder.app.handlers[0].callback(FakeUpdate(text="request"), SimpleNamespace())
            await app.start()
            await until(lambda: summaries(caplog))
            result = summaries(caplog)[0]
            assert result["event_id"] == handler.event.id
            assert len(result["run_ids"]) == 1
            assert result["outcome"] == ("agent_failed" if denied else "sent")
            assert result["phases"]["session_open"]["availability"] == "measured"
            assert lookups >= 1
            if denied:
                assert manager.http_broker_revocations == [("soveren-chatgpt-plan", "conversation")]
                assert not manager.http_broker_calls
                assert not client.calls
                assert not builder.app.bot.calls
            else:
                assert result["phases"]["codex_turn"]["availability"] == "measured"
                assert result["phases"]["telegram_send"]["availability"] == "measured"
                assert manager.http_broker_calls
                assert all(credential == bearer.encode() for credential, _ in manager.http_broker_calls)
                assert created_backends[0]._backend.client_info == provider.client_info
                assert len(builder.app.bot.calls) == 1
                # Completed decision replay neither calls the OAuth owner nor the model again.
                before = lookups
                await handler.handle(handler.event)
                assert lookups == before
            assert bearer not in caplog.text
            assert bearer not in repr(manager.commands)
            assert bearer not in repr(manager.exec_envs)
            assert len(summaries(caplog)) == 1
        finally:
            await app.stop()
            handler.conn.close()

    asyncio.run(run())
