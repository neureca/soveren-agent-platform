import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from soveren_agent_platform.runtime.failures import is_non_retryable_event_error
from soveren_agent_platform.sandbox import CredentialBrokerCapability, SandboxHandle
from soveren_agent_platform.sessions import (
    ChatGPTPlanAccessToken,
    CodexChatGPTPlanCredentials,
    CodexClientInfo,
    CodexCredentialUnavailable,
    TenantBoundaryError,
)


class PlanBroker:
    def __init__(self):
        self.bindings = []
        self.revoked = []
        self.guards = []
        self.native_auth_present = False
        self.endpoint = CredentialBrokerCapability(
            base_url="http://soveren-credential-broker:8080/bindings/test-capability",
            network_ip="172.30.0.4",
        )

    async def provision_http_credential(self, handle, *, credential, binding):
        self.bindings.append((handle, credential, binding))
        return self.endpoint

    async def revoke_http_credential(self, handle, *, name, scope="conversation"):
        self.revoked.append((handle, name, scope))

    async def run_command(self, handle, command, **kwargs):
        self.guards.append((handle, command, kwargs))
        if self.native_auth_present:
            raise RuntimeError("SIWC requires a CODEX_HOME without native auth.json")


def plan_handle():
    return SandboxHandle("container", "sandbox", "tenant-a", "chat-a", "/workspace", "/codex-home")


def grant(value="fixture-access-secret", seconds=3600):
    return ChatGPTPlanAccessToken(value, datetime.now(UTC) + timedelta(seconds=seconds))


def provider(callback, **kwargs):
    return CodexChatGPTPlanCredentials(
        tenant_id="tenant-a", get_access_token=callback,
        client_info=CodexClientInfo("Pulsy", "0.1.0", "Pulsy"), **kwargs,
    )


def test_plan_token_is_protected_and_bound_to_fixed_responses_origin():
    async def run():
        token = grant()

        async def get():
            return token

        credential = provider(get)
        broker = PlanBroker()
        config = await credential.provision(broker, plan_handle())
        assert len(broker.bindings) == 1
        handle, credential_bytes, binding = broker.bindings[0]
        assert credential_bytes == token.access_token.encode()
        assert handle == plan_handle()
        assert binding.target_origin == "https://api.openai.com"
        assert binding.allowed_methods == ("POST",)
        assert binding.allowed_path_prefixes == ("/v1/responses",)
        assert binding.scope == "conversation"
        assert "authorization" not in binding.allowed_request_headers
        assert config.client_info == CodexClientInfo("Pulsy", "0.1.0", "Pulsy")
        assert any("/bindings/test-capability/v1" in value for value in config.config_overrides)
        assert ("NO_PROXY", "soveren-credential-broker,172.30.0.4") in config.launch_env
        assert token.access_token not in repr(token)
        assert token.access_token not in repr(credential)
        assert token.access_token not in repr(config)
        assert token.access_token not in repr(broker.guards)
        assert broker.guards[0][2].get("input_data") is None

    asyncio.run(run())


@pytest.mark.parametrize("operation", ["provision", "refresh_for_turn"])
def test_plan_foreign_handle_denied_before_token_lookup_or_io(operation):
    async def run():
        looked_up = False

        async def get():
            nonlocal looked_up
            looked_up = True
            return grant()

        broker = PlanBroker()
        with pytest.raises(TenantBoundaryError):
            await getattr(provider(get), operation)(broker, replace(plan_handle(), tenant_id="foreign"))
        assert not looked_up and not broker.guards and not broker.bindings

    asyncio.run(run())


def test_plan_refresh_rotates_binding_without_native_secret_provisioning():
    async def run():
        current = grant("first-access-secret")

        async def get():
            return current

        credential = provider(get)
        broker = PlanBroker()
        initial = await credential.provision(broker, plan_handle())
        current = grant("rotated-access-secret")
        await credential.refresh_for_turn(broker, plan_handle())
        assert [token for _, token, _ in broker.bindings] == [b"first-access-secret", b"rotated-access-secret"]
        assert broker.bindings[0][2] == broker.bindings[1][2]
        assert "first-access-secret" not in repr(initial)
        assert "rotated-access-secret" not in repr(broker.guards)

    asyncio.run(run())


@pytest.mark.parametrize("seconds", [-10, 60])
def test_expired_or_expiring_token_revokes_binding_and_is_terminal(seconds):
    async def run():
        async def get():
            return grant(seconds=seconds)

        broker = PlanBroker()
        with pytest.raises(CodexCredentialUnavailable) as caught:
            await provider(get).provision(broker, plan_handle())
        assert caught.value.reason == "access_token_expired_or_expiring"
        assert is_non_retryable_event_error(caught.value)
        assert not broker.bindings
        assert broker.revoked == [(plan_handle(), "soveren-chatgpt-plan", "conversation")]

    asyncio.run(run())


def test_reauthorization_required_revokes_existing_binding():
    async def run():
        denied = False

        async def get():
            if denied:
                raise CodexCredentialUnavailable()
            return grant()

        broker = PlanBroker()
        credential = provider(get)
        await credential.provision(broker, plan_handle())
        denied = True
        with pytest.raises(CodexCredentialUnavailable):
            await credential.refresh_for_turn(broker, plan_handle())
        assert len(broker.bindings) == 1
        assert len(broker.revoked) == 1

    asyncio.run(run())


def test_transient_owner_failure_is_not_reclassified_as_reauthorization():
    async def run():
        async def get():
            raise TimeoutError("OAuth provider unavailable")

        broker = PlanBroker()
        with pytest.raises(TimeoutError) as caught:
            await provider(get).provision(broker, plan_handle())
        assert not is_non_retryable_event_error(caught.value)
        assert not broker.bindings
        assert len(broker.revoked) == 1

    asyncio.run(run())


def test_native_auth_cache_blocks_siwc_before_reading_grant():
    async def run():
        async def get():
            pytest.fail("native cache must be rejected before token lookup")

        broker = PlanBroker()
        broker.native_auth_present = True
        with pytest.raises(RuntimeError, match="without native auth.json"):
            await provider(get).provision(broker, plan_handle())
        assert not broker.bindings
        assert broker.guards[0][2]["env"] == {"CODEX_HOME": "/codex-home"}

    asyncio.run(run())


@pytest.mark.parametrize("cache", ["absent", "file", "dangling_symlink"])
def test_native_auth_guard_executes_real_shell_without_reading_or_removing_cache(tmp_path, cache):
    async def run():
        looked_up = False
        native_cache = tmp_path / "auth.json"
        if cache == "file":
            native_cache.write_text("fixture-native-cache")
        elif cache == "dangling_symlink":
            native_cache.symlink_to(tmp_path / "missing-native-cache")

        class ShellGuardBroker(PlanBroker):
            async def run_command(self, handle, command, *, env=None, workdir=None):
                process = await asyncio.create_subprocess_exec(
                    *command, env=env, cwd=workdir,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await process.communicate()
                assert not stdout
                if process.returncode:
                    raise RuntimeError(stderr.decode())

        async def get():
            nonlocal looked_up
            looked_up = True
            return grant()

        broker = ShellGuardBroker()
        handle = replace(plan_handle(), codex_home=str(tmp_path), workspace_root=str(tmp_path))
        if cache == "absent":
            await provider(get).provision(broker, handle)
            assert looked_up and len(broker.bindings) == 1
            assert not native_cache.exists()
        else:
            with pytest.raises(RuntimeError, match="without native auth.json"):
                await provider(get).provision(broker, handle)
            assert not looked_up and not broker.bindings
            if cache == "file":
                assert native_cache.read_text() == "fixture-native-cache"
            else:
                assert native_cache.is_symlink()

    asyncio.run(run())


def test_plan_bindings_are_serialized_for_one_credential_provider():
    async def run():
        active = 0
        maximum = 0

        async def get():
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(0)
            active -= 1
            return grant()

        credential = provider(get)
        broker = PlanBroker()
        await asyncio.gather(*(credential.provision(broker, plan_handle()) for _ in range(3)))
        assert maximum == 1
        assert len(broker.bindings) == 3

    asyncio.run(run())


def test_revocation_drains_through_repeated_waiter_cancellation():
    async def run():
        started = asyncio.Event()
        finish = asyncio.Event()
        completed = False

        class SlowRevocation(PlanBroker):
            async def revoke_http_credential(self, handle, *, name, scope="conversation"):
                nonlocal completed
                started.set()
                await finish.wait()
                await super().revoke_http_credential(handle, name=name, scope=scope)
                completed = True

        async def get():
            raise asyncio.CancelledError()

        broker = SlowRevocation()
        task = asyncio.create_task(provider(get).provision(broker, plan_handle()))
        await asyncio.wait_for(started.wait(), 1)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not completed
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        assert completed and len(broker.revoked) == 1

    asyncio.run(run())


def test_failed_revocation_preserves_terminal_original_error():
    async def run():
        class BrokenRevocation(PlanBroker):
            async def revoke_http_credential(self, handle, *, name, scope="conversation"):
                raise TimeoutError("broker unavailable")

        async def get():
            raise CodexCredentialUnavailable()

        with pytest.raises(ExceptionGroup) as caught:
            await provider(get).provision(BrokenRevocation(), plan_handle())
        assert is_non_retryable_event_error(caught.value)
        assert isinstance(caught.value.exceptions[0], CodexCredentialUnavailable)
        assert isinstance(caught.value.exceptions[1], TimeoutError)

    asyncio.run(run())


@pytest.mark.parametrize("token", ["", "bad\ntoken", "non-ascii-\u0410", "x" * (64 * 1024 + 1)])
def test_plan_token_rejects_invalid_bearer_without_echoing_it(token):
    with pytest.raises(ValueError) as caught:
        grant(token)
    assert str(caught.value) == "ChatGPT plan grant requires an ASCII bearer and an aware expiration datetime"


def test_plan_token_rejects_ambiguous_expiration():
    with pytest.raises(ValueError):
        ChatGPTPlanAccessToken("fixture-access", datetime(2026, 10, 15))


@pytest.mark.parametrize("name,version", [(None, "1"), ("Pulsy", None), ("", "1"), ("Pulsy", "bad\nversion")])
def test_client_identity_rejects_missing_or_unsafe_required_fields(name, version):
    with pytest.raises(ValueError):
        CodexClientInfo(name, version)


@pytest.mark.parametrize("seconds", [True, -1, float("inf"), float("nan")])
def test_plan_rejects_invalid_minimum_lifetime(seconds):
    async def get():
        return grant()

    with pytest.raises(ValueError, match="minimum_validity_s"):
        provider(get, minimum_validity_s=seconds)
