"""HTTP contract tests: public registry -> real connector/auth -> local HTTP server."""

from __future__ import annotations

import asyncio
import base64
import json
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import aiohttp
import jwt
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from soveren_agent_platform.conversation import ConversationScope
from soveren_agent_platform.github import (
    GitHubAccess,
    GitHubAppCredentials,
    GitHubConnector,
    GitHubError,
    GitHubErrorCode,
    GitHubRepositoryAccess,
    register_github_tools,
)
from soveren_agent_platform.sessions import DynamicToolRegistry

SCOPE = ConversationScope("tenant", "conversation")
REPO = "owner/repository"
COMMIT, TREE, BLOB, HEAD, BASE = (char * 40 for char in "abcde")
_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
PEM = _KEY.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
CREDS = GitHubAppCredentials("123", 456, PEM)


class Provider:
    def __init__(self):
        self.calls = []
        self.tokens = 0
        self.repository = REPO
        self.token_repositories = ["repository"]
        self.commit = COMMIT
        self.text = b"one\ntwo\nthree\n"
        self.mode = "100644"
        self.size = None
        self.truncated = False
        self.entries = None
        self.merge_base = BASE
        self.files = [{"filename": "src.py", "status": "modified", "patch": "@@ -1 +1 @@\n-old\n+new"}]
        self.status = None
        self.delay = 0
        self.raw_response = None
        self.permissions = {"contents": "read", "pull_requests": "read"}
        self.token_status = None
        self.fail_once = False
        self.malformed = False
        self.token_gate = None
        self.token_started = asyncio.Event()
        self.read_gate = None
        self.read_started = asyncio.Event()
        self.tree_response = None
        self.blob_response = None

    async def handle(self, request):
        self.calls.append((request.method, request.path, dict(request.query)))
        if request.path.startswith("/app/"):
            self.tokens += 1
            signed = request.headers["Authorization"].removeprefix("Bearer ")
            claims = jwt.decode(signed, _KEY.public_key(), algorithms=["RS256"], issuer="123")
            assert claims["iat"] < time.time() < claims["exp"] <= time.time() + 600
            body = await request.json()
            assert body == {
                "repositories": self.token_repositories,
                "permissions": self.permissions,
            }
            assert request.method == "POST"
            self.token_started.set()
            if self.token_gate is not None:
                await self.token_gate.wait()
            if self.token_status:
                return web.Response(status=self.token_status, text="secret-installation-token")
            return web.json_response(
                {
                    "token": f"secret-installation-token-{self.tokens}",
                    "expires_at": datetime.fromtimestamp(time.time() + 3600, UTC).isoformat(),
                }
            )
        assert request.method == "GET"
        assert request.headers["Authorization"].startswith("Bearer secret-installation-token-")
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.raw_response is not None:
            return web.Response(body=self.raw_response)
        if self.status:
            status = self.status
            if self.fail_once:
                self.status = None
            headers = {"Retry-After": "1"} if status == 429 else {}
            return web.Response(status=status, text="secret-installation-token", headers=headers)
        if self.malformed:
            return web.Response(text="not JSON secret-installation-token")
        self.read_started.set()
        if self.read_gate is not None:
            await self.read_gate.wait()
        if "/commits/" in request.path:
            ref = request.path.rsplit("/", 1)[1]
            commit = ref if len(ref) == 40 else self.commit
            return web.json_response({"sha": commit, "commit": {"tree": {"sha": TREE}}})
        if "/git/trees/" in request.path:
            entries = self.entries
            if entries is None:
                entries = [
                    {
                        "path": "src.py",
                        "sha": BLOB,
                        "type": "blob",
                        "mode": self.mode,
                        "size": self.size if self.size is not None else len(self.text),
                    }
                ]
            return web.json_response(
                self.tree_response
                if self.tree_response is not None
                else {"sha": request.path.rsplit("/", 1)[1], "tree": entries, "truncated": self.truncated}
            )
        if "/git/blobs/" in request.path:
            return web.json_response(
                self.blob_response
                if self.blob_response is not None
                else {
                    "sha": BLOB,
                    "content": base64.b64encode(self.text).decode(),
                    "encoding": "base64",
                    "size": len(self.text),
                }
            )
        if "/pulls/" in request.path:
            return web.json_response(
                {
                    "number": 1,
                    "title": "Change",
                    "state": "open",
                    "changed_files": len(self.files),
                    "base": {"sha": BASE, "repo": {"full_name": self.repository}},
                    "head": {"sha": HEAD},
                }
            )
        if "/compare/" in request.path:
            assert request.path.endswith(f"{BASE}...{HEAD}")
            assert dict(request.query) == {"per_page": "1", "page": "1"}
            return web.json_response({"merge_base_commit": {"sha": self.merge_base}, "files": self.files})
        raise AssertionError(request.path)


@asynccontextmanager
async def wired(monkeypatch, *, operations=None, repository=REPO, refs=("test",)):
    provider = Provider()
    provider.token_repositories = [repository.split("/")[1]]
    app = web.Application()
    app.router.add_route("*", "/{path:.*}", provider.handle)
    async with TestServer(app) as server:
        original = aiohttp.ClientSession.request

        def request(session, method, url, **kwargs):
            assert url.startswith("https://api.github.com/")
            assert kwargs["allow_redirects"] is False
            assert session.trust_env is False
            return original(session, method, str(server.make_url(url.removeprefix("https://api.github.com"))), **kwargs)

        monkeypatch.setattr(aiohttp.ClientSession, "request", request)
        grant = GitHubRepositoryAccess(repository, refs, (1,))
        if operations is not None:
            grant = GitHubRepositoryAccess(repository, refs, (1,), operations)
        connector = GitHubConnector(credentials=CREDS, access=GitHubAccess(SCOPE, (grant,)))
        registry = DynamicToolRegistry()
        register_github_tools(registry, connector, scope=SCOPE)
        async with connector:
            yield provider, connector, registry
        assert connector._http._session is None or connector._http._session.closed


async def invoke(registry, operation, **arguments):
    result = await registry.call(
        {
            "callId": "call",
            "threadId": "thread",
            "turnId": "turn",
            "namespace": "platform_github",
            "tool": operation,
            "arguments": {"repository": REPO, **arguments},
        }
    )
    text = result["contentItems"][0]["text"]
    assert "secret-installation-token" not in text
    assert "PRIVATE KEY" not in text
    return result["success"], json.loads(text)


async def pin(registry):
    ok, result = await invoke(registry, "resolve_ref", ref="test")
    assert ok
    return result["commit"]


def test_registry_auth_pinning_and_file_citations(monkeypatch):
    async def scenario():
        async with wired(monkeypatch) as (provider, connector, registry):
            assert registry.conversation == ("tenant", "conversation")
            commit = await pin(registry)
            provider.commit = HEAD
            assert await pin(registry) == commit == COMMIT
            ok, tree = await invoke(registry, "list_tree", commit=commit)
            assert ok and tree["entries"][0]["path"] == "src.py" and not tree["truncated"]
            ok, file = await invoke(registry, "read_file", commit=commit, path="src.py", start_line=2, end_line=2)
            assert ok and file["text"] == "two\n" and file["next_line"] == 3
            assert file["citation"] == {
                "repository": REPO,
                "commit": COMMIT,
                "path": "src.py",
                "start_line": 2,
                "end_line": 2,
                "url": f"https://github.com/{REPO}/blob/{COMMIT}/src.py#L2-L2",
            }
            assert provider.tokens == 1
            assert sum("/commits/test" in path for _, path, _ in provider.calls) == 1
            assert all("/git/trees/" not in path or path.endswith(TREE) for _, path, _ in provider.calls)
            connector.revoke()
            ok, result = await invoke(registry, "resolve_ref", ref="test")
            assert not ok and result["error"] == "connector_closed"

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "operation,arguments",
    [
        ("resolve_ref", {"ref": "main"}),
        ("resolve_ref", {"ref": "test", "tenant_id": "foreign"}),
        ("resolve_ref", {"ref": "test", "repository": "foreign/repo"}),
        ("list_tree", {"commit": COMMIT}),
        ("read_pull_request", {"number": 2}),
        ("read_pull_request", {"number": True}),
    ],
)
def test_denied_before_any_provider_request(monkeypatch, operation, arguments):
    async def scenario():
        async with wired(monkeypatch) as (provider, connector, registry):
            assert not (await invoke(registry, operation, **arguments))[0]
            with pytest.raises(GitHubError, match="scope_denied"):
                await connector.execute(
                    ConversationScope("foreign", "conversation"), "resolve_ref", {"repository": REPO, "ref": "test"}
                )
            with pytest.raises(GitHubError):
                register_github_tools(DynamicToolRegistry(), connector, scope=ConversationScope("tenant", "foreign"))
            assert provider.calls == []

    asyncio.run(scenario())


def test_operation_scope_and_registry_rebinding(monkeypatch):
    async def scenario():
        async with wired(monkeypatch, operations=frozenset({"read_file"})) as (provider, connector, registry):
            assert [spec.name for spec in registry.specs()] == ["read_file"]
            with pytest.raises(GitHubError):
                await connector.execute(SCOPE, "resolve_ref", {"repository": REPO, "ref": "test"})
            with pytest.raises(ValueError):
                registry.bind_conversation(tenant_id="tenant", source_id="foreign")
            assert not provider.calls

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "status,error",
    [
        (401, "authentication_failed"),
        (403, "provider_forbidden"),
        (404, "provider_not_found_or_inaccessible"),
        (429, "provider_rate_limited"),
        (503, "provider_unavailable"),
        (302, "invalid_provider_response"),
    ],
)
def test_http_failures_are_safe_distinguishable(monkeypatch, caplog, status, error):
    async def scenario():
        async with wired(monkeypatch) as (provider, connector, registry):
            provider.status = status
            ok, result = await invoke(registry, "resolve_ref", ref="test")
            assert not ok and result == {"error": error}
            assert provider.tokens == (2 if status == 401 else 1)
            if status == 401:
                assert connector._http._closed

    asyncio.run(scenario())
    assert "secret-installation-token" not in caplog.text and "PRIVATE KEY" not in caplog.text
    assert "PRIVATE KEY" not in repr(CREDS)


def test_malformed_json_and_token_failure(monkeypatch):
    async def scenario():
        async with wired(monkeypatch) as (provider, _, registry):
            provider.malformed = True
            assert (await invoke(registry, "resolve_ref", ref="test"))[1]["error"] == "invalid_provider_response"
            provider.malformed = False
            provider.token_status = 403
            provider.status = 401
            assert (await invoke(registry, "resolve_ref", ref="test"))[1]["error"] == "provider_forbidden"

    asyncio.run(scenario())


def test_single_flight_expiry_and_bounded_401_recovery(monkeypatch):
    async def scenario():
        async with wired(monkeypatch) as (provider, connector, registry):
            commit = await pin(registry)
            connector._http._expiry = time.time() + 30
            results = await asyncio.gather(*(invoke(registry, "list_tree", commit=commit) for _ in range(8)))
            assert all(ok for ok, _ in results) and provider.tokens == 2
            provider.status, provider.fail_once = 401, True
            assert (await invoke(registry, "list_tree", commit=commit))[0]
            assert provider.tokens == 3

    asyncio.run(scenario())


def test_revoke_during_refresh(monkeypatch):
    async def scenario():
        async with wired(monkeypatch) as (provider, connector, registry):
            provider.token_gate = asyncio.Event()
            pending = asyncio.create_task(invoke(registry, "resolve_ref", ref="test"))
            await provider.token_started.wait()
            connector.revoke()
            provider.token_gate.set()
            assert (await pending)[1]["error"] == "connector_closed"
            assert connector._http._token is None
            assert len(provider.calls) == 1

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "kind,error",
    [
        ("binary", "not_a_regular_utf8_text_file"),
        ("utf8", "not_a_regular_utf8_text_file"),
        ("symlink", "not_a_regular_utf8_text_file"),
        ("oversize", "response_too_large"),
        ("missing", "path_not_found"),
        ("truncated_missing", "response_too_large"),
        ("line", "line_exceeds_byte_budget"),
        ("traversal", "invalid_arguments"),
    ],
)
def test_file_failure_semantics(monkeypatch, kind, error):
    async def scenario():
        async with wired(monkeypatch) as (provider, _, registry):
            commit = await pin(registry)
            if kind == "binary":
                provider.text = b"a\0b"
            if kind == "utf8":
                provider.text = b"\xff"
            if kind == "symlink":
                provider.mode = "120000"
            if kind == "oversize":
                provider.size = 256 * 1024 + 1
            if kind in ("missing", "truncated_missing"):
                provider.entries = []
            if kind == "truncated_missing":
                provider.truncated = True
            if kind == "line":
                provider.text = b"x" * 25_000
            path = "../src.py" if kind == "traversal" else "src.py"
            ok, result = await invoke(registry, "read_file", commit=commit, path=path)
            assert not ok and result["error"] == error
            assert (
                not any("/git/blobs/" in path for _, path, _ in provider.calls)
                if kind in ("symlink", "oversize", "missing", "truncated_missing", "traversal")
                else True
            )

    asyncio.run(scenario())


def test_empty_and_bounded_file_tree_pages(monkeypatch):
    async def scenario():
        async with wired(monkeypatch) as (provider, _, registry):
            commit = await pin(registry)
            provider.text = b""
            ok, result = await invoke(registry, "read_file", commit=commit, path="src.py")
            assert ok and result["text"] == "" and not result["truncated"]
            provider.text = b"one\ntwo\n"
            ok, result = await invoke(registry, "read_file", commit=commit, path="src.py", max_bytes=4)
            assert ok and result["text"] == "one\n" and result["next_line"] == 2
            provider.entries = [{"path": f"src/{i:04}", "type": "blob", "sha": BLOB} for i in range(220)]
            provider.truncated = True
            ok, result = await invoke(registry, "list_tree", commit=commit, prefix="src")
            assert ok and result["next_offset"] == 200 and result["provider_truncated"]
            _, result = await invoke(registry, "list_tree", commit=commit, prefix="src", offset=200)
            assert len(result["entries"]) == 20 and result["truncated"] and result["next_offset"] is None

    asyncio.run(scenario())


def test_pr_immutable_compare_and_partial_patches(monkeypatch):
    async def scenario():
        async with wired(monkeypatch) as (provider, _, registry):
            ok, result = await invoke(registry, "read_pull_request", number=1)
            assert ok and result["head_commit"] == HEAD and result["base_commit"] == BASE
            assert result["file_list_complete"] and result["patch_completeness"] == "not_guaranteed_by_provider"
            assert result["files"][0]["head_citation"]["commit"] == HEAD
            provider.files = [{"filename": f"{i}.py", "status": "modified"} for i in range(300)]
            ok, result = await invoke(registry, "read_pull_request", number=1)
            assert ok and not result["file_list_complete"] and result["truncated"]
            assert result["next_offset"] is not None and not result["files"][0]["patch_available"]
            assert sum("/pulls/" in path for _, path, _ in provider.calls) == 1
            assert (await invoke(registry, "list_tree", commit=HEAD))[0]

    asyncio.run(scenario())


def test_safe_handling_of_unexpected_adapter_failure(monkeypatch, caplog):
    async def scenario():
        async with wired(monkeypatch) as (_, connector, registry):
            original = connector._http._request

            async def failure(*args, **kwargs):
                raise TimeoutError("secret-installation-token")

            monkeypatch.setattr(connector._http, "_request", failure)
            assert (await invoke(registry, "resolve_ref", ref="test"))[1]["error"] == "provider_timeout"

            async def unexpected(*args, **kwargs):
                raise RuntimeError("secret-installation-token")

            monkeypatch.setattr(connector._http, "_request", unexpected)
            assert not (await invoke(registry, "resolve_ref", ref="test"))[0]
            monkeypatch.setattr(connector._http, "_request", original)

    asyncio.run(scenario())
    assert "secret-installation-token" not in caplog.text


def test_invalid_trusted_configuration():
    with pytest.raises(ValueError):
        GitHubRepositoryAccess("https://evil/repo")
    with pytest.raises(ValueError):
        GitHubRepositoryAccess(REPO, operations=frozenset({"write"}))
    with pytest.raises(ValueError):
        GitHubAccess(SCOPE, (GitHubRepositoryAccess(REPO), GitHubRepositoryAccess(REPO)))
    for repository in ("owner/.", "owner/.."):
        with pytest.raises(ValueError):
            GitHubRepositoryAccess(repository)
    for ref in (".", "..", "test\n"):
        with pytest.raises(ValueError):
            GitHubRepositoryAccess(REPO, refs=(ref,))
    assert GitHubError(GitHubErrorCode.AUTH).args == ("authentication_failed",)


def test_actual_http_timeout_and_stream_limit(monkeypatch):
    async def scenario():
        async with wired(monkeypatch) as (provider, connector, registry):
            await pin(registry)
            session = connector._http._session
            await session.close()
            connector._http._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=0.01))
            provider.delay = 0.05
            ok, result = await invoke(registry, "list_tree", commit=COMMIT)
            assert not ok and result["error"] == "provider_timeout"
            provider.delay = 0
            await connector._http._session.close()
            connector._http._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
            provider.raw_response = b"x" * (8 * 1024 * 1024 + 1)
            ok, result = await invoke(registry, "list_tree", commit=COMMIT)
            assert not ok and result["error"] == "response_too_large"

    asyncio.run(scenario())


def test_continuation_default_end_and_lf_numbering(monkeypatch):
    async def scenario():
        async with wired(monkeypatch) as (provider, _, registry):
            await pin(registry)
            provider.text = ("x\u2028y\n" + "line\n" * 210).encode()
            ok, result = await invoke(registry, "read_file", commit=COMMIT, path="src.py", start_line=201)
            assert ok and result["total_lines"] == 211 and result["citation"]["start_line"] == 201
            assert result["citation"]["end_line"] == 211 and not result["truncated"]

    asyncio.run(scenario())


def test_large_first_page_item_is_failure_not_empty_progress(monkeypatch):
    async def scenario():
        async with wired(monkeypatch) as (provider, _, registry):
            await pin(registry)
            provider.entries = [{"path": "x" * 25_000, "type": "blob", "sha": BLOB}]
            ok, result = await invoke(registry, "list_tree", commit=COMMIT)
            assert not ok and result["error"] == "response_too_large"
            provider.files = [{"filename": "x" * 25_000, "status": "modified"}]
            ok, result = await invoke(registry, "read_pull_request", number=1)
            assert not ok and result["error"] == "response_too_large"

    asyncio.run(scenario())


def test_minimal_permissions_and_saturated_admission(monkeypatch):
    async def scenario():
        async with wired(monkeypatch, operations=frozenset({"resolve_ref", "list_tree"})) as (
            provider,
            connector,
            registry,
        ):
            provider.permissions = {"contents": "read"}
            provider.token_gate = asyncio.Event()
            pending = [asyncio.create_task(invoke(registry, "resolve_ref", ref="test")) for _ in range(8)]
            await provider.token_started.wait()
            ok, result = await invoke(registry, "resolve_ref", ref="test")
            assert not ok and result["error"] == "connector_busy"
            provider.token_gate.set()
            assert all(ok for ok, _ in await asyncio.gather(*pending))
            assert provider.tokens == 1

    asyncio.run(scenario())


def test_diff_rename_delete_add_citations_and_merge_base_reads(monkeypatch):
    async def scenario():
        async with wired(monkeypatch) as (provider, _, registry):
            provider.merge_base = "f" * 40
            provider.files = [
                {"filename": "new.py", "previous_filename": "old.py", "status": "renamed", "patch": "@@ -1 +1 @@"},
                {"filename": "deleted.py", "status": "removed"},
                {"filename": "added.py", "status": "added"},
            ]
            ok, result = await invoke(registry, "read_pull_request", number=1)
            assert ok
            renamed, deleted, added = result["files"]
            assert renamed["base_citation"]["path"] == "old.py"
            assert renamed["base_citation"]["commit"] == provider.merge_base
            assert renamed["head_citation"]["path"] == "new.py"
            assert deleted["head_citation"] is None and added["base_citation"] is None
            assert (await invoke(registry, "read_file", commit=provider.merge_base, path="src.py"))[0]

    asyncio.run(scenario())


def test_expired_refresh_denial_does_not_fall_back(monkeypatch):
    async def scenario():
        async with wired(monkeypatch) as (provider, connector, registry):
            await pin(registry)
            connector._http._expiry = time.time() + 10
            provider.token_status = 403
            before = len(provider.calls)
            ok, result = await invoke(registry, "list_tree", commit=COMMIT)
            assert not ok and result["error"] == "provider_forbidden"
            assert connector._http._token is None
            assert len(provider.calls) == before + 1
            assert provider.calls[-1][0] == "POST"

    asyncio.run(scenario())


def test_core_import_without_optional_github_dependencies():
    import subprocess
    import sys

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import importlib.abc
import sys
class NoGitHubDependencies(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'aiohttp', 'jwt', 'cryptography'}:
            raise ImportError('optional GitHub dependency forbidden')
sys.meta_path.insert(0, NoGitHubDependencies())
from soveren_agent_platform.app_api import AgentPlatformApp
from soveren_agent_platform.sessions import DynamicToolRegistry
assert 'soveren_agent_platform.github' not in sys.modules
""",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("repository", ["OWNER/repository", "owner/Repository"])
def test_pr_accepts_canonical_repository_casing(monkeypatch, repository):
    async def scenario():
        async with wired(monkeypatch, repository=repository) as (provider, _, registry):
            ok, result = await invoke(registry, "read_pull_request", repository=repository, number=1)
            assert ok and result["repository"] == repository
            assert provider.tokens == 1
            assert result["head_commit"] == HEAD

    asyncio.run(scenario())


@pytest.mark.parametrize("foreign_repository", ["foreign/repository", "owner/foreign"])
def test_pr_rejects_foreign_canonical_repository(monkeypatch, foreign_repository):
    async def scenario():
        async with wired(monkeypatch) as (provider, _, registry):
            provider.repository = foreign_repository
            ok, result = await invoke(registry, "read_pull_request", number=1)
            assert not ok and result["error"] == "invalid_provider_response"
            assert len(provider.calls) == 2  # Token and metadata; no commit/diff reads.

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", [403, 503, "timeout"])
def test_refresh_failure_is_shared_and_later_call_can_retry(monkeypatch, failure):
    async def scenario():
        async with wired(monkeypatch) as (provider, connector, registry):
            await pin(registry)
            connector._http._expiry = time.time() + 30
            provider.token_started.clear()
            provider.token_gate = asyncio.Event()
            if failure == "timeout":
                await connector._http._session.close()
                connector._http._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=0.1))
            else:
                provider.token_status = failure
            pending = [asyncio.create_task(invoke(registry, "list_tree", commit=COMMIT)) for _ in range(8)]
            await provider.token_started.wait()
            if failure != "timeout":
                provider.token_gate.set()
            results = await asyncio.gather(*pending)
            provider.token_gate.set()
            error = {403: "provider_forbidden", 503: "provider_unavailable", "timeout": "provider_timeout"}[failure]
            assert all(not ok and result == {"error": error} for ok, result in results)
            assert provider.tokens == 2  # Initial token plus one shared failed refresh.
            assert connector._http._token is None
            provider.token_status = None
            if failure == "timeout":
                await connector._http._session.close()
                connector._http._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
                provider.delay = 0.12  # The explicit retry uses normal request deadlines.
            assert (await invoke(registry, "list_tree", commit=COMMIT))[0]
            assert provider.tokens == 3

    asyncio.run(scenario())


@pytest.mark.parametrize("cancelled_index", [0, 1])
def test_cancelling_one_refresh_waiter_preserves_other_call(monkeypatch, cancelled_index):
    async def scenario():
        async with wired(monkeypatch) as (provider, connector, registry):
            await pin(registry)
            connector._http._expiry = time.time() + 30
            provider.token_started.clear()
            provider.token_gate = asyncio.Event()
            pending = [asyncio.create_task(invoke(registry, "list_tree", commit=COMMIT)) for _ in range(2)]
            await provider.token_started.wait()
            pending[cancelled_index].cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending[cancelled_index]
            provider.token_gate.set()
            assert (await pending[1 - cancelled_index])[0]
            assert provider.tokens == 2

    asyncio.run(scenario())


def test_cancel_all_refresh_waiters_joins_orphan_and_allows_new_read(monkeypatch):
    async def scenario():
        async with wired(monkeypatch) as (provider, connector, registry):
            await pin(registry)
            connector._http._expiry = time.time() + 30
            provider.token_started.clear()
            provider.token_gate = asyncio.Event()
            pending = [asyncio.create_task(invoke(registry, "list_tree", commit=COMMIT)) for _ in range(2)]
            await provider.token_started.wait()
            refresh = connector._http._refresh.task
            for call in pending:
                call.cancel()
            results = await asyncio.gather(*pending, return_exceptions=True)
            provider.token_gate.set()
            assert all(isinstance(result, asyncio.CancelledError) for result in results)
            assert refresh.done() and connector._http._refresh is None
            assert connector._http._token is None
            assert (await invoke(registry, "list_tree", commit=COMMIT))[0]
            assert provider.tokens == 3

    asyncio.run(scenario())


@pytest.mark.parametrize("shutdown", ["revoke", "close"])
def test_shutdown_terminates_shared_refresh_without_returning_token(monkeypatch, shutdown):
    async def scenario():
        async with wired(monkeypatch) as (provider, connector, registry):
            await pin(registry)
            connector._http._expiry = time.time() + 30
            provider.token_started.clear()
            provider.token_gate = asyncio.Event()
            pending = [asyncio.create_task(invoke(registry, "list_tree", commit=COMMIT)) for _ in range(2)]
            await provider.token_started.wait()
            refresh = connector._http._refresh.task
            if shutdown == "revoke":
                connector.revoke()
            else:
                await connector.close()
            results = await asyncio.gather(*pending)
            provider.token_gate.set()
            assert all(not ok and result == {"error": "connector_closed"} for ok, result in results)
            assert refresh.done() and connector._http._refresh is None
            assert connector._http._token is None and connector._http._credentials is None
            assert provider.tokens == 2

    asyncio.run(scenario())


def test_new_read_waits_for_cancelled_refresh_cleanup(monkeypatch):
    async def scenario():
        async with wired(monkeypatch) as (provider, connector, registry):
            await pin(registry)
            connector._http._expiry = time.time() + 30
            provider.token_started.clear()
            provider.token_gate = asyncio.Event()
            original = asyncio.create_task(invoke(registry, "list_tree", commit=COMMIT))
            await provider.token_started.wait()
            original.cancel()
            replacement = asyncio.create_task(invoke(registry, "list_tree", commit=COMMIT))
            with pytest.raises(asyncio.CancelledError):
                await original
            provider.token_gate.set()
            assert (await replacement)[0]
            assert provider.tokens == 3

    asyncio.run(scenario())


def test_concurrent_401_recovery_shares_failed_refresh(monkeypatch):
    async def scenario():
        async with wired(monkeypatch) as (provider, connector, registry):
            await pin(registry)
            provider.status = 401
            provider.token_status = 403
            provider.token_started.clear()
            provider.token_gate = asyncio.Event()
            pending = [asyncio.create_task(invoke(registry, "list_tree", commit=COMMIT)) for _ in range(8)]
            await provider.token_started.wait()
            provider.token_gate.set()
            results = await asyncio.gather(*pending)
            assert all(not ok and result == {"error": "provider_forbidden"} for ok, result in results)
            assert provider.tokens == 2 and connector._http._token is None

    asyncio.run(scenario())


@pytest.mark.parametrize("operation", ["resolve_ref", "read_pull_request"])
@pytest.mark.parametrize("failure", [403, 503, "timeout"])
def test_initial_pin_shares_failed_refresh_and_later_retry(monkeypatch, operation, failure):
    async def scenario():
        async with wired(monkeypatch, refs=("test", "dev")) as (provider, connector, registry):
            await pin(registry)
            connector._http._expiry = time.time() + 30
            provider.token_started.clear()
            provider.token_gate = asyncio.Event()
            if failure == "timeout":
                await connector._http._session.close()
                connector._http._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=0.1))
            else:
                provider.token_status = failure
            args = {"ref": "dev"} if operation == "resolve_ref" else {"number": 1}
            pending = [asyncio.create_task(invoke(registry, operation, **args)) for _ in range(8)]
            await provider.token_started.wait()
            if failure != "timeout":
                provider.token_gate.set()
            results = await asyncio.gather(*pending)
            provider.token_gate.set()
            error = {403: "provider_forbidden", 503: "provider_unavailable", "timeout": "provider_timeout"}[failure]
            assert all(not ok and result == {"error": error} for ok, result in results)
            assert provider.tokens == 2
            provider.token_status = None
            if failure == "timeout":
                await connector._http._session.close()
                connector._http._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
                provider.delay = 0.12  # The explicit retry uses normal request deadlines.
            assert (await invoke(registry, operation, **args))[0]
            assert provider.tokens == 3

    asyncio.run(scenario())


@pytest.mark.parametrize("operation", ["resolve_ref", "read_pull_request"])
def test_concurrent_pin_reads_metadata_once_and_preserves_snapshot(monkeypatch, operation):
    async def scenario():
        async with wired(monkeypatch) as (provider, _, registry):
            provider.read_gate = asyncio.Event()
            args = {"ref": "test"} if operation == "resolve_ref" else {"number": 1}
            pending = [asyncio.create_task(invoke(registry, operation, **args)) for _ in range(8)]
            await provider.read_started.wait()
            provider.read_gate.set()
            results = await asyncio.gather(*pending)
            assert all(ok for ok, _ in results)
            assert all(result == results[0][1] for _, result in results)
            path = f"/repos/{REPO}/commits/test" if operation == "resolve_ref" else f"/repos/{REPO}/pulls/1"
            assert sum(url == path for _, url, _ in provider.calls) == 1
            provider.commit = HEAD
            assert (await invoke(registry, operation, **args))[1] == results[0][1]
            assert sum(url == path for _, url, _ in provider.calls) == 1

    asyncio.run(scenario())


def test_different_pin_keys_are_not_serialized(monkeypatch):
    async def scenario():
        async with wired(monkeypatch, refs=("test", "dev")) as (provider, _, registry):
            provider.read_gate = asyncio.Event()
            pending = [asyncio.create_task(invoke(registry, "resolve_ref", ref=ref)) for ref in ("test", "dev")]
            pending.append(asyncio.create_task(invoke(registry, "read_pull_request", number=1)))
            try:
                async with asyncio.timeout(0.5):
                    while sum("/commits/" in path or "/pulls/" in path for _, path, _ in provider.calls) < 3:
                        await asyncio.sleep(0)
            finally:
                provider.read_gate.set()
                results = await asyncio.gather(*pending)
            assert all(ok for ok, _ in results)
            assert provider.tokens == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("operation", ["resolve_ref", "read_pull_request"])
@pytest.mark.parametrize("shutdown", ["cancel_one", "cancel_all", "revoke", "close"])
@pytest.mark.parametrize("phase", ["token", "metadata"])
def test_initial_pin_cancellation_and_shutdown(monkeypatch, operation, shutdown, phase):
    async def scenario():
        async with wired(monkeypatch) as (provider, connector, registry):
            gate = asyncio.Event()
            if phase == "token":
                provider.token_gate = gate
                started = provider.token_started
            else:
                provider.read_gate = gate
                started = provider.read_started
            args = {"ref": "test"} if operation == "resolve_ref" else {"number": 1}
            pending = [asyncio.create_task(invoke(registry, operation, **args)) for _ in range(2)]
            await started.wait()
            if shutdown.startswith("cancel"):
                pending[0].cancel()
                if shutdown == "cancel_all":
                    pending[1].cancel()
                with pytest.raises(asyncio.CancelledError):
                    await pending[0]
                if shutdown == "cancel_all":
                    with pytest.raises(asyncio.CancelledError):
                        await pending[1]
                    assert connector._http._refresh is None
                    assert not connector._refs and not connector._pulls and not connector._snapshots
                gate.set()
                if shutdown == "cancel_one":
                    assert (await pending[1])[0]
                else:
                    assert (await invoke(registry, operation, **args))[0]
                assert provider.tokens == (2 if shutdown == "cancel_all" and phase == "token" else 1)
            else:
                if shutdown == "revoke":
                    connector.revoke()
                else:
                    await connector.close()
                results = await asyncio.gather(*pending)
                gate.set()
                assert all(not ok and result == {"error": "connector_closed"} for ok, result in results)
                assert not connector._refs and not connector._pulls and not connector._snapshots
                assert connector._http._refresh is None

    asyncio.run(scenario())


@pytest.mark.parametrize("operation", ["resolve_ref", "read_pull_request"])
def test_initial_pin_shares_metadata_failure_without_caching_it(monkeypatch, operation):
    async def scenario():
        async with wired(monkeypatch) as (provider, _, registry):
            # Authorization succeeds; it is the initial metadata GET that fails.
            provider.status = 503
            args = {"ref": "test"} if operation == "resolve_ref" else {"number": 1}
            pending = [asyncio.create_task(invoke(registry, operation, **args)) for _ in range(8)]
            results = await asyncio.gather(*pending)
            assert all(not ok and result == {"error": "provider_unavailable"} for ok, result in results)
            assert len(provider.calls) == 2  # Token and one shared metadata GET.
            provider.status = None
            assert (await invoke(registry, operation, **args))[0]
            assert provider.tokens == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("operation", ["list_tree", "read_file"])
@pytest.mark.parametrize("sha", [None, "f" * 40])
def test_rejects_tree_with_missing_or_different_identity(monkeypatch, operation, sha):
    async def scenario():
        async with wired(monkeypatch) as (provider, _, registry):
            await pin(registry)
            provider.tree_response = {"tree": [], "truncated": False}
            if sha is not None:
                provider.tree_response["sha"] = sha
            args = {"path": "src.py"} if operation == "read_file" else {}
            ok, result = await invoke(registry, operation, commit=COMMIT, **args)
            assert not ok and result == {"error": "invalid_provider_response"}

    asyncio.run(scenario())


@pytest.mark.parametrize("defect", ["missing_sha", "wrong_sha", "short_content", "entry_size", "bad_base64"])
def test_rejects_blob_identity_or_size_mismatch(monkeypatch, defect):
    async def scenario():
        async with wired(monkeypatch) as (provider, _, registry):
            await pin(registry)
            provider.blob_response = {
                "sha": BLOB,
                "content": base64.b64encode(provider.text).decode(),
                "encoding": "base64",
                "size": len(provider.text),
            }
            if defect == "missing_sha":
                del provider.blob_response["sha"]
            elif defect == "wrong_sha":
                provider.blob_response["sha"] = "f" * 40
            elif defect == "short_content":
                provider.blob_response["content"] = ""
            elif defect == "entry_size":
                provider.size = len(provider.text) + 1
            else:
                provider.blob_response["content"] = "!!!!"
            ok, result = await invoke(registry, "read_file", commit=COMMIT, path="src.py")
            assert not ok and result == {"error": "invalid_provider_response"}

    asyncio.run(scenario())
