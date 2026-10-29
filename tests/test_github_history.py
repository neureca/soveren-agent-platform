"""Public tools -> scoped connector -> actual auth/HTTP, without real credentials."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from test_github_connector import BASE, COMMIT, CREDS, HEAD, REPO, SCOPE, TREE, Provider, invoke, pin

from soveren_agent_platform.conversation import ConversationScope
from soveren_agent_platform.github import (
    GitHubAccess,
    GitHubConnector,
    GitHubRepositoryAccess,
    register_github_tools,
)
from soveren_agent_platform.sessions import DynamicToolRegistry

OLD, FOREIGN = "f" * 40, "9" * 40
PATCH = {"filename": "src.py", "status": "modified", "patch": "@@ -1 +1 @@\n-text-left\n+text-center"}


def commit_data(sha, *, files=None):
    date = "2026-09-23T10:00:00Z" if sha == OLD else "2026-10-26T10:00:00Z"
    parent = OLD if sha == BASE else BASE
    return {
        "sha": sha,
        "commit": {
            "tree": {"sha": TREE},
            "message": "old diagnostics" if sha == OLD else "Center headings",
            "author": {"date": date},
            "committer": {"date": date},
        },
        "parents": [] if sha == OLD else [{"sha": parent}],
        "stats": {"additions": 1, "deletions": 1, "total": 2},
        "files": files if files is not None else [PATCH],
    }


class HistoryProvider(Provider):
    def __init__(self):
        super().__init__()
        self.history = [commit_data(HEAD), commit_data(OLD)]
        self.commit_files = [PATCH]
        self.compare_files = [PATCH]
        self.pr_head = HEAD
        self.pr_base_ref = "test"
        self.history_override = None
        self.comparison_status = "ahead"
        self.before_result = None

    async def handle(self, request):
        path = request.path
        new = (
            path.endswith("/commits")
            or "/compare/" in path
            or path.endswith("/pulls")
            or "/pulls/" in path
            or ("/commits/" in path and len(path.rsplit("/", 1)[-1]) == 40)
        )
        if not new:
            return await super().handle(request)
        self.calls.append((request.method, path, dict(request.query)))
        assert request.method == "GET"
        assert request.headers["Authorization"].startswith("Bearer secret-installation-token-")
        if self.before_result is not None:
            await self.before_result()
        if self.status:
            return web.Response(status=self.status, text="secret-installation-token")
        if path.endswith("/commits"):
            assert request.query["sha"] == COMMIT
            if self.history_override is not None:
                return web.json_response(self.history_override)
            start = (int(request.query["page"]) - 1) * 20
            return web.json_response(self.history[start : start + 20])
        if "/commits/" in path:
            sha = path.rsplit("/", 1)[-1]
            start = (int(request.query.get("page", "1")) - 1) * 30
            return web.json_response(commit_data(sha, files=self.commit_files[start : start + 30]))
        if "/compare/" in path:
            base, head = path.rsplit("/", 1)[-1].split("...")
            proof = request.query["page"] == "2"
            ancestors = {
                COMMIT: {COMMIT, HEAD, BASE, OLD},
                HEAD: {HEAD, BASE, OLD},
                BASE: {BASE, OLD},
                OLD: {OLD},
                FOREIGN: {FOREIGN, OLD},
            }
            if base == head:
                status, merge = "identical", base
            elif base in ancestors[head]:
                status, merge = "ahead", base
            elif head in ancestors[base]:
                status, merge = "behind", head
            else:
                status, merge = "diverged", OLD
            if not proof and self.comparison_status == "diverged":
                status, merge = "diverged", OLD
            return web.json_response(
                {
                    "base_commit": commit_data(base),
                    "merge_base_commit": {"sha": merge},
                    "status": status,
                    "total_commits": 2,
                    "ahead_by": 2,
                    "behind_by": int(status == "diverged"),
                    **({} if proof else {"files": self.compare_files}),
                }
            )
        item = {
            "number": 1,
            "title": "Center headings",
            "state": "closed",
            "created_at": "2026-10-25T10:00:00Z",
            "merged_at": "2026-10-26T10:00:00Z",
            "changed_files": len(self.compare_files),
            "base": {"ref": self.pr_base_ref, "sha": BASE, "repo": {"full_name": REPO}},
            "head": {"ref": "feature", "sha": self.pr_head},
        }
        return web.json_response([item] if path.endswith("/pulls") else item)


@asynccontextmanager
async def wired_history(monkeypatch, *, pr_refs=True, refs=("test",)):
    provider = HistoryProvider()
    provider.permissions = {"contents": "read", **({"pull_requests": "read"} if pr_refs else {})}
    app = web.Application()
    app.router.add_route("*", "/{path:.*}", provider.handle)
    async with TestServer(app) as server:
        original = aiohttp.ClientSession.request

        def request(session, method, url, **kwargs):
            assert url.startswith("https://api.github.com/")
            assert kwargs["allow_redirects"] is False and session.trust_env is False
            return original(session, method, str(server.make_url(url.removeprefix("https://api.github.com"))), **kwargs)

        monkeypatch.setattr(aiohttp.ClientSession, "request", request)
        grant = GitHubRepositoryAccess(REPO, refs, pull_requests_from_refs=pr_refs)
        connector = GitHubConnector(credentials=CREDS, access=GitHubAccess(SCOPE, (grant,)))
        registry = DynamicToolRegistry()
        register_github_tools(registry, connector, scope=SCOPE)
        try:
            async with connector:
                yield provider, connector, registry
        finally:
            monkeypatch.setattr(aiohttp.ClientSession, "request", original)


def test_contract_preserves_positional_operation_argument_and_validates_new_policy():
    assert GitHubRepositoryAccess(REPO, ("test",), (), frozenset({"read_file"})).pull_requests_from_refs is False
    with pytest.raises(ValueError):
        GitHubRepositoryAccess(REPO, pull_requests_from_refs="yes")


def test_history_uses_pinned_sha_explicit_utc_interval_and_excludes_old_diagnostic(monkeypatch):
    async def scenario():
        async with wired_history(monkeypatch) as (provider, _, registry):
            await pin(registry)
            ok, result = await invoke(
                registry,
                "list_commits",
                commit=COMMIT,
                since="2026-10-23T03:00:00+03:00",
                until="2026-10-29T00:00:00Z",
                path="src.py",
            )
            assert ok and [item["commit"] for item in result["commits"]] == [HEAD]
            assert result["commits"][0]["citation"]["url"].endswith("/commit/" + HEAD)
            assert result["date_basis"] == "commit_committer_timestamp_not_branch_arrival_or_deployment"
            assert result["next_page"] is None and result["since"] == "2026-10-23T00:00:00Z"
            assert provider.calls[-1][2] == {
                "sha": COMMIT,
                "since": "2026-10-23T00:00:00Z",
                "until": "2026-10-29T00:00:00Z",
                "path": "src.py",
                "per_page": "20",
                "page": "1",
            }

    asyncio.run(scenario())


def test_history_full_filtered_page_still_has_continuation_and_exclusive_end(monkeypatch):
    async def scenario():
        async with wired_history(monkeypatch) as (provider, _, registry):
            await pin(registry)
            provider.history = [commit_data(OLD)] * 20
            ok, result = await invoke(registry, "list_commits", commit=COMMIT, since="2026-10-23T00:00:00Z")
            assert ok and result["commits"] == [] and result["next_page"] == 2 and result["truncated"]
            provider.history = [commit_data(HEAD)]
            ok, result = await invoke(registry, "list_commits", commit=COMMIT, until="2026-10-26T10:00:00Z", page=2)
            assert ok and result["commits"] == [] and result["next_page"] is None

    asyncio.run(scenario())


def test_historical_commit_diff_and_file_read_require_ancestry_proof(monkeypatch):
    async def scenario():
        async with wired_history(monkeypatch) as (provider, _, registry):
            await pin(registry)
            ok, result = await invoke(registry, "read_commit", commit=HEAD, anchor_commit=COMMIT)
            assert ok and result["files"][0]["patch"].endswith("+text-center")
            assert result["diff_kind"] == "first_parent_to_commit" and result["base_commit"] == BASE
            assert result["file_list_complete"] and result["next_offset"] is None
            assert any(
                path.endswith(HEAD + "..." + COMMIT) and query["page"] == "2" for _, path, query in provider.calls
            )
            ok, result = await invoke(registry, "read_file", commit=HEAD, anchor_commit=COMMIT, path="src.py")
            assert ok and result["citation"]["commit"] == HEAD
            before = len(provider.calls)
            ok, result = await invoke(registry, "read_commit", commit=FOREIGN, anchor_commit=COMMIT)
            assert not ok and result["error"] == "scope_denied"
            assert len(provider.calls) == before + 1 and provider.calls[-1][1].endswith(FOREIGN + "..." + COMMIT)
            assert not any(path.endswith("/commits/" + FOREIGN) for _, path, _ in provider.calls)

    asyncio.run(scenario())


def test_historical_access_survives_trusted_ref_restore_without_new_ref_resolution(monkeypatch):
    async def scenario():
        async with wired_history(monkeypatch) as (_, connector, registry):
            await pin(registry)
            saved = connector.get_ref_snapshot(SCOPE, repository=REPO, ref="test")
        async with wired_history(monkeypatch) as (provider, connector, registry):
            await connector.restore_snapshot(SCOPE, repository=REPO, ref="test", snapshot=saved)
            ok, result = await invoke(registry, "read_commit", commit=HEAD, anchor_commit=COMMIT)
            assert ok and result["commit"] == HEAD
            assert not any(path.endswith("/commits/test") for _, path, _ in provider.calls)

    asyncio.run(scenario())


def test_commit_file_windows_missing_patch_and_provider_cap_are_explicit(monkeypatch):
    async def scenario():
        async with wired_history(monkeypatch) as (provider, _, registry):
            await pin(registry)
            provider.commit_files = [{"filename": f"p{i}.py", "status": "modified", "patch": "x"} for i in range(31)]
            ok, first = await invoke(registry, "read_commit", commit=COMMIT, anchor_commit=COMMIT)
            assert ok and first["next_offset"] == 30 and first["truncated"]
            provider.commit_files[-1].pop("patch")
            ok, last = await invoke(registry, "read_commit", commit=COMMIT, anchor_commit=COMMIT, offset=30)
            assert ok and last["next_offset"] is None and last["truncated"]
            assert not last["files"][0]["patch_available"] and last["files"][0]["patch"] is None
            provider.commit_files = [PATCH] * 3000
            ok, cap = await invoke(registry, "read_commit", commit=COMMIT, anchor_commit=COMMIT, offset=2970)
            assert ok and cap["provider_file_limit_reached"] and cap["truncated"] and cap["next_offset"] is None

    asyncio.run(scenario())


def test_compare_distinguishes_divergent_semantics_and_file_cap(monkeypatch):
    async def scenario():
        async with wired_history(monkeypatch) as (provider, _, registry):
            await pin(registry)
            ok, result = await invoke(
                registry, "compare_commits", base_commit=BASE, head_commit=HEAD, anchor_commit=COMMIT
            )
            assert ok and result["exact_base_to_head"] and result["merge_base_commit"] == BASE
            provider.comparison_status = "diverged"
            provider.compare_files = [PATCH] * 300
            ok, result = await invoke(
                registry, "compare_commits", base_commit=BASE, head_commit=HEAD, anchor_commit=COMMIT
            )
            assert ok and not result["exact_base_to_head"] and not result["file_list_complete"] and result["truncated"]
            assert result["files"][0]["base_citation"]["commit"] == OLD

    asyncio.run(scenario())


def test_branch_scoped_pr_discovery_read_and_unapproved_head_denial(monkeypatch):
    async def scenario():
        async with wired_history(monkeypatch) as (provider, _, registry):
            await pin(registry)
            ok, result = await invoke(registry, "list_pull_requests", ref="test")
            assert ok and result["pull_requests"][0]["number"] == 1 and result["live_discovery"]
            ok, result = await invoke(registry, "read_pull_request", number=1, anchor_commit=COMMIT)
            assert ok and result["head_commit"] == HEAD
        async with wired_history(monkeypatch) as (provider, _, registry):
            await pin(registry)
            provider.pr_head = FOREIGN
            ok, result = await invoke(registry, "read_pull_request", number=1, anchor_commit=COMMIT)
            assert not ok and result["error"] == "scope_denied"
            assert not any(query.get("page") == "1" and "/compare/" in path for _, path, query in provider.calls)
            provider.pr_base_ref = "master"
            before = len(provider.calls)
            ok, result = await invoke(registry, "list_pull_requests", ref="master")
            assert not ok and len(provider.calls) == before

    asyncio.run(scenario())


def test_contents_only_app_policy_does_not_request_or_grant_pr_access(monkeypatch):
    async def scenario():
        async with wired_history(monkeypatch, pr_refs=False) as (provider, _, registry):
            await pin(registry)
            before = len(provider.calls)
            ok, result = await invoke(registry, "list_pull_requests", ref="test")
            assert not ok and result["error"] == "scope_denied" and len(provider.calls) == before

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "args",
    [
        {"since": "2026-10-01"},
        {"since": "2026-10-30T00:00:00Z", "until": "2026-10-23T00:00:00Z"},
        {"path": "../private"},
        {"page": True},
    ],
)
def test_invalid_history_inputs_fail_before_provider(monkeypatch, args):
    async def scenario():
        async with wired_history(monkeypatch) as (provider, _, registry):
            await pin(registry)
            before = len(provider.calls)
            ok, result = await invoke(registry, "list_commits", commit=COMMIT, **args)
            assert not ok and result["error"] == "invalid_arguments" and len(provider.calls) == before

    asyncio.run(scenario())


def test_provider_error_and_malformed_history_are_not_empty_success(monkeypatch):
    async def scenario():
        async with wired_history(monkeypatch) as (provider, _, registry):
            await pin(registry)
            provider.status = 403
            ok, result = await invoke(registry, "list_commits", commit=COMMIT)
            assert not ok and result["error"] == "provider_forbidden"
            provider.status = None
            provider.history_override = {"commits": []}
            ok, result = await invoke(registry, "list_commits", commit=COMMIT)
            assert not ok and result["error"] == "invalid_provider_response"
            provider.history_override = [{"sha": HEAD}]
            ok, result = await invoke(registry, "list_commits", commit=COMMIT)
            assert not ok and "secret" not in json.dumps(result)

    asyncio.run(scenario())


def test_foreign_scope_and_revocation_during_history_read_discard_results(monkeypatch):
    async def scenario():
        async with wired_history(monkeypatch) as (provider, connector, registry):
            await pin(registry)
            before = len(provider.calls)
            from soveren_agent_platform.github import GitHubError

            with pytest.raises(GitHubError):
                await connector.execute(
                    ConversationScope("foreign", "conversation"), "list_commits", {"repository": REPO, "commit": COMMIT}
                )
            assert len(provider.calls) == before

            async def revoke():
                connector.revoke()

            provider.before_result = revoke
            ok, result = await invoke(registry, "list_commits", commit=COMMIT)
            assert not ok and result["error"] == "connector_closed"

    asyncio.run(scenario())


def test_history_only_grant_can_restore_trusted_anchor_without_file_permission():
    async def scenario():
        from soveren_agent_platform.github import GitHubError, GitHubSnapshot

        grant = GitHubRepositoryAccess(REPO, ("test",), operations=frozenset({"resolve_ref", "list_commits"}))
        async with GitHubConnector(credentials=CREDS, access=GitHubAccess(SCOPE, (grant,))) as connector:
            snapshot = GitHubSnapshot(REPO, COMMIT, TREE)
            await connector.restore_snapshot(SCOPE, repository=REPO, ref="test", snapshot=snapshot)
            assert connector.get_ref_snapshot(SCOPE, repository=REPO, ref="test") == snapshot
            with pytest.raises(GitHubError):
                await connector.execute(SCOPE, "read_file", {"repository": REPO, "commit": COMMIT, "path": "src.py"})

    asyncio.run(scenario())


def test_cached_objects_and_proofs_do_not_cross_history_anchors(monkeypatch):
    async def scenario():
        from soveren_agent_platform.github import GitHubSnapshot

        async with wired_history(monkeypatch, refs=("test", "dev")) as (provider, connector, registry):
            await pin(registry)
            await connector.restore_snapshot(
                SCOPE, repository=REPO, ref="dev", snapshot=GitHubSnapshot(REPO, FOREIGN, TREE)
            )
            ok, _ = await invoke(registry, "read_commit", commit=HEAD, anchor_commit=COMMIT)
            assert ok
            for operation, args in (
                ("read_commit", {"commit": HEAD, "anchor_commit": FOREIGN}),
                ("read_file", {"commit": HEAD, "anchor_commit": FOREIGN, "path": "src.py"}),
                ("list_tree", {"commit": HEAD, "anchor_commit": FOREIGN}),
                ("compare_commits", {"base_commit": HEAD, "head_commit": FOREIGN, "anchor_commit": FOREIGN}),
                ("read_pull_request", {"number": 1, "anchor_commit": FOREIGN}),
                ("read_commit", {"commit": FOREIGN, "anchor_commit": COMMIT}),
            ):
                before = len(provider.calls)
                ok, result = await invoke(registry, operation, **args)
                assert not ok and result["error"] == "scope_denied", (operation, result)
                assert len(provider.calls) == before + (2 if operation == "read_pull_request" else 1)
                assert provider.calls[-1][2]["page"] == "2"
            ok, result = await invoke(
                registry,
                "compare_commits",
                base_commit=FOREIGN,
                head_commit=COMMIT,
                base_anchor_commit=FOREIGN,
                anchor_commit=COMMIT,
            )
            assert ok and result["status"] == "diverged" and not result["exact_base_to_head"]
            assert result["base_anchor_commit"] == FOREIGN
            assert connector._ancestry
            connector.revoke()
            assert not connector._ancestry and not connector._snapshots

    asyncio.run(scenario())


def test_history_pagination_returns_distinct_later_commits_without_loss(monkeypatch):
    async def scenario():
        async with wired_history(monkeypatch) as (provider, _, registry):
            await pin(registry)
            provider.history = [commit_data(f"{index:040x}") for index in range(1, 22)]
            ok, first = await invoke(registry, "list_commits", commit=COMMIT)
            assert ok and len(first["commits"]) == 20 and first["next_page"] == 2
            ok, second = await invoke(registry, "list_commits", commit=COMMIT, page=first["next_page"])
            assert ok and len(second["commits"]) == 1 and second["next_page"] is None
            assert [item["commit"] for item in first["commits"] + second["commits"]] == [
                f"{i:040x}" for i in range(1, 22)
            ]
            assert provider.calls[-1][2]["page"] == "2"

    asyncio.run(scenario())
