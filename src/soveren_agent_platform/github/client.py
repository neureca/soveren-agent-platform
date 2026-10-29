"""Scoped, immutable, bounded GitHub reads without a checkout."""

from __future__ import annotations

import asyncio
import base64
import binascii
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

from soveren_agent_platform.conversation import ConversationScope

from .contracts import (
    GitHubAccess,
    GitHubAppCredentials,
    GitHubError,
    GitHubErrorCode,
    GitHubRepositoryAccess,
    GitHubSnapshot,
)
from .http import GitHubAppHttp


def _object(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise GitHubError(GitHubErrorCode.PROTOCOL)
    return value


def _string(value: Any) -> str:
    if not isinstance(value, str):
        raise GitHubError(GitHubErrorCode.PROTOCOL)
    return value


def _sha(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{40}", value):
        raise GitHubError(GitHubErrorCode.PROTOCOL)
    return value


def _number(value: Any) -> int:
    if type(value) is not int or value < 0:
        raise GitHubError(GitHubErrorCode.PROTOCOL)
    return value


def _bounded(value: int, low: int, high: int) -> None:
    if type(value) is not int or not low <= value <= high:
        raise GitHubError(GitHubErrorCode.INVALID)


def _path(value: str, *, allow_empty: bool = False) -> None:
    if not isinstance(value, str) or len(value) > 1024 or len(value.split("/")) > 32:
        raise GitHubError(GitHubErrorCode.INVALID)
    if value == "" and allow_empty:
        return
    if any(part in ("", ".", "..") for part in value.split("/")) or any(ord(c) < 32 for c in value):
        raise GitHubError(GitHubErrorCode.INVALID)


def _entries(payload: Any, expected_sha: str) -> tuple[list[dict[str, Any]], bool]:
    data = _object(payload)
    if _sha(data.get("sha")) != expected_sha:
        raise GitHubError(GitHubErrorCode.PROTOCOL)
    if not isinstance(data.get("tree"), list) or type(data.get("truncated")) is not bool:
        raise GitHubError(GitHubErrorCode.PROTOCOL)
    return [_object(item) for item in data["tree"]], data["truncated"]


def _citation(repository: str, commit: str, path: str = "") -> dict[str, Any]:
    return {
        "repository": repository,
        "commit": commit,
        "path": path,
        "url": f"https://github.com/{repository}/blob/{commit}/{quote(path, safe='/')}",
    }


def _request_sha(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{40}", value):
        raise GitHubError(GitHubErrorCode.INVALID)
    return value


def _date(value: Any, *, code: GitHubErrorCode = GitHubErrorCode.PROTOCOL) -> datetime:
    try:
        if not isinstance(value, str):
            raise ValueError
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.utcoffset() is None or not 1970 <= result.year <= 2099:
            raise ValueError
        return result.astimezone(UTC)
    except (ValueError, TypeError, OverflowError):
        raise GitHubError(code) from None


def _utc(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _commit_citation(repository: str, commit: str) -> dict[str, Any]:
    return {"repository": repository, "commit": commit, "url": f"https://github.com/{repository}/commit/{commit}"}


def _commit_summary(repository: str, payload: Any) -> dict[str, Any]:
    data = _object(payload)
    commit = _sha(data.get("sha"))
    details = _object(data.get("commit"))
    message = _string(details.get("message")).encode()
    parents = data.get("parents")
    if not isinstance(parents, list) or len(parents) > 32:
        raise GitHubError(GitHubErrorCode.PROTOCOL)
    return {
        "commit": commit,
        "message": message[:2048].decode("utf-8", errors="ignore"),
        "message_truncated": len(message) > 2048,
        "authored_at": _utc(_date(_object(details.get("author")).get("date"))),
        "committed_at": _utc(_date(_object(details.get("committer")).get("date"))),
        "parents": [_sha(_object(parent).get("sha")) for parent in parents],
        "citation": _commit_citation(repository, commit),
    }


def _diff_window(
    repository: str, base: str | None, head: str, files: list[dict[str, Any]], offset: int,
) -> tuple[list[dict[str, Any]], int | None, bool]:
    page: list[dict[str, Any]] = []
    budget, trimmed = 24_000, False
    for item in files[offset : offset + 30]:
        path = _string(item.get("filename"))
        status = _string(item.get("status"))
        old_path = _string(item.get("previous_filename")) if status == "renamed" else path
        patch = item.get("patch")
        if patch is not None and not isinstance(patch, str):
            raise GitHubError(GitHubErrorCode.PROTOCOL)
        cost = len(path.encode()) + len(old_path.encode()) + 500
        if budget < cost:
            if not page:
                raise GitHubError(GitHubErrorCode.TOO_LARGE)
            break
        budget -= cost
        patch_bytes = (patch or "").encode()
        fragment = patch_bytes[:budget].decode("utf-8", errors="ignore")
        cut = len(patch_bytes) > budget
        trimmed |= cut or patch is None
        budget -= len(fragment.encode())
        page.append(
            {
                "path": path,
                "status": status,
                "previous_path": old_path if old_path != path else None,
                "patch": fragment if patch else None,
                "patch_truncated": cut,
                "patch_available": patch is not None,
                "head_citation": _citation(repository, head, path) if status != "removed" else None,
                "base_citation": (_citation(repository, base, old_path)
                                  if status != "added" and base is not None else None),
            }
        )
    next_offset = offset + len(page) if offset + len(page) < len(files) else None
    return page, next_offset, trimmed


@dataclass(frozen=True, slots=True)
class _PullSnapshot:
    number: int
    title: str
    state: str
    base: str
    head: str
    changed_files: int


@dataclass(slots=True)
class _PinRead[T]:
    task: asyncio.Task[T]
    waiters: int = 0


class GitHubConnector:
    """Host-only connector owned by one trusted conversation; revoke/close terminal.

    Refs and PRs pin on first successful resolution for this instance's lifetime.
    Recreate a connector and registry to accept a new selection/authorization.
    """

    def __init__(self, *, credentials: GitHubAppCredentials, access: GitHubAccess) -> None:
        self._access = access
        self._http = GitHubAppHttp(credentials, access)
        self._refs: dict[tuple[str, str], GitHubSnapshot] = {}
        self._snapshots: dict[tuple[str, str], GitHubSnapshot] = {}
        self._pulls: dict[tuple[str, int, str | None, str | None], _PullSnapshot] = {}
        self._ref_reads: dict[tuple[str, str], _PinRead[GitHubSnapshot]] = {}
        self._pull_reads: dict[tuple[str, int, str | None, str | None], _PinRead[_PullSnapshot]] = {}
        self._ancestry: set[tuple[str, str, str]] = set()
        self._inflight = 0

    @property
    def access(self) -> GitHubAccess:
        return self._access

    async def __aenter__(self) -> GitHubConnector:
        self._http.ensure_open()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    def revoke(self) -> None:
        self._http.revoke()
        for read in self._ref_reads.values():
            read.task.cancel()
        for pull_read in self._pull_reads.values():
            pull_read.task.cancel()
        self._refs.clear()
        self._snapshots.clear()
        self._pulls.clear()
        self._ancestry.clear()

    async def restore_snapshot(
        self,
        scope: ConversationScope,
        *,
        repository: str,
        ref: str,
        snapshot: GitHubSnapshot,
    ) -> None:
        """Restore a trusted persisted pin only under its original authorized ref."""
        self._http.ensure_open()
        if scope != self.access.conversation:
            raise GitHubError(GitHubErrorCode.DENIED)
        grant = next((item for item in self.access.repositories if item.repository == repository), None)
        if grant is None or ref not in grant.refs or not ({"list_tree", "read_file", "list_commits", "read_commit",
                                   "compare_commits", "read_pull_request"} & grant.operations):
            raise GitHubError(GitHubErrorCode.DENIED)
        if (
            not isinstance(snapshot, GitHubSnapshot)
            or snapshot.repository != repository
            or re.fullmatch(r"[0-9a-f]{40}", snapshot.commit) is None
            or re.fullmatch(r"[0-9a-f]{40}", snapshot.tree) is None
        ):
            raise GitHubError(GitHubErrorCode.DENIED)
        self._http.ensure_open()
        ref_key = (repository, ref)
        prior = self._refs.get(ref_key)
        if prior is not None and prior != snapshot:
            raise GitHubError(GitHubErrorCode.DENIED)
        commit_key = (repository, snapshot.commit)
        prior_commit = self._snapshots.get(commit_key)
        if prior_commit is not None and prior_commit != snapshot:
            raise GitHubError(GitHubErrorCode.PROTOCOL)
        self._refs.setdefault(ref_key, snapshot)
        self._snapshots.setdefault(commit_key, snapshot)

    def get_ref_snapshot(
        self,
        scope: ConversationScope,
        *,
        repository: str,
        ref: str,
    ) -> GitHubSnapshot | None:
        """Return an already-resolved pin for trusted app persistence, without provider I/O."""
        self._http.ensure_open()
        if scope != self.access.conversation:
            raise GitHubError(GitHubErrorCode.DENIED)
        grant = next((item for item in self.access.repositories if item.repository == repository), None)
        if grant is None or ref not in grant.refs or not ({"list_tree", "read_file", "list_commits", "read_commit",
                                   "compare_commits", "read_pull_request"} & grant.operations):
            raise GitHubError(GitHubErrorCode.DENIED)
        return self._refs.get((repository, ref))

    async def close(self) -> None:
        ref_tasks = [read.task for read in self._ref_reads.values()]
        pull_tasks = [read.task for read in self._pull_reads.values()]
        self.revoke()
        await asyncio.gather(*ref_tasks, *pull_tasks, return_exceptions=True)
        await self._http.close()

    async def _pin[K, T](
        self,
        key: K,
        cache: dict[K, T],
        pending: dict[K, _PinRead[T]],
        fetch: Callable[[], Awaitable[T]],
    ) -> T:
        """Share an initial read by identity; retain only its successful snapshot."""

        async def load() -> T:
            result = await fetch()
            self._http.ensure_open()
            cache[key] = result
            return result

        def finished(task: asyncio.Task[T]) -> None:
            if not task.cancelled():
                task.exception()
            if key in pending and pending[key].task is task:
                del pending[key]

        while True:
            self._http.ensure_open()
            if key in cache:
                return cache[key]
            read = pending.get(key)
            if read is not None and read.task.cancelling():
                await asyncio.gather(read.task, return_exceptions=True)
                continue
            if read is None:
                read = _PinRead(asyncio.create_task(load()))
                pending[key] = read
                read.task.add_done_callback(finished)
            read.waiters += 1
            try:
                return await asyncio.shield(read.task)
            except asyncio.CancelledError:
                self._http.ensure_open()
                raise
            finally:
                read.waiters -= 1
                if read.waiters == 0 and not read.task.done():
                    read.task.cancel()
                    await asyncio.gather(read.task, return_exceptions=True)

    async def execute(
        self,
        scope: ConversationScope,
        operation: str,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """Trusted invocation seam; tools capture scope at registration, never from arguments."""
        repository = arguments.get("repository")
        grant = self._authorize(scope, operation, repository)
        if self._inflight >= 8:
            raise GitHubError(GitHubErrorCode.BUSY)
        self._inflight += 1
        try:
            async with asyncio.timeout(45):
                result = await self._execute(grant, operation, arguments)
                self._http.ensure_open()
                return result
        except TimeoutError:
            raise GitHubError(GitHubErrorCode.TIMEOUT) from None
        except GitHubError:
            raise
        except Exception:
            # The public host API is also a secret-safe exception boundary.
            raise GitHubError(GitHubErrorCode.PROTOCOL) from None
        finally:
            self._inflight -= 1

    def _authorize(self, scope: ConversationScope, operation: str, repository: Any) -> GitHubRepositoryAccess:
        if scope != self.access.conversation:
            raise GitHubError(GitHubErrorCode.DENIED)
        self._http.ensure_open()
        for grant in self.access.repositories:
            if grant.repository == repository and operation in grant.operations:
                return grant
        raise GitHubError(GitHubErrorCode.DENIED)

    async def _execute(self, grant: GitHubRepositoryAccess, operation: str, args: dict[str, Any]) -> dict[str, Any]:
        if operation == "resolve_ref":
            ref = args.get("ref")
            if ref not in grant.refs:
                raise GitHubError(GitHubErrorCode.DENIED)
            snap = await self._pin(
                (grant.repository, ref), self._refs, self._ref_reads, lambda: self._snapshot(grant.repository, ref)
            )
            return {
                "repository": snap.repository,
                "ref": ref,
                "commit": snap.commit,
                "citation": _citation(snap.repository, snap.commit),
            }
        if operation == "list_pull_requests":
            return await self._list_pulls(grant, args)
        if operation == "read_pull_request":
            number = args.get("number")
            if type(number) is not int or number < 1:
                raise GitHubError(GitHubErrorCode.DENIED)
            anchor = None
            base_anchor = None
            if number not in grant.pull_requests:
                if not grant.pull_requests_from_refs:
                    raise GitHubError(GitHubErrorCode.DENIED)
                anchor = _request_sha(args.get("anchor_commit"))
                self._known_snapshot(grant.repository, anchor)
                base_anchor = _request_sha(args.get("base_anchor_commit", anchor))
                self._known_snapshot(grant.repository, base_anchor)
            return await self._pull(grant.repository, number, args.get("offset", 0),
                                    grant=grant if anchor is not None else None,
                                    anchor=anchor, base_anchor=base_anchor)
        if operation == "compare_commits":
            return await self._compare(grant.repository, args)
        if operation == "read_commit":
            return await self._read_commit(grant.repository, args)
        commit = args.get("commit")
        if args.get("anchor_commit") is not None:
            snap = await self._historical_snapshot(grant.repository, commit, args["anchor_commit"])
        else:
            snap = self._known_snapshot(grant.repository, commit)
        if operation == "list_commits":
            return await self._history(snap, args)
        if operation == "list_tree":
            return await self._tree(snap, args.get("prefix", ""), args.get("offset", 0))
        if operation == "read_file":
            return await self._file(snap, args)
        raise GitHubError(GitHubErrorCode.INVALID)

    def _known_snapshot(self, repository: str, commit: Any) -> GitHubSnapshot:
        if not isinstance(commit, str) or (repository, commit) not in self._snapshots:
            raise GitHubError(GitHubErrorCode.DENIED)
        return self._snapshots[(repository, commit)]

    async def _historical_snapshot(self, repository: str, commit: Any, anchor: Any) -> GitHubSnapshot:
        target, anchor = _request_sha(commit), _request_sha(anchor)
        self._known_snapshot(repository, anchor)
        if target == anchor or (repository, target, anchor) in self._ancestry:
            return self._known_snapshot(repository, target)
        # Page2 deliberately omits GitHub's potentially large first-page files array.
        data = _object(await self._http.get(f"/repos/{repository}/compare/{target}...{anchor}",
                                           params={"per_page": "1", "page": "2"}))
        if (data.get("status") not in {"ahead", "identical"}
                or _sha(_object(data.get("merge_base_commit")).get("sha")) != target):
            raise GitHubError(GitHubErrorCode.DENIED)
        base = _object(data.get("base_commit"))
        if _sha(base.get("sha")) != target:
            raise GitHubError(GitHubErrorCode.PROTOCOL)
        snap = GitHubSnapshot(repository, target, _sha(_object(_object(base.get("commit")).get("tree")).get("sha")))
        self._http.ensure_open()
        self._snapshots[(repository, target)] = snap
        self._ancestry.add((repository, target, anchor))
        return snap

    async def _snapshot(self, repository: str, ref: str) -> GitHubSnapshot:
        data = _object(
            await self._http.get(
                f"/repos/{repository}/commits/{quote(ref, safe='')}",
                params={"per_page": "1", "page": "1"},
            )
        )
        snap = GitHubSnapshot(
            repository, _sha(data.get("sha")), _sha(_object(_object(data.get("commit")).get("tree")).get("sha"))
        )
        if re.fullmatch(r"[0-9a-f]{40}", ref) and snap.commit != ref:
            raise GitHubError(GitHubErrorCode.PROTOCOL)
        self._http.ensure_open()
        self._snapshots[(repository, snap.commit)] = snap
        return snap

    async def _tree(self, snap: GitHubSnapshot, prefix: str, offset: int) -> dict[str, Any]:
        _path(prefix, allow_empty=True)
        _bounded(offset, 0, 100_000)
        entries, provider_truncated = _entries(
            await self._http.get(
                f"/repos/{snap.repository}/git/trees/{snap.tree}",
                params={"recursive": "1"},
            ),
            snap.tree,
        )
        selected = []
        for item in entries:
            path = _string(item.get("path"))
            if not prefix or path == prefix or path.startswith(prefix + "/"):
                selected.append({"path": path, "type": _string(item.get("type")), "sha": _sha(item.get("sha"))})
        selected.sort(key=lambda item: item["path"])
        page: list[dict[str, Any]] = []
        budget = 24_000
        for item in selected[offset : offset + 200]:
            cost = len(item["path"].encode()) + 100
            if cost > budget:
                if not page:
                    raise GitHubError(GitHubErrorCode.TOO_LARGE)
                break
            budget -= cost
            page.append(item)
        next_offset = offset + len(page) if offset + len(page) < len(selected) else None
        return {
            "repository": snap.repository,
            "commit": snap.commit,
            "entries": page,
            "provider_truncated": provider_truncated,
            "truncated": provider_truncated or next_offset is not None,
            "next_offset": next_offset,
            "citation": _citation(snap.repository, snap.commit),
        }

    async def _file(self, snap: GitHubSnapshot, args: dict[str, Any]) -> dict[str, Any]:
        path = args.get("path")
        if not isinstance(path, str):
            raise GitHubError(GitHubErrorCode.INVALID)
        _path(path)
        start = args.get("start_line", 1)
        _bounded(start, 1, 1_000_000)
        end, max_bytes = args.get("end_line", start + 199), args.get("max_bytes", 24_000)
        _bounded(end, start, start + 199)
        _bounded(max_bytes, 1, 24_000)
        tree = snap.tree
        components = path.split("/")
        entry: dict[str, Any] = {}
        for index, component in enumerate(components):
            entries, truncated = _entries(await self._http.get(f"/repos/{snap.repository}/git/trees/{tree}"), tree)
            found = next((item for item in entries if item.get("path") == component), None)
            if found is None:
                if truncated:
                    raise GitHubError(GitHubErrorCode.TOO_LARGE)
                raise GitHubError(GitHubErrorCode.PATH_MISSING)
            entry = found
            last = index == len(components) - 1
            mode = entry.get("mode")
            if (not last and mode != "040000") or (last and mode not in ("100644", "100755")):
                raise GitHubError(GitHubErrorCode.NOT_TEXT)
            tree = _sha(entry.get("sha"))
        entry_size = _number(entry.get("size"))
        if entry_size > 256 * 1024:
            raise GitHubError(GitHubErrorCode.TOO_LARGE)
        blob = _object(await self._http.get(f"/repos/{snap.repository}/git/blobs/{tree}"))
        if _sha(blob.get("sha")) != tree:
            raise GitHubError(GitHubErrorCode.PROTOCOL)
        blob_size = _number(blob.get("size"))
        if blob_size > 256 * 1024:
            raise GitHubError(GitHubErrorCode.TOO_LARGE)
        if blob_size != entry_size:
            raise GitHubError(GitHubErrorCode.PROTOCOL)
        if blob.get("encoding") != "base64":
            raise GitHubError(GitHubErrorCode.NOT_TEXT)
        try:
            raw = base64.b64decode("".join(_string(blob.get("content")).split()), validate=True)
        except (ValueError, binascii.Error):
            raise GitHubError(GitHubErrorCode.PROTOCOL) from None
        if len(raw) > 256 * 1024:
            raise GitHubError(GitHubErrorCode.TOO_LARGE)
        if len(raw) != blob_size:
            raise GitHubError(GitHubErrorCode.PROTOCOL)
        try:
            text = raw.decode("utf-8")
            if any(ord(char) < 32 and char not in "\n\r\t" for char in text):
                raise ValueError
        except (UnicodeError, ValueError):
            raise GitHubError(GitHubErrorCode.NOT_TEXT) from None
        lines = text.split("\n")
        lines = [line + "\n" for line in lines[:-1]] + ([lines[-1]] if lines[-1] else [])
        output: list[str] = []
        used = 0
        for line in lines[start - 1 : end]:
            size = len(line.encode())
            if used + size > max_bytes:
                if not output:
                    raise GitHubError(GitHubErrorCode.LINE_TOO_LARGE)
                break
            output.append(line)
            used += size
        last_line = start + len(output) - 1 if output else None
        next_line = start + len(output) if start + len(output) <= len(lines) else None
        citation = _citation(snap.repository, snap.commit, path)
        citation.update({"start_line": start if output else None, "end_line": last_line})
        if output:
            citation["url"] += f"#L{start}-L{last_line}"
        return {
            "text": "".join(output),
            "total_lines": len(lines),
            "citation": citation,
            "truncated": next_line is not None,
            "next_line": next_line,
        }

    async def _pull_snapshot(
        self, repository: str, number: int, grant: GitHubRepositoryAccess | None = None,
        anchor: str | None = None, base_anchor: str | None = None,
    ) -> _PullSnapshot:
        data = _object(await self._http.get(f"/repos/{repository}/pulls/{number}"))
        base = _object(data.get("base"))
        canonical_repository = _string(_object(base.get("repo")).get("full_name"))
        if canonical_repository.casefold() != repository.casefold():
            raise GitHubError(GitHubErrorCode.PROTOCOL)
        pull = _PullSnapshot(
            number,
            _string(data.get("title"))[:1024],
            _string(data.get("state")),
            _sha(base.get("sha")),
            _sha(_object(data.get("head")).get("sha")),
            _number(data.get("changed_files")),
        )
        # Explicit-number grants retain their original contract; ref grants require ancestry.
        if grant is None:
            await self._snapshot(repository, pull.base)
            await self._snapshot(repository, pull.head)
        else:
            if base.get("ref") not in grant.refs or data.get("number") != number:
                raise GitHubError(GitHubErrorCode.DENIED)
            await self._historical_snapshot(repository, pull.base, base_anchor)
            await self._historical_snapshot(repository, pull.head, anchor)
        return pull

    async def _pull(
        self, repository: str, number: int, offset: int, *,
        grant: GitHubRepositoryAccess | None = None, anchor: str | None = None, base_anchor: str | None = None,
    ) -> dict[str, Any]:
        _bounded(offset, 0, 300)
        pull = await self._pin(
            (repository, number, anchor, base_anchor), self._pulls, self._pull_reads,
            lambda: self._pull_snapshot(repository, number, grant, anchor, base_anchor),
        )
        data = _object(
            await self._http.get(
                f"/repos/{repository}/compare/{pull.base}...{pull.head}",
                params={"per_page": "1", "page": "1"},
            )
        )
        merge_base = _sha(_object(data.get("merge_base_commit")).get("sha"))
        if (repository, merge_base) not in self._snapshots:
            await self._snapshot(repository, merge_base)
        if not isinstance(data.get("files"), list):
            raise GitHubError(GitHubErrorCode.PROTOCOL)
        files = [_object(item) for item in data["files"]]
        page, next_offset, trimmed = _diff_window(repository, merge_base, pull.head, files, offset)
        file_list_complete = len(files) < 300 and len(files) == pull.changed_files
        return {
            "repository": repository,
            "number": number,
            "title": pull.title,
            "state": pull.state,
            "base_commit": pull.base,
            "head_commit": pull.head,
            "merge_base_commit": merge_base,
            "diff_kind": "merge_base_to_head",
            "files": page,
            "next_offset": next_offset,
            "file_list_complete": file_list_complete,
            "truncated": not file_list_complete or next_offset is not None or trimmed,
            "patch_completeness": "not_guaranteed_by_provider",
        }

    async def _history(self, snap: GitHubSnapshot, args: dict[str, Any]) -> dict[str, Any]:
        page = args.get("page", 1)
        _bounded(page, 1, 10_000)
        since = _date(args["since"], code=GitHubErrorCode.INVALID) if "since" in args else None
        until = _date(args["until"], code=GitHubErrorCode.INVALID) if "until" in args else None
        if since is not None and until is not None and since >= until:
            raise GitHubError(GitHubErrorCode.INVALID)
        params = {"sha": snap.commit, "per_page": "20", "page": str(page)}
        if since is not None:
            params["since"] = _utc(since)
        if until is not None:
            params["until"] = _utc(until)
        if "path" in args:
            _path(args["path"])
            params["path"] = args["path"]
        data = await self._http.get(f"/repos/{snap.repository}/commits", params=params)
        if not isinstance(data, list) or len(data) > 20:
            raise GitHubError(GitHubErrorCode.PROTOCOL)
        commits = [_commit_summary(snap.repository, item) for item in data]
        selected = [item for item in commits
                    if (since is None or _date(item["committed_at"]) >= since)
                    and (until is None or _date(item["committed_at"]) < until)]
        return {
            "repository": snap.repository, "anchor_commit": snap.commit, "commits": selected,
            "since": _utc(since) if since is not None else None,
            "until": _utc(until) if until is not None else None,
            "interval": "[since,until)", "date_basis": "commit_committer_timestamp_not_branch_arrival_or_deployment",
            "path": args.get("path"), "page": page,
            "next_page": page + 1 if len(data) == 20 else None,
            "truncated": len(data) == 20,
            "citation": _commit_citation(snap.repository, snap.commit),
        }

    async def _read_commit(self, repository: str, args: dict[str, Any]) -> dict[str, Any]:
        offset = args.get("offset", 0)
        _bounded(offset, 0, 2999)
        snap = await self._historical_snapshot(repository, args.get("commit"), args.get("anchor_commit"))
        data = _object(await self._http.get(f"/repos/{repository}/commits/{snap.commit}",
                                          params={"per_page": "30", "page": str(offset // 30 + 1)}))
        summary = _commit_summary(repository, data)
        if summary["commit"] != snap.commit:
            raise GitHubError(GitHubErrorCode.PROTOCOL)
        if _sha(_object(_object(data.get("commit")).get("tree")).get("sha")) != snap.tree:
            raise GitHubError(GitHubErrorCode.PROTOCOL)
        raw = data.get("files")
        if not isinstance(raw, list) or len(raw) > 30:
            raise GitHubError(GitHubErrorCode.PROTOCOL)
        files = [_object(item) for item in raw]
        parents = summary["parents"]
        base = parents[0] if parents else None
        local = offset % 30
        selected, next_local, trimmed = _diff_window(repository, base, snap.commit, files, local)
        next_offset = (offset // 30) * 30 + next_local if next_local is not None else None
        cap = len(raw) == 30 and offset // 30 == 99
        if next_offset is None and len(raw) == 30 and not cap:
            next_offset = (offset // 30 + 1) * 30
        stats = _object(data.get("stats"))
        return {
            "repository": repository, "anchor_commit": args["anchor_commit"], **summary,
            "base_commit": base, "diff_kind": "first_parent_to_commit" if parents else "root_commit",
            "stats": {name: _number(stats.get(name)) for name in ("additions", "deletions", "total")},
            "files": selected, "next_offset": next_offset, "provider_file_limit_reached": cap,
            "file_list_complete": offset == 0 and next_offset is None and not cap,
            "truncated": next_offset is not None or cap or trimmed,
            "patch_completeness": "not_guaranteed_by_provider",
        }

    async def _compare(self, repository: str, args: dict[str, Any]) -> dict[str, Any]:
        offset = args.get("offset", 0)
        _bounded(offset, 0, 300)
        anchor = _request_sha(args.get("anchor_commit"))
        base_anchor = _request_sha(args.get("base_anchor_commit", anchor))
        self._known_snapshot(repository, anchor)
        self._known_snapshot(repository, base_anchor)
        base = await self._historical_snapshot(repository, args.get("base_commit"), base_anchor)
        head = await self._historical_snapshot(repository, args.get("head_commit"), anchor)
        data = _object(await self._http.get(f"/repos/{repository}/compare/{base.commit}...{head.commit}",
                                          params={"per_page": "1", "page": "1"}))
        if _sha(_object(data.get("base_commit")).get("sha")) != base.commit:
            raise GitHubError(GitHubErrorCode.PROTOCOL)
        merge_base = _sha(_object(data.get("merge_base_commit")).get("sha"))
        status = data.get("status")
        if status not in {"ahead", "behind", "identical", "diverged"}:
            raise GitHubError(GitHubErrorCode.PROTOCOL)
        raw = data.get("files")
        if not isinstance(raw, list) or len(raw) > 300:
            raise GitHubError(GitHubErrorCode.PROTOCOL)
        files, next_offset, trimmed = _diff_window(repository, merge_base, head.commit,
                                                  [_object(item) for item in raw], offset)
        return {
            "repository": repository, "anchor_commit": args["anchor_commit"],
            "base_anchor_commit": base_anchor,
            "base_commit": base.commit, "head_commit": head.commit, "merge_base_commit": merge_base,
            "status": status, "diff_kind": "merge_base_to_head",
            "exact_base_to_head": merge_base == base.commit,
            "total_commits": _number(data.get("total_commits")),
            "ahead_by": _number(data.get("ahead_by")), "behind_by": _number(data.get("behind_by")),
            "files": files, "next_offset": next_offset, "file_list_complete": len(raw) < 300,
            "truncated": len(raw) == 300 or next_offset is not None or trimmed,
            "patch_completeness": "not_guaranteed_by_provider",
            "citation": {"repository": repository, "base_commit": base.commit, "head_commit": head.commit,
                         "url": f"https://github.com/{repository}/compare/{base.commit}...{head.commit}"},
        }

    async def _list_pulls(self, grant: GitHubRepositoryAccess, args: dict[str, Any]) -> dict[str, Any]:
        ref = args.get("ref")
        if not grant.pull_requests_from_refs or ref not in grant.refs:
            raise GitHubError(GitHubErrorCode.DENIED)
        page, state = args.get("page", 1), args.get("state", "all")
        _bounded(page, 1, 10_000)
        if not isinstance(state, str) or state not in {"all", "open", "closed"}:
            raise GitHubError(GitHubErrorCode.INVALID)
        data = await self._http.get(f"/repos/{grant.repository}/pulls",
                                   params={"base": ref, "state": state, "sort": "created", "direction": "desc",
                                           "per_page": "20", "page": str(page)})
        if not isinstance(data, list) or len(data) > 20:
            raise GitHubError(GitHubErrorCode.PROTOCOL)
        pulls = []
        for payload in data:
            item = _object(payload)
            base = _object(item.get("base"))
            canonical = _string(_object(base.get("repo")).get("full_name"))
            if base.get("ref") != ref or canonical.casefold() != grant.repository.casefold():
                raise GitHubError(GitHubErrorCode.PROTOCOL)
            number = _number(item.get("number"))
            if number < 1 or item.get("state") not in {"open", "closed"}:
                raise GitHubError(GitHubErrorCode.PROTOCOL)
            pulls.append({
                "number": number, "title": _string(item.get("title"))[:1024], "state": item["state"],
                "base_ref": ref, "head_ref": _string(_object(item.get("head")).get("ref")),
                "created_at": _utc(_date(item["created_at"])) if item.get("created_at") is not None else None,
                "merged_at": _utc(_date(item["merged_at"])) if item.get("merged_at") is not None else None,
                "citation": {"repository": grant.repository, "number": number,
                             "url": f"https://github.com/{grant.repository}/pull/{number}"},
            })
        return {"repository": grant.repository, "ref": ref, "state": state, "pull_requests": pulls,
                "live_discovery": True, "next_page": page + 1 if len(data) == 20 else None,
                "truncated": len(data) == 20}
