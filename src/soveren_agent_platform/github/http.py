"""Fixed-origin, bounded GitHub App transport; credentials stay on the host."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import aiohttp
import jwt

from .contracts import GitHubAccess, GitHubAppCredentials, GitHubError, GitHubErrorCode


@dataclass(slots=True)
class _TokenRefresh:
    task: asyncio.Task[str]
    waiters: int = 0


class GitHubAppHttp:
    """One conversation grant, one HTTP client, on-demand shared token refresh.

    Public operations are on GitHubConnector. This internal transport never accepts
    model-supplied URLs. No redirects, env proxy credentials or automatic retries.
    """

    def __init__(
        self,
        credentials: GitHubAppCredentials,
        access: GitHubAccess,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._credentials: GitHubAppCredentials | None = credentials
        self._access = access
        self._clock = clock
        self._token: str | None = None
        self._expiry = 0.0
        self._refresh: _TokenRefresh | None = None
        self._session: aiohttp.ClientSession | None = None
        self._closed = False

    def ensure_open(self) -> None:
        if self._closed:
            raise GitHubError(GitHubErrorCode.CLOSED)

    def revoke(self) -> None:
        """Terminal local revoke. Already forwarded provider requests may finish."""
        self._closed = True
        self._token = None
        self._credentials = None
        if self._refresh is not None:
            self._refresh.task.cancel()

    async def close(self) -> None:
        self.revoke()
        refresh = self._refresh
        if refresh is not None:
            await asyncio.gather(refresh.task, return_exceptions=True)
        if self._session is not None:
            await self._session.close()

    async def get(self, path: str, *, params: dict[str, str] | None = None) -> Any:
        token = await self._get_token()
        try:
            return await self._request("GET", path, token, params=params)
        except GitHubError as exc:
            if exc.code != GitHubErrorCode.AUTH:
                raise
        # One recovery attempt only. Do not invalidate a newer concurrent refresh.
        if self._token == token:
            self._token = None
        token = await self._get_token()
        try:
            return await self._request("GET", path, token, params=params)
        except GitHubError as exc:
            if exc.code == GitHubErrorCode.AUTH:
                self.revoke()
            raise

    async def _get_token(self) -> str:
        while True:
            self.ensure_open()
            if self._token is not None and self._expiry > self._clock() + 120:
                return self._token
            refresh = self._refresh
            if refresh is not None and refresh.task.cancelling():
                # The previous cohort lost its final waiter; join cleanup before
                # a new caller starts a replacement attempt.
                await asyncio.gather(refresh.task, return_exceptions=True)
                continue
            if refresh is None:
                self._token = None  # Failed refresh never falls back to a stale token.
                refresh = _TokenRefresh(asyncio.create_task(self._refresh_token()))
                self._refresh = refresh
                refresh.task.add_done_callback(self._refresh_finished)
            refresh.waiters += 1
            try:
                # Cancellation belongs to this caller, not to its peers.
                return await asyncio.shield(refresh.task)
            except asyncio.CancelledError:
                if self._closed:
                    raise GitHubError(GitHubErrorCode.CLOSED) from None
                raise
            finally:
                refresh.waiters -= 1
                if refresh.waiters == 0 and not refresh.task.done():
                    refresh.task.cancel()
                    await asyncio.gather(refresh.task, return_exceptions=True)

    def _refresh_finished(self, task: asyncio.Task[str]) -> None:
        # Retrieve failures even if every caller was cancelled, without logging
        # provider details or keeping a failed task after its cohort is done.
        if not task.cancelled():
            task.exception()
        if self._refresh is not None and self._refresh.task is task:
            self._refresh = None

    async def _refresh_token(self) -> str:
        self.ensure_open()
        credentials = self._credentials
        if credentials is None:
            raise GitHubError(GitHubErrorCode.CLOSED)
        now = int(self._clock())
        try:
            signed = jwt.encode(
                {"iat": now - 60, "exp": now + 540, "iss": credentials.app_id},
                credentials.private_key,
                algorithm="RS256",
            )
        except Exception:
            raise GitHubError(GitHubErrorCode.AUTH) from None
        permissions = {"contents": "read"}
        if any(
            ("read_pull_request" in repo.operations and (repo.pull_requests or repo.pull_requests_from_refs))
            or ("list_pull_requests" in repo.operations and repo.pull_requests_from_refs)
            for repo in self._access.repositories
        ):
            permissions["pull_requests"] = "read"
        payload = await self._request(
            "POST",
            f"/app/installations/{credentials.installation_id}/access_tokens",
            signed,
            body={
                "repositories": list(
                    dict.fromkeys(repo.repository.split("/")[1] for repo in self._access.repositories)
                ),
                "permissions": permissions,
            },
        )
        try:
            token = payload["token"]
            expiry = datetime.fromisoformat(payload["expires_at"].replace("Z", "+00:00"))
            if not isinstance(token, str) or not token or expiry.utcoffset() is None:
                raise ValueError
            deadline = expiry.timestamp()
            if deadline <= self._clock() + 120:
                raise ValueError
        except (KeyError, TypeError, ValueError, AttributeError):
            raise GitHubError(GitHubErrorCode.PROTOCOL) from None
        self.ensure_open()  # Revocation during the awaited token request wins.
        self._token, self._expiry = token, deadline
        return token

    async def _request(
        self,
        method: str,
        path: str,
        token: str,
        *,
        params: dict[str, str] | None = None,
        body: dict[str, Any] | None = None,
    ) -> Any:
        self.ensure_open()
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=20, connect=5, sock_read=15),
                trust_env=False,
                connector=aiohttp.TCPConnector(limit=8),
            )
        try:
            async with self._session.request(
                method,
                "https://api.github.com" + path,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                    "User-Agent": "soveren-github-readonly",
                },
                params=params,
                json=body,
                allow_redirects=False,
            ) as response:
                status = response.status
                if status == 401:
                    raise GitHubError(GitHubErrorCode.AUTH)
                if status == 429 or (
                    status == 403
                    and (response.headers.get("X-RateLimit-Remaining") == "0" or "Retry-After" in response.headers)
                ):
                    raise GitHubError(GitHubErrorCode.RATE_LIMITED)
                if status == 403:
                    raise GitHubError(GitHubErrorCode.FORBIDDEN)
                if status == 404:
                    raise GitHubError(GitHubErrorCode.NOT_FOUND)
                if status >= 500:
                    raise GitHubError(GitHubErrorCode.UNAVAILABLE)
                if status not in (200, 201):
                    raise GitHubError(GitHubErrorCode.PROTOCOL)
                data = bytearray()
                async for chunk in response.content.iter_chunked(64 * 1024):
                    if len(data) + len(chunk) > 8 * 1024 * 1024:
                        raise GitHubError(GitHubErrorCode.TOO_LARGE)
                    data.extend(chunk)
                self.ensure_open()
                try:
                    return json.loads(data)
                except (ValueError, UnicodeError):
                    raise GitHubError(GitHubErrorCode.PROTOCOL) from None
        except TimeoutError:
            raise GitHubError(GitHubErrorCode.TIMEOUT) from None
        except aiohttp.ClientError:
            raise GitHubError(GitHubErrorCode.UNAVAILABLE) from None
