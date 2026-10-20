"""Trusted GitHub authorization and safe, model-facing failures."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import StrEnum

from soveren_agent_platform.conversation import ConversationScope


class GitHubErrorCode(StrEnum):
    DENIED = "scope_denied"
    INVALID = "invalid_arguments"
    CLOSED = "connector_closed"
    BUSY = "connector_busy"
    AUTH = "authentication_failed"
    FORBIDDEN = "provider_forbidden"
    NOT_FOUND = "provider_not_found_or_inaccessible"
    RATE_LIMITED = "provider_rate_limited"
    TIMEOUT = "provider_timeout"
    UNAVAILABLE = "provider_unavailable"
    PROTOCOL = "invalid_provider_response"
    TOO_LARGE = "response_too_large"
    PATH_MISSING = "path_not_found"
    NOT_TEXT = "not_a_regular_utf8_text_file"
    LINE_TOO_LARGE = "line_exceeds_byte_budget"


class GitHubError(Exception):
    """Never carries provider bodies, headers, URLs or underlying exceptions."""

    def __init__(self, code: GitHubErrorCode) -> None:
        self.code = code
        super().__init__(code.value)


OPERATIONS = frozenset({"resolve_ref", "list_tree", "read_file", "read_pull_request"})


@dataclass(frozen=True, slots=True)
class GitHubRepositoryAccess:
    repository: str
    refs: tuple[str, ...] = ()
    pull_requests: tuple[int, ...] = ()
    operations: frozenset[str] = OPERATIONS

    def __post_init__(self) -> None:
        if (
            not isinstance(self.repository, str)
            or not re.fullmatch(r"[A-Za-z0-9_-]+/[A-Za-z0-9_.-]+", self.repository)
            or self.repository.split("/")[1] in (".", "..")
        ):
            raise ValueError("invalid GitHub repository")
        if not isinstance(self.refs, tuple) or not isinstance(self.pull_requests, tuple):
            raise ValueError("authorization collections must be immutable")
        if not isinstance(self.operations, frozenset) or not self.operations <= OPERATIONS:
            raise ValueError("invalid GitHub operations")
        if len(self.refs) > 32 or len(self.pull_requests) > 32:
            raise ValueError("at most 32 refs and PRs per repository")
        if any(
            not isinstance(ref, str)
            or not ref.strip()
            or len(ref) > 256
            or ref in (".", "..")
            or any(ord(char) < 32 for char in ref)
            for ref in self.refs
        ):
            raise ValueError("explicit non-empty refs required")
        if any(type(number) is not int or number < 1 for number in self.pull_requests):
            raise ValueError("invalid PR number")


@dataclass(frozen=True, slots=True)
class GitHubAccess:
    conversation: ConversationScope
    repositories: tuple[GitHubRepositoryAccess, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.repositories, tuple) or not 1 <= len(self.repositories) <= 100:
            raise ValueError("one to 100 immutable repository grants required")
        names = [repo.repository.casefold() for repo in self.repositories]
        if len(names) != len(set(names)):
            raise ValueError("duplicate repository grants")


@dataclass(frozen=True, slots=True)
class GitHubAppCredentials:
    app_id: str
    installation_id: int
    private_key: bytes = field(repr=False)

    def __post_init__(self) -> None:
        if not self.app_id or type(self.installation_id) is not int or self.installation_id < 1:
            raise ValueError("GitHub App and installation identity required")
        if not isinstance(self.private_key, bytes) or not self.private_key:
            raise ValueError("private signing key required")


@dataclass(frozen=True, slots=True)
class GitHubSnapshot:
    repository: str
    commit: str
    tree: str
