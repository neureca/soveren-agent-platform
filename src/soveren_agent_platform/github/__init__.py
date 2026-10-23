"""Optional host-side GitHub integration. Install the ``github`` extra."""

from .client import GitHubConnector
from .contracts import (
    GitHubAccess,
    GitHubAppCredentials,
    GitHubError,
    GitHubErrorCode,
    GitHubRepositoryAccess,
    GitHubSnapshot,
)
from .tools import register_github_tools

__all__ = [
    "GitHubAccess",
    "GitHubAppCredentials",
    "GitHubConnector",
    "GitHubError",
    "GitHubErrorCode",
    "GitHubRepositoryAccess",
    "GitHubSnapshot",
    "register_github_tools",
]
