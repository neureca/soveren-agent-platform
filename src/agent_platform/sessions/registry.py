"""Registry for execution session backends."""
from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field

from agent_platform.sessions.backend import SessionBackend


@dataclass(slots=True)
class SessionBackendRegistry:
    """App-facing registry for reusable and custom session backends."""

    backends: dict[str, SessionBackend] = field(default_factory=dict)

    def register(self, name: str, backend: SessionBackend) -> None:
        if not name or not isinstance(name, str):
            raise ValueError("backend name must be a non-empty string")
        if name in self.backends:
            raise ValueError(f"session backend already registered: {name!r}")
        self.backends[name] = backend

    def get(self, name: str) -> SessionBackend | None:
        return self.backends.get(name)

    def require(self, name: str) -> SessionBackend:
        backend = self.get(name)
        if backend is None:
            raise KeyError(f"no session backend registered for {name!r}")
        return backend

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self.backends))

    def as_dict(self) -> dict[str, SessionBackend]:
        return dict(self.backends)

    def __contains__(self, name: object) -> bool:
        return name in self.backends

    def __iter__(self) -> Iterator[str]:
        return iter(self.backends)


SessionBackendMapping = Mapping[str, SessionBackend] | SessionBackendRegistry


def normalize_session_backends(backends: SessionBackendMapping) -> Mapping[str, SessionBackend]:
    if isinstance(backends, SessionBackendRegistry):
        return backends.backends
    return backends

