"""Registry for backend-specific session inspectors."""
from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field

from soveren_agent_platform.sessions.contracts import SessionInspector


@dataclass(slots=True)
class SessionInspectorRegistry:
    """App-facing registry for optional session indexer inspectors."""

    inspectors: dict[str, SessionInspector] = field(default_factory=dict)

    def register(self, backend_name: str, inspector: SessionInspector) -> None:
        if not backend_name or not isinstance(backend_name, str):
            raise ValueError("backend name must be a non-empty string")
        if backend_name in self.inspectors:
            raise ValueError(f"session inspector already registered: {backend_name!r}")
        self.inspectors[backend_name] = inspector

    def get(self, backend_name: str) -> SessionInspector | None:
        return self.inspectors.get(backend_name)

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self.inspectors))

    def as_dict(self) -> dict[str, SessionInspector]:
        return dict(self.inspectors)

    def __contains__(self, backend_name: object) -> bool:
        return backend_name in self.inspectors

    def __iter__(self) -> Iterator[str]:
        return iter(self.inspectors)


SessionInspectorMapping = Mapping[str, SessionInspector] | SessionInspectorRegistry


def normalize_session_inspectors(inspectors: SessionInspectorMapping) -> Mapping[str, SessionInspector]:
    if isinstance(inspectors, SessionInspectorRegistry):
        return inspectors.inspectors
    return inspectors
