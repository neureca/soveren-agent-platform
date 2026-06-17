"""Contracts for inbound message batching."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

DecisionKind = Literal["wait", "flush", "force_flush"]


@dataclass(slots=True)
class InboundMessage:
    tenant_id: str
    channel: str
    source_id: str
    raw_event_id: str
    text: str | None
    payload: dict[str, Any]
    message_at: int
    source_event_id: str | None = None


@dataclass(slots=True)
class MessageFeatures:
    is_command: bool = False
    has_question_mark: bool = False
    has_imperative: bool = False
    has_explicit_target: bool = False
    has_continuation_marker: bool = False
    ends_with_continuation_word: bool = False
    ends_with_open_punctuation: bool = False
    is_short_dependent_phrase: bool = False
    gap_s_from_prev: int | None = None


@dataclass(slots=True)
class BatchState:
    batch_id: str
    tenant_id: str
    channel: str
    source_id: str
    messages: list[dict[str, Any]]
    features: list[MessageFeatures]
    now: int
    first_message_at: int
    last_message_at: int
    message_count: int
    quiet_window_s: int
    max_window_s: int
    max_count: int

    @property
    def last(self) -> MessageFeatures:
        return self.features[-1] if self.features else MessageFeatures()

    @property
    def quiet_elapsed(self) -> bool:
        return self.now - self.last_message_at >= self.quiet_window_s

    @property
    def max_window_elapsed(self) -> bool:
        return self.now - self.first_message_at >= self.max_window_s

    @property
    def max_count_reached(self) -> bool:
        return self.message_count >= self.max_count


@dataclass(frozen=True, slots=True)
class BatchRule:
    name: str
    decision: DecisionKind
    weight: int
    reason: str
    predicate: object


@dataclass(slots=True)
class BatchDecision:
    action: Literal["wait", "flush"]
    wait_score: int
    flush_score: int
    matched_rules: list[str] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)


class BatchStore(Protocol):
    async def append_inbound_message(self, message: InboundMessage) -> str | None:
        ...

    async def load_state(
        self,
        batch_id: str,
        *,
        quiet_window_s: int,
        max_window_s: int,
        max_count: int,
    ) -> BatchState | None:
        ...

    async def store_decision(
        self,
        batch_id: str,
        decision: BatchDecision,
        *,
        state: BatchState | None = None,
    ) -> None:
        ...

    async def mark_routed(self, batch_id: str) -> bool:
        ...
