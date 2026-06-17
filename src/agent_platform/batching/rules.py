"""Default inbound batching rule engine."""
from __future__ import annotations

import re
from collections.abc import Callable

from agent_platform.batching.contracts import (
    BatchDecision,
    BatchRule,
    BatchState,
    DecisionKind,
    MessageFeatures,
)

DEFAULT_QUIET_WINDOW_S = 2
DEFAULT_MAX_WINDOW_S = 10
DEFAULT_MAX_COUNT = 20

CONTINUATION_WORDS = {
    "и", "но", "а", "если", "чтобы", "потому что", "так как",
}
CONTINUATION_MARKERS = {
    "сек", "сейчас", "щас", "щас еще", "сейчас еще", "дальше", "еще", "ещё",
}
DEPENDENT_PHRASES = {
    "вот это", "туда", "там", "и еще", "и ещё", "потом это",
}
IMPERATIVE_RE = re.compile(
    r"\b(сделай|проверь|отправь|создай|запиши|добавь|открой|закрой|продолжи|"
    r"почини|обнови|запусти|покажи)\b",
    re.IGNORECASE,
)
TARGET_RE = re.compile(
    r"\b(codex|claude|agent|агент|в эту сессию|в сессию|по [a-zа-я0-9_.-]+)\b",
    re.IGNORECASE,
)


def _rule(
    *,
    name: str,
    decision: DecisionKind,
    weight: int,
    reason: str,
    predicate: Callable[[BatchState], bool],
) -> BatchRule:
    return BatchRule(name, decision, weight, reason, predicate)


RULES: list[BatchRule] = [
    _rule(
        name="immediate_command",
        decision="force_flush",
        weight=100,
        reason="command/admin message",
        predicate=lambda s: s.last.is_command,
    ),
    _rule(
        name="max_window_elapsed",
        decision="force_flush",
        weight=100,
        reason="max batch window elapsed",
        predicate=lambda s: s.max_window_elapsed,
    ),
    _rule(
        name="max_count_reached",
        decision="force_flush",
        weight=100,
        reason="max message count reached",
        predicate=lambda s: s.max_count_reached,
    ),
    _rule(
        name="continuation_marker",
        decision="wait",
        weight=35,
        reason="last message says more is coming",
        predicate=lambda s: s.last.has_continuation_marker,
    ),
    _rule(
        name="continuation_word",
        decision="wait",
        weight=25,
        reason="last message ends with a continuation word",
        predicate=lambda s: s.last.ends_with_continuation_word,
    ),
    _rule(
        name="open_punctuation",
        decision="wait",
        weight=20,
        reason="last message ends with open punctuation",
        predicate=lambda s: s.last.ends_with_open_punctuation,
    ),
    _rule(
        name="short_dependent_phrase",
        decision="wait",
        weight=20,
        reason="last message is a dependent phrase",
        predicate=lambda s: s.last.is_short_dependent_phrase,
    ),
    _rule(
        name="rapid_same_source",
        decision="wait",
        weight=10,
        reason="messages are arriving rapidly",
        predicate=lambda s: s.last.gap_s_from_prev is not None and s.last.gap_s_from_prev <= 1,
    ),
    _rule(
        name="quiet_window_elapsed",
        decision="flush",
        weight=45,
        reason="quiet window elapsed",
        predicate=lambda s: s.quiet_elapsed,
    ),
    _rule(
        name="question",
        decision="flush",
        weight=15,
        reason="last message looks like a complete question",
        predicate=lambda s: s.last.has_question_mark,
    ),
    _rule(
        name="imperative",
        decision="flush",
        weight=25,
        reason="last message contains an imperative request",
        predicate=lambda s: s.last.has_imperative,
    ),
    _rule(
        name="explicit_target",
        decision="flush",
        weight=20,
        reason="last message has an explicit target/session/project",
        predicate=lambda s: s.last.has_explicit_target,
    ),
]


def decide_batch(state: BatchState | None) -> BatchDecision:
    if state is None or not state.messages:
        return BatchDecision("wait", wait_score=0, flush_score=0)
    matched = [rule for rule in RULES if rule.predicate(state)]  # type: ignore[operator]
    if any(rule.decision == "force_flush" for rule in matched):
        return BatchDecision(
            "flush",
            wait_score=sum(rule.weight for rule in matched if rule.decision == "wait"),
            flush_score=sum(rule.weight for rule in matched if rule.decision in ("flush", "force_flush")),
            matched_rules=[rule.name for rule in matched],
            reasons=[rule.reason for rule in matched],
        )
    wait_score = sum(rule.weight for rule in matched if rule.decision == "wait")
    flush_score = sum(rule.weight for rule in matched if rule.decision == "flush")
    action = "flush" if flush_score >= wait_score and state.quiet_elapsed else "wait"
    return BatchDecision(
        action,
        wait_score=wait_score,
        flush_score=flush_score,
        matched_rules=[rule.name for rule in matched],
        reasons=[rule.reason for rule in matched],
    )


def extract_features(message: dict, *, prev: dict | None = None) -> MessageFeatures:
    text = str(message.get("text") or "").strip()
    lower = text.lower()
    gap = None
    if prev and message.get("message_at") is not None and prev.get("message_at") is not None:
        gap = max(0, int(message["message_at"]) - int(prev["message_at"]))
    return MessageFeatures(
        is_command=text.startswith("/"),
        has_question_mark="?" in text or "？" in text,
        has_imperative=bool(IMPERATIVE_RE.search(lower)),
        has_explicit_target=bool(TARGET_RE.search(lower)),
        has_continuation_marker=any(marker in lower for marker in CONTINUATION_MARKERS),
        ends_with_continuation_word=_ends_with_any(lower, CONTINUATION_WORDS),
        ends_with_open_punctuation=lower.endswith(("-", ":", "—")),
        is_short_dependent_phrase=lower in DEPENDENT_PHRASES or (len(lower) <= 12 and lower in CONTINUATION_MARKERS),
        gap_s_from_prev=gap,
    )


def _ends_with_any(text: str, words: set[str]) -> bool:
    stripped = text.rstrip(" .,!?)»\"'")
    return any(stripped.endswith(f" {word}") or stripped == word for word in words)

