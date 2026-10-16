"""Best-effort, content-free latency summaries for the bundled SQLite runtime.

Operational rows are authoritative. Measurements and log deduplication live only
in this process; neither an exactly-once log nor recovery of monotonic clocks is
possible after a restart. No additional durable writes are made here.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
from collections import OrderedDict
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Literal

from soveren_agent_platform.json_types import JsonObject
from soveren_agent_platform.storage.adapter import SQLiteAdapter, SQLiteConnectionHandle
from soveren_agent_platform.storage.sqlite import run_sqlite

log = logging.getLogger(__name__)
Phase = Literal["session_open", "codex_turn", "dispatch"]
ScopeKey = tuple[str, str, str]
_PHASES = (
    "telegram_ingress",
    "batching",
    "agent_wait",
    "session_open",
    "codex_turn",
    "dispatch",
    "outbound_wait",
    "telegram_send",
)
_MAX_PARTS = 256


@dataclass(slots=True)
class _Trace:
    started: float | None = None
    phases: dict[str, float] = field(default_factory=dict)
    enqueued: dict[str, float] = field(default_factory=dict)
    send_attempts: int = 0
    wait_attempts: int = 0
    sent_parts: set[str] = field(default_factory=set)
    replayed: bool = False
    partial: bool = False
    codex_turns: dict[str, CodexTurnTiming] = field(default_factory=dict)
    runtime_attempts: list[_RuntimeAttempt] = field(default_factory=list)
    runtime_span_count: int = 0
    runtime_truncated: bool = False


_current: ContextVar[tuple[ScopeKey, _Trace] | None] = ContextVar("turn_latency", default=None)


RuntimeStage = Literal[
    "prepare_turn",
    "prepare_warm",
    "prepare_cold",
    "sandbox_acquire",
    "credentials_refresh",
    "backend_build",
    "backend_rebuild",
    "backend_shutdown",
    "ensure_thread",
    "acquire_orphan_recovery",
    "acquire_conversation_lock_wait",
    "acquire_capacity_reservation",
    "acquire_tenant_lock_wait",
    "acquire_ensure_egress",
    "acquire_egress_lock_wait",
    "acquire_internal_network",
    "acquire_network_subnet",
    "acquire_public_network",
    "acquire_egress_container",
    "acquire_egress_start",
    "acquire_egress_connect",
    "acquire_egress_health",
    "acquire_relay_reconcile",
    "acquire_relay_identity",
    "acquire_relay_resolve",
    "acquire_relay_host_check",
    "acquire_relay_firewall",
    "acquire_relay_probe",
    "acquire_network_policy_inspect",
    "acquire_network_policy_apply",
    "acquire_broker_network_prepare",
    "acquire_broker_remove_unowned",
    "acquire_container_lookup",
    "acquire_reuse_existing",
    "acquire_container_create",
    "siwc_bind",
    "siwc_binding_lock_wait",
    "siwc_auth_file_check",
    "siwc_get_access_token",
    "siwc_grant_validation",
    "siwc_provision_http_credential",
    "siwc_endpoint_validation",
    "siwc_revocation",
    "siwc_host_conversation_lock_wait",
    "siwc_host_tenant_lock_wait",
    "broker_tenant_lock_wait",
    "broker_shared_lock_wait",
    "broker_verified_networks",
    "broker_container",
    "broker_health",
    "broker_network_access",
    "broker_registry_sync",
    "broker_prune_networks",
]
_RUNTIME_STAGES: tuple[RuntimeStage, ...] = (
    "prepare_turn",
    "prepare_warm",
    "prepare_cold",
    "sandbox_acquire",
    "credentials_refresh",
    "backend_build",
    "backend_rebuild",
    "backend_shutdown",
    "ensure_thread",
)


_ACQUIRE_STAGES: tuple[RuntimeStage, ...] = (
    "acquire_orphan_recovery",
    "acquire_conversation_lock_wait",
    "acquire_capacity_reservation",
    "acquire_tenant_lock_wait",
    "acquire_ensure_egress",
    "acquire_egress_lock_wait",
    "acquire_internal_network",
    "acquire_network_subnet",
    "acquire_public_network",
    "acquire_egress_container",
    "acquire_egress_start",
    "acquire_egress_connect",
    "acquire_egress_health",
    "acquire_relay_reconcile",
    "acquire_relay_identity",
    "acquire_relay_resolve",
    "acquire_relay_host_check",
    "acquire_relay_firewall",
    "acquire_relay_probe",
    "acquire_network_policy_inspect",
    "acquire_network_policy_apply",
    "acquire_broker_network_prepare",
    "acquire_broker_remove_unowned",
    "acquire_container_lookup",
    "acquire_reuse_existing",
    "acquire_container_create",
)

_REFRESH_STAGES: tuple[RuntimeStage, ...] = (
    "siwc_bind",
    "siwc_binding_lock_wait",
    "siwc_auth_file_check",
    "siwc_get_access_token",
    "siwc_grant_validation",
    "siwc_provision_http_credential",
    "siwc_endpoint_validation",
    "siwc_revocation",
    "siwc_host_conversation_lock_wait",
    "siwc_host_tenant_lock_wait",
)

_BROKER_STAGES: tuple[RuntimeStage, ...] = (
    "broker_tenant_lock_wait",
    "broker_shared_lock_wait",
    "broker_verified_networks",
    "broker_container",
    "broker_health",
    "broker_network_access",
    "broker_registry_sync",
    "broker_prune_networks",
)


@dataclass(frozen=True, slots=True)
class _RuntimeSpan:
    stage: RuntimeStage
    start: float
    end: float
    failed: bool
    span_id: int
    parents: tuple[int, ...]


@dataclass(slots=True)
class _RuntimeAttempt:
    owner: asyncio.Task[object] | None
    start: float
    end: float | None = None
    failed: bool = False
    spans: list[_RuntimeSpan] = field(default_factory=list)
    native_windows: dict[str, tuple[float, float]] = field(default_factory=dict)
    open_spans: list[int] = field(default_factory=list)
    next_span_id: int = 0


_runtime_attempt: ContextVar[_RuntimeAttempt | None] = ContextVar("codex_runtime_attempt", default=None)


def _task() -> asyncio.Task[object] | None:
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None


@contextmanager
def measure_phase(phase: Phase) -> Iterator[None]:
    """Measure actual work, including failed attempts, in the current worker turn."""
    current = _current.get()
    started = time.monotonic()
    attempt = None
    token = None
    if phase == "codex_turn" and current is not None:
        trace = current[1]
        if len(trace.runtime_attempts) < _MAX_PARTS:
            attempt = _RuntimeAttempt(owner=_task(), start=started)
            trace.runtime_attempts.append(attempt)
        else:
            trace.partial = True
            trace.runtime_truncated = True
        token = _runtime_attempt.set(attempt)
    try:
        yield
    except BaseException:
        if attempt is not None:
            attempt.failed = True
        raise
    finally:
        ended = time.monotonic()
        if attempt is not None:
            attempt.end = ended
            attempt.owner = None
        if token is not None:
            _runtime_attempt.reset(token)
        if current is not None:
            trace = current[1]
            trace.phases[phase] = trace.phases.get(phase, 0.0) + ended - started


@contextmanager
def measure_codex_runtime(stage: RuntimeStage) -> Iterator[None]:
    """Only awaited work in the task owning the current send/capture is included."""
    attempt = _runtime_attempt.get()
    current = _current.get()
    if attempt is None or attempt.owner is not _task() or attempt.end is not None or current is None:
        yield
        return
    trace = current[1]
    if len(attempt.open_spans) >= _MAX_PARTS:
        trace.partial = trace.runtime_truncated = True
        yield
        return
    started = time.monotonic()
    attempt.next_span_id += 1
    span_id = attempt.next_span_id
    parents = tuple(attempt.open_spans)
    attempt.open_spans.append(span_id)
    failed = False
    try:
        yield
    except BaseException:
        failed = True
        raise
    finally:
        attempt.open_spans.pop()
        if trace.runtime_span_count < _MAX_PARTS:
            attempt.spans.append(_RuntimeSpan(stage, started, time.monotonic(), failed, span_id, parents))
            trace.runtime_span_count += 1
        else:
            trace.partial = True
            trace.runtime_truncated = True


@asynccontextmanager
async def measure_codex_lock(lock: asyncio.Lock, stage: RuntimeStage) -> AsyncIterator[None]:
    """Same acquire/release as asyncio.Lock; time only the original acquisition."""
    with measure_codex_runtime(stage):
        await lock.acquire()
    try:
        yield
    finally:
        lock.release()


def _coverage(windows: list[tuple[float, float]], start: float, end: float) -> float:
    """Union clipped windows, so parents and children never double-count coverage."""
    clipped = sorted((max(start, a), min(end, b)) for a, b in windows if b > start and a < end)
    covered = 0.0
    cursor = start
    for a, b in clipped:
        if b > cursor:
            covered += b - max(cursor, a)
            cursor = b
    return covered


def _runtime_metric(values: list[float], failed: int = 0) -> JsonObject:
    return {
        "duration_ms": round(sum(values) * 1000, 3) if values else None,
        "availability": "partial" if values and failed else "measured" if values else "unavailable",
        "samples": len(values),
        "incomplete_samples": failed,
        "reason": "incomplete_observation" if values and failed else None if values else "not_observed_in_codex_turn",
    }


def _detail_group(
    trace: _Trace,
    parent_stage: RuntimeStage,
    stages: tuple[RuntimeStage, ...],
    prefix: str,
    envelopes: frozenset[RuntimeStage] = frozenset(),
) -> JsonObject:
    parents = [
        (attempt, span)
        for attempt in trace.runtime_attempts
        for span in attempt.spans
        if span.stage == parent_stage and attempt.end is not None
    ]
    result: JsonObject = {"parent_samples": len(parents)}
    for stage in stages:
        samples = [
            (span, parent)
            for attempt, parent in parents
            for span in attempt.spans
            if span.stage == stage and parent.span_id in span.parents
        ]
        values = [max(0.0, min(span.end, parent.end) - max(span.start, parent.start)) for span, parent in samples]
        result[stage.removeprefix(prefix)] = _runtime_metric(values, sum(span.failed for span, _ in samples))
    covered = []
    residual = []
    failed_covered = 0
    for attempt, parent in parents:
        children = [
            (span.start, span.end)
            for span in attempt.spans
            if span.stage in stages
            and span.stage != parent_stage
            and span.stage not in envelopes
            and parent.span_id in span.parents
        ]
        coverage = _coverage(children, parent.start, parent.end)
        if children:
            covered.append(coverage)
            failed_covered += parent.failed
        residual.append(max(0.0, parent.end - parent.start - coverage))
    result["accounted"] = _runtime_metric(covered, failed_covered)
    result["unmeasured"] = _runtime_metric(residual, sum(parent.failed for _, parent in parents))
    if trace.runtime_truncated:
        for metric in result.values():
            if isinstance(metric, dict) and metric.get("duration_ms") is not None:
                metric["availability"] = "partial"
                metric["reason"] = "observation_limit"
    return result


def _runtime_details(trace: _Trace) -> JsonObject:
    attempts = [attempt for attempt in trace.runtime_attempts if attempt.end is not None]
    result: JsonObject = {"scope": "phases.codex_turn", "attempts": len(attempts)}
    for stage in _RUNTIME_STAGES:
        spans = [span for attempt in attempts for span in attempt.spans if span.stage == stage]
        result[stage] = _runtime_metric([span.end - span.start for span in spans], sum(span.failed for span in spans))
    accounted: list[float] = []
    unmeasured: list[float] = []
    prepare_unmeasured: list[float] = []
    native: list[float] = []
    accounted_failed = native_failed = prepare_failed = 0
    for attempt in attempts:
        assert attempt.end is not None
        windows = [(span.start, span.end) for span in attempt.spans]
        native_windows = list(attempt.native_windows.values())
        if windows or native_windows:
            accounted.append(_coverage(windows + native_windows, attempt.start, attempt.end))
            accounted_failed += attempt.failed
        unmeasured.append(
            max(0.0, attempt.end - attempt.start - _coverage(windows + native_windows, attempt.start, attempt.end))
        )
        if native_windows:
            native.append(_coverage(native_windows, attempt.start, attempt.end))
            native_failed += attempt.failed
        children = [
            (span.start, span.end)
            for span in attempt.spans
            if span.stage not in {"prepare_turn", "prepare_warm", "prepare_cold"}
        ]
        for parent in (span for span in attempt.spans if span.stage == "prepare_turn"):
            prepare_failed += parent.failed
            prepare_unmeasured.append(
                max(0.0, parent.end - parent.start - _coverage(children, parent.start, parent.end))
            )
    failed = sum(attempt.failed for attempt in attempts)
    result["native_event_window"] = _runtime_metric(native, native_failed)
    result["accounted"] = _runtime_metric(accounted, accounted_failed)
    result["unmeasured"] = _runtime_metric(unmeasured, failed if unmeasured else 0)
    result["prepare_unmeasured"] = _runtime_metric(prepare_unmeasured, prepare_failed)
    result["sandbox_acquire_details"] = _detail_group(
        trace,
        "sandbox_acquire",
        _ACQUIRE_STAGES + _BROKER_STAGES,
        "acquire_",
        frozenset({"acquire_ensure_egress", "acquire_relay_reconcile"}),
    )
    result["credentials_refresh_details"] = _detail_group(
        trace,
        "credentials_refresh",
        _REFRESH_STAGES + _BROKER_STAGES,
        "siwc_",
        frozenset({"siwc_bind"}),
    )
    result["egress_unmeasured"] = _detail_group(
        trace,
        "acquire_ensure_egress",
        _ACQUIRE_STAGES,
        "acquire_",
        frozenset({"acquire_ensure_egress", "acquire_relay_reconcile"}),
    )["unmeasured"]
    result["relay_unmeasured"] = _detail_group(
        trace,
        "acquire_relay_reconcile",
        tuple(
            stage
            for stage in _ACQUIRE_STAGES
            if stage
            in {
                "acquire_relay_identity",
                "acquire_relay_resolve",
                "acquire_relay_host_check",
                "acquire_relay_firewall",
                "acquire_relay_probe",
            }
        ),
        "acquire_",
    )["unmeasured"]
    result["truncated"] = trace.runtime_truncated
    if trace.runtime_truncated:
        for metric in result.values():
            if isinstance(metric, dict) and metric.get("duration_ms") is not None:
                metric["availability"] = "partial"
                metric["reason"] = "observation_limit"
    return result


@dataclass(frozen=True, slots=True)
class CodexTurnTiming:
    """Only in-process app-server event measurements; no provider payload data."""

    start_ack_s: float | None = None
    start_to_first_output_s: float | None = None
    first_output_to_completion_s: float | None = None
    reconnects: int | None = None
    unavailable_reason: str = "events_not_observed"
    request_started_at: float | None = None
    observed_until: float | None = None


def record_codex_turn_timing(key: str, timing: CodexTurnTiming) -> None:
    current = _current.get()
    if current is None:
        return
    trace = current[1]
    if key in trace.codex_turns or len(trace.codex_turns) < _MAX_PARTS:
        # Repeated capture updates this turn; it must not double-count its ACK.
        trace.codex_turns[key] = timing
        attempt = _runtime_attempt.get()
        if (
            attempt is not None
            and attempt.owner is _task()
            and attempt.end is None
            and timing.request_started_at is not None
            and timing.observed_until is not None
        ):
            attempt.native_windows[key] = (timing.request_started_at, timing.observed_until)
    else:
        trace.partial = True
        trace.runtime_truncated = True


def _codex_details(turns: dict[str, CodexTurnTiming]) -> JsonObject:
    result: JsonObject = {"turns_observed": len(turns), "first_output_event": "item/agentMessage/delta_nonempty"}
    for name in ("start_ack", "start_to_first_output", "first_output_to_completion"):
        values = [value for turn in turns.values() if (value := getattr(turn, name + "_s")) is not None]
        reasons = {turn.unavailable_reason for turn in turns.values()}
        result[name] = {
            "duration_ms": round(sum(values) * 1000, 3) if values else None,
            "availability": "measured"
            if values and len(values) == len(turns)
            else "partial"
            if values
            else "unavailable",
            "samples": len(values),
            "reason": None
            if values and len(values) == len(turns)
            else "some_turn_events_unavailable"
            if values
            else next(iter(reasons))
            if len(reasons) == 1
            else "events_not_observed",
        }
    reconnects = [turn.reconnects for turn in turns.values() if turn.reconnects is not None]
    result["reconnects"] = sum(reconnects) if reconnects else None
    result["reconnects_availability"] = (
        "measured" if reconnects and len(reconnects) == len(turns) else "partial" if reconnects else "unavailable"
    )
    return result


def record_outbound_enqueue(
    tenant_id: str,
    source_id: str,
    correlation_id: str | None,
    message_id: str | None,
) -> None:
    current = _current.get()
    if current is None or message_id is None or current[0] != (tenant_id, source_id, correlation_id):
        return
    trace = current[1]
    if len(trace.enqueued) < _MAX_PARTS:
        trace.enqueued.setdefault(message_id, time.monotonic())
    else:
        trace.partial = True


def record_receipt_replay() -> None:
    current = _current.get()
    if current is not None:
        current[1].replayed = True


@dataclass(frozen=True, slots=True)
class _Snapshot:
    status: str
    created_at: int
    updated_at: int
    attempts: int
    batch_id: str | None
    first_message_at: int | None
    batch_created_at: int | None
    inbound_count: int | None
    run_ids: list[str]
    outbound_ids: list[str]
    outbound_states: dict[str, int]
    outbound_attempts: int
    part_count: int
    last_sent_at: int | None


def _snapshot(conn: sqlite3.Connection, key: ScopeKey) -> _Snapshot | None:
    tenant, source, event_id = key
    event = conn.execute(
        "SELECT e.status, e.created_at, e.updated_at, e.attempts, b.id AS batch_id,"
        " b.first_message_at, b.created_at AS batch_created_at, b.message_count"
        " FROM event_queue e LEFT JOIN inbound_batches b ON b.id = json_extract(e.payload_json, '$.batch_id')"
        " AND b.tenant_id = e.tenant_id AND b.source_id = ? WHERE e.tenant_id = ? AND e.id = ?"
        " AND json_extract(e.payload_json, '$.source_id') = ?",
        (source, tenant, event_id, source),
    ).fetchone()
    if event is None:
        return None
    # Read only identifiers/states/timestamps. Never fetch payloads or LLM output.
    runs = conn.execute(
        "SELECT id FROM agent_runs WHERE tenant_id = ? AND source_id = ? AND trigger_event_id = ?"
        " ORDER BY created_at, id LIMIT 256",
        key,
    ).fetchall()
    parts = conn.execute(
        "SELECT status, COUNT(*) AS n, SUM(attempts) AS attempts, MAX(sent_at) AS sent_at"
        " FROM outbound_messages WHERE tenant_id = ? AND source_id = ? AND correlation_id = ?"
        " AND channel = 'telegram' GROUP BY status",
        key,
    ).fetchall()
    ids = conn.execute(
        "SELECT id FROM outbound_messages WHERE tenant_id = ? AND source_id = ? AND correlation_id = ?"
        " AND channel = 'telegram' ORDER BY created_at, id LIMIT 256",
        key,
    ).fetchall()
    return _Snapshot(
        status=event["status"],
        created_at=event["created_at"],
        updated_at=event["updated_at"],
        attempts=event["attempts"],
        batch_id=event["batch_id"],
        first_message_at=event["first_message_at"],
        batch_created_at=event["batch_created_at"],
        inbound_count=event["message_count"],
        run_ids=[row["id"] for row in runs],
        outbound_ids=[row["id"] for row in ids],
        outbound_states={row["status"]: row["n"] for row in parts},
        outbound_attempts=sum(row["attempts"] for row in parts),
        part_count=sum(row["n"] for row in parts),
        last_sent_at=max((row["sent_at"] for row in parts if row["sent_at"] is not None), default=None),
    )


class SQLiteTurnLatencyLogger(SQLiteAdapter):
    """Bounded process-local measurements, summarized against durable state.

    Used by ``create_telegram_agent_app(latency_logging=True)``. All caches,
    including completed-log suppression, retain at most 2048 entries by default.
    Missing/evicted stages remain unavailable. There is no background log scan.
    """

    def __init__(self, handle: SQLiteConnectionHandle, *, cache_limit: int = 2048) -> None:
        super().__init__(handle)
        if isinstance(cache_limit, bool) or cache_limit < 1:
            raise ValueError("cache_limit must be positive")
        self.cache_limit = cache_limit
        self._ingress: OrderedDict[ScopeKey, tuple[float, float]] = OrderedDict()
        self._batches: OrderedDict[ScopeKey, tuple[float, _Trace]] = OrderedDict()
        self._traces: OrderedDict[ScopeKey, _Trace] = OrderedDict()
        self._completed: OrderedDict[ScopeKey, bool] = OrderedDict()

    def _trim(self) -> None:
        for cache in (self._ingress, self._batches, self._traces, self._completed):
            while len(cache) > self.cache_limit:
                cache.popitem(last=False)

    def ingress(
        self,
        tenant_id: str,
        source_id: str,
        raw_event_id: str,
        *,
        started: float,
    ) -> None:
        self._ingress.setdefault((tenant_id, source_id, raw_event_id), (started, time.monotonic()))
        self._trim()

    def batch_ready(
        self,
        tenant_id: str,
        source_id: str,
        batch_id: str,
        raw_event_ids: list[str],
    ) -> None:
        ingress = self._ingress
        stamps = [ingress.pop((tenant_id, source_id, raw), None) for raw in raw_event_ids]
        measured = [stamp for stamp in stamps if stamp is not None]
        trace = _Trace()
        if measured and len(measured) == len(stamps):
            first = min(measured)
            trace.started = first[0]
            trace.phases["telegram_ingress"] = sum(stamp[1] - stamp[0] for stamp in measured)
            trace.phases["batching"] = time.monotonic() - first[1]
        self._batches[(tenant_id, source_id, batch_id)] = (time.monotonic(), trace)
        self._trim()

    @contextmanager
    def agent(self, tenant_id: str, source_id: str, event_id: str, batch_id: str | None) -> Iterator[None]:
        key = (tenant_id, source_id, event_id)
        traces = self._traces
        if key not in traces:
            batch = self._batches.pop((tenant_id, source_id, batch_id), None) if batch_id is not None else None
            trace = batch[1] if batch is not None else _Trace()
            if batch is not None:
                trace.phases["agent_wait"] = time.monotonic() - batch[0]
            traces[key] = trace
        trace = traces[key]
        self._trim()
        token = _current.set((key, trace))
        try:
            yield
        finally:
            _current.reset(token)

    def sending(self, tenant_id: str, source_id: str, event_id: str | None, message_id: str) -> None:
        if event_id is None:
            return
        key = (tenant_id, source_id, event_id)
        trace = self._traces.get(key)
        if trace is not None and message_id in trace.enqueued:
            trace.wait_attempts += 1
            wait = time.monotonic() - trace.enqueued.pop(message_id)
            trace.phases["outbound_wait"] = trace.phases.get("outbound_wait", 0.0) + wait

    async def sent_attempt(
        self,
        tenant_id: str,
        source_id: str,
        event_id: str | None,
        message_id: str,
        *,
        duration_s: float,
    ) -> None:
        if event_id is None:
            return
        key = (tenant_id, source_id, event_id)
        traces = self._traces
        trace = traces.setdefault(key, _Trace())
        self._trim()
        trace.send_attempts += 1
        trace.phases["telegram_send"] = trace.phases.get("telegram_send", 0.0) + duration_s
        if len(trace.sent_parts) < _MAX_PARTS:
            trace.sent_parts.add(message_id)
        else:
            trace.partial = True
        if len(trace.enqueued) < _MAX_PARTS:
            trace.enqueued[message_id] = time.monotonic()
        await self.finish(tenant_id, source_id, event_id)

    async def finish(self, tenant_id: str, source_id: str, event_id: str) -> None:
        """Telemetry failure must never retry an already committed business effect."""
        try:
            await self._finish((tenant_id, source_id, event_id))
        except Exception:
            # Do not include exception messages: SQL/provider errors may contain content.
            log.warning("turn latency summary unavailable")

    async def _finish(self, key: ScopeKey) -> None:
        completed = self._completed
        if key in completed:
            return
        snapshot = await run_sqlite(self._conn, _snapshot, key)
        if snapshot is None or snapshot.status not in {"done", "dead_letter"}:
            return
        states = snapshot.outbound_states
        if snapshot.status == "dead_letter":
            outcome = "agent_failed"
        elif states.get("uncertain"):
            outcome = "uncertain"
        elif states.get("dead_letter") or states.get("cancelled"):
            outcome = "send_failed"
        elif sum(states.values()) != states.get("sent", 0):
            return
        elif not states:
            outcome = "no_reply"
        else:
            outcome = "sent"
        # Recheck after the awaited read: agent/outbound can finish concurrently.
        if key in completed:
            return
        trace = self._traces.pop(key, _Trace())
        completed[key] = True
        self._trim()
        if trace.replayed and not trace.phases:
            return
        phases: JsonObject = {}
        for name in _PHASES:
            value = trace.phases.get(name)
            attempts = trace.send_attempts if name == "telegram_send" else trace.wait_attempts
            partial = name in {"telegram_send", "outbound_wait"} and attempts < snapshot.outbound_attempts
            phases[name] = {
                "duration_ms": round(value * 1000, 3) if value is not None else None,
                "availability": ("partial" if partial else "measured") if value is not None else "unavailable",
                "reason": "not_observed_in_this_process"
                if value is None
                else "earlier_attempts_unavailable"
                if partial
                else None,
            }
        payload: JsonObject = {
            "schema_version": 1,
            "event_id": key[2],
            "batch_id": snapshot.batch_id,
            "inbound_messages": snapshot.inbound_count,
            "run_ids": [value for value in snapshot.run_ids],
            "outbound_ids": [value for value in snapshot.outbound_ids],
            "outcome": outcome,
            "agent_attempts": snapshot.attempts,
            "outbound_attempts": snapshot.outbound_attempts,
            "send_attempts_measured": trace.send_attempts,
            "wait_attempts_measured": trace.wait_attempts,
            "reply_parts": snapshot.part_count,
            "identifier_limit_reached": snapshot.part_count > len(snapshot.outbound_ids)
            or len(snapshot.run_ids) == _MAX_PARTS,
            "reply_states": dict(states),
            "phases": phases,
            "codex_turn_details": {**_codex_details(trace.codex_turns), "runtime": _runtime_details(trace)},
            "total_ms": round((time.monotonic() - trace.started) * 1000, 3) if trace.started is not None else None,
            "first_message_at_unix_s": snapshot.first_message_at,
            "batch_created_at_unix_s": snapshot.batch_created_at,
            "event_created_at_unix_s": snapshot.created_at,
            "event_updated_at_unix_s": snapshot.updated_at,
            "last_sent_at_unix_s": snapshot.last_sent_at,
            "measurement_scope": "observed_attempts_in_this_process",
            "partial": trace.partial
            or trace.send_attempts != snapshot.outbound_attempts
            or trace.wait_attempts != snapshot.outbound_attempts
            or len(trace.sent_parts) != snapshot.part_count
            or any(name not in trace.phases for name in _PHASES),
        }
        log.info("turn_latency %s", json.dumps(payload, separators=(",", ":")), extra={"turn_latency": payload})
