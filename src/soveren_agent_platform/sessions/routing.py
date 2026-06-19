"""Backend-neutral session routing contracts."""
from __future__ import annotations

import json
import re
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Protocol

from soveren_agent_platform.sessions import snapshots

_TOKEN_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9_./-]{3,}")
HIGH_CONFIDENCE_SCORE = 25.0


@dataclass(slots=True)
class SessionSnapshot:
    session_id: str
    kind: str
    backend: str
    status: Literal["idle", "busy"]
    title: str | None = None
    cwd: str | None = None
    topic_key: str | None = None
    summary: str | None = None
    keywords: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SessionRouteRequest:
    tenant_id: str
    source_id: str
    text: str
    preferred_kind: str | None = None
    user_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RouteHint:
    action: Literal["route_existing", "open_new", "ask_clarification", "no_match"]
    confidence: float
    session_id: str | None = None
    reasons: list[str] = field(default_factory=list)
    candidates: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class SessionRouteResult:
    snapshots: list[SessionSnapshot]
    hint: RouteHint


class SessionRouter(Protocol):
    async def route(self, request: SessionRouteRequest) -> SessionRouteResult:
        ...


class EmptySessionRouter:
    async def route(self, request: SessionRouteRequest) -> SessionRouteResult:
        return SessionRouteResult(
            snapshots=[],
            hint=RouteHint(action="no_match", confidence=0.0, reasons=["no session router configured"]),
        )


class DeterministicSessionRouter:
    """LLM-free router based on session metadata, snapshots, owner, and recency."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        high_confidence_score: float = HIGH_CONFIDENCE_SCORE,
    ) -> None:
        self.conn = conn
        self.high_confidence_score = high_confidence_score

    async def route(self, request: SessionRouteRequest) -> SessionRouteResult:
        rows = _active_candidates(
            self.conn,
            tenant_id=request.tenant_id,
            source_id=request.source_id,
            kind=request.preferred_kind,
        )
        scored = [
            _score_candidate(self.conn, row, request=request)
            for row in rows
        ]
        scored.sort(key=lambda item: item["score"], reverse=True)
        best = scored[0] if scored else None

        if best is None:
            action: Literal["route_existing", "open_new", "ask_clarification", "no_match"] = "no_match"
            selected_session_id = None
            score = 0.0
            reasons = ["no active session candidates"]
        elif best["score"] >= self.high_confidence_score and best["has_semantic_match"]:
            action = "route_existing"
            selected_session_id = best["session"]["id"]
            score = float(best["score"])
            reasons = list(best["reasons"])
        else:
            action = "ask_clarification"
            selected_session_id = None
            score = float(best["score"])
            reasons = ["top candidate below confidence threshold", *best["reasons"]]

        candidates = [_candidate_payload(item) for item in scored]
        confidence = min(score / 100.0, 1.0)
        _log_route_decision(
            self.conn,
            request=request,
            selected_session_id=selected_session_id,
            action=action,
            confidence=confidence,
            candidates=candidates,
            reasons=reasons,
        )
        return SessionRouteResult(
            snapshots=[_snapshot_from_item(item) for item in scored],
            hint=RouteHint(
                action=action,
                confidence=confidence,
                session_id=selected_session_id,
                reasons=reasons,
                candidates=candidates,
            ),
        )


def _active_candidates(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    source_id: str,
    kind: str | None,
) -> list[sqlite3.Row]:
    params: list[Any] = [tenant_id, source_id]
    kind_filter = ""
    if kind:
        kind_filter = " AND kind = ?"
        params.append(kind)
    return list(conn.execute(
        "SELECT * FROM runtime_sessions"
        " WHERE tenant_id = ? AND source_id = ?"
        "   AND status IN ('idle','busy')"
        f"{kind_filter}"
        " ORDER BY last_used_at DESC",
        params,
    ))


def _score_candidate(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    request: SessionRouteRequest,
) -> dict[str, Any]:
    score = 0.0
    reasons: list[str] = []
    snapshot = snapshots.latest_snapshot(conn, row["id"])
    text_tokens = set(_tokens(request.text))
    snapshot_keywords = set(snapshots.snapshot_keywords(snapshot))

    if row["status"] == "idle":
        score += 4
        reasons.append("session is idle")
    elif row["status"] == "busy":
        score -= 8
        reasons.append("session is busy")

    if request.user_id is not None and row["owner_id"] is not None and str(row["owner_id"]) == str(request.user_id):
        score += 6
        reasons.append("same user")

    recency = _recency_score(row["last_used_at"])
    if recency:
        score += recency
        reasons.append(f"recent session +{recency:g}")

    metadata_hits = sorted(text_tokens & _metadata_terms(row, snapshot))
    has_semantic_match = False
    if metadata_hits:
        has_semantic_match = True
        add = min(45, 15 * len(metadata_hits))
        score += add
        reasons.append(f"metadata match: {', '.join(metadata_hits[:5])}")

    keyword_hits = sorted(text_tokens & snapshot_keywords)
    if keyword_hits:
        has_semantic_match = True
        add = min(40, 8 * len(keyword_hits))
        score += add
        reasons.append(f"keyword match: {', '.join(keyword_hits[:5])}")

    if row["id"].lower() in request.text.lower() or row["backend_session_id"].lower() in request.text.lower():
        has_semantic_match = True
        score += 100
        reasons.append("explicit session id mentioned")

    return {
        "session": row,
        "snapshot": snapshot,
        "score": score,
        "has_semantic_match": has_semantic_match,
        "reasons": reasons or ["recency fallback only"],
    }


def _recency_score(last_used_at: int | None) -> float:
    if not last_used_at:
        return 0.0
    age = max(0, int(time.time()) - int(last_used_at))
    if age <= 300:
        return 15
    if age <= 3600:
        return 10
    if age <= 86400:
        return 4
    return 0.0


def _metadata_terms(row: sqlite3.Row, snapshot: sqlite3.Row | None) -> set[str]:
    values = [
        row["title"] or "",
        row["cwd"] or "",
        Path(row["cwd"] or "").name,
    ]
    try:
        metadata = json.loads(row["metadata_json"])
    except Exception:
        metadata = {}
    if isinstance(metadata, dict):
        values.extend(str(value) for value in metadata.values() if isinstance(value, (str, int, float)))
    if snapshot is not None:
        values.extend([
            snapshot["topic_key"] or "",
            snapshot["branch"] or "",
            snapshot["cwd"] or "",
            Path(snapshot["cwd"] or "").name,
        ])
    return set(_tokens(" ".join(values)))


def _tokens(text: str) -> list[str]:
    return [
        token.strip("./-_").lower()
        for token in _TOKEN_RE.findall(text)
        if len(token.strip("./-_")) >= 3
    ]


def _candidate_payload(item: dict[str, Any]) -> dict[str, Any]:
    row = item["session"]
    snapshot = item["snapshot"]
    return {
        "session_id": row["id"],
        "status": row["status"],
        "kind": row["kind"],
        "backend": row["backend"],
        "title": row["title"],
        "cwd": row["cwd"],
        "topic_key": snapshot["topic_key"] if snapshot is not None else None,
        "score": item["score"],
        "has_semantic_match": item["has_semantic_match"],
        "reasons": item["reasons"],
    }


def _snapshot_from_item(item: dict[str, Any]) -> SessionSnapshot:
    row = item["session"]
    snapshot = item["snapshot"]
    return SessionSnapshot(
        session_id=row["id"],
        kind=row["kind"],
        backend=row["backend"],
        status=row["status"],
        title=row["title"],
        cwd=row["cwd"],
        topic_key=snapshot["topic_key"] if snapshot is not None else None,
        summary=snapshot["summary"] if snapshot is not None else None,
        keywords=snapshots.snapshot_keywords(snapshot),
        metadata={
            "score": item["score"],
            "has_semantic_match": item["has_semantic_match"],
            "reasons": item["reasons"],
        },
    )


def _log_route_decision(
    conn: sqlite3.Connection,
    *,
    request: SessionRouteRequest,
    selected_session_id: str | None,
    action: str,
    confidence: float,
    candidates: list[dict[str, Any]],
    reasons: list[str],
) -> str:
    decision_id = "rsr_" + uuid.uuid4().hex
    conn.execute(
        "INSERT INTO runtime_session_route_decisions"
        " (id, tenant_id, source_id, user_id, preferred_kind, fragment_text,"
        "  selected_session_id, action, confidence, candidates_json,"
        "  reasons_json, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            decision_id,
            request.tenant_id,
            request.source_id,
            request.user_id,
            request.preferred_kind,
            request.text,
            selected_session_id,
            action,
            confidence,
            json.dumps(candidates, ensure_ascii=False),
            json.dumps(reasons, ensure_ascii=False),
            int(time.time()),
        ),
    )
    return decision_id
