"""Searchable sidecar snapshots for runtime session routing."""
from __future__ import annotations

import json
import re
import sqlite3
import time
import uuid
from pathlib import Path

SNAPSHOT_VERSION = 1
MAX_EVENTS = 20
MAX_SUMMARY_CHARS = 1600
MAX_KEYWORDS = 40

_WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9_./-]{3,}")
_FILE_RE = re.compile(
    r"(?:^|\s)([A-Za-z0-9_./-]+\.(?:py|ts|tsx|js|jsx|md|sql|json|toml|yaml|yml|cs|fs|go|rs))"
)
_STOPWORDS = {
    "что", "это", "как", "для", "или", "про", "при", "над", "под", "там",
    "тут", "еще", "ещё", "уже", "если", "чтобы", "надо", "нужно", "можно",
    "the", "and", "for", "with", "from", "this", "that", "into", "then",
}


def refresh_snapshot(conn: sqlite3.Connection, session_id: str, *, now: int | None = None) -> str | None:
    session = conn.execute(
        "SELECT * FROM runtime_sessions WHERE id = ?",
        (session_id,),
    ).fetchone()
    if session is None:
        return None
    events = conn.execute(
        "SELECT id, direction, payload_text, marker, created_at"
        " FROM runtime_session_events"
        " WHERE session_id = ?"
        " ORDER BY created_at DESC, rowid DESC LIMIT ?",
        (session_id, MAX_EVENTS),
    ).fetchall()
    ordered = list(reversed(events))
    combined = "\n".join(
        f"{row['direction']}: {row['payload_text']}"
        for row in ordered
        if row["payload_text"]
    )
    metadata = _metadata(session)
    branch = metadata.get("branch")
    text_for_keywords = " ".join([
        combined,
        session["title"] or "",
        session["cwd"] or "",
        str(metadata.get("topic_key") or ""),
    ])
    keywords = _extract_keywords(text_for_keywords)
    topic_key = str(metadata.get("topic_key") or _topic_key(session, keywords) or "")
    snapshot_id = "rss_" + uuid.uuid4().hex
    now = now if now is not None else int(time.time())
    conn.execute(
        "INSERT INTO runtime_session_context_snapshots"
        " (id, session_id, version, source_event_id, source_range_json,"
        "  summary, keywords_json, entities_json, files_json, cwd, branch,"
        "  topic_key, open_questions_json, last_user_intent, last_agent_state,"
        "  confidence, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            snapshot_id,
            session_id,
            SNAPSHOT_VERSION,
            ordered[-1]["id"] if ordered else None,
            json.dumps({
                "first_event_id": ordered[0]["id"] if ordered else None,
                "last_event_id": ordered[-1]["id"] if ordered else None,
                "event_count": len(ordered),
            }, ensure_ascii=False),
            _make_summary(combined, session=session),
            json.dumps(keywords, ensure_ascii=False),
            json.dumps([], ensure_ascii=False),
            json.dumps(_extract_files(combined), ensure_ascii=False),
            session["cwd"] or "",
            branch,
            topic_key or None,
            json.dumps([], ensure_ascii=False),
            _last_payload(ordered, "input"),
            _last_payload(ordered, "output"),
            0.65 if ordered else 0.35,
            now,
        ),
    )
    return snapshot_id


def latest_snapshot(conn: sqlite3.Connection, session_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM runtime_session_context_snapshots"
        " WHERE session_id = ?"
        " ORDER BY created_at DESC, rowid DESC LIMIT 1",
        (session_id,),
    ).fetchone()


def snapshot_keywords(snapshot: sqlite3.Row | None) -> list[str]:
    if snapshot is None:
        return []
    try:
        parsed = json.loads(snapshot["keywords_json"])
    except Exception:
        return []
    return [str(item) for item in parsed if isinstance(item, str)]


def _metadata(session: sqlite3.Row) -> dict:
    try:
        parsed = json.loads(session["metadata_json"])
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _make_summary(text: str, *, session: sqlite3.Row) -> str:
    prefix = f"{session['kind']} {session['title'] or Path(session['cwd'] or '').name}".strip()
    if not text.strip():
        return prefix
    return f"{prefix}\n{text.strip()[-MAX_SUMMARY_CHARS:]}".strip()


def _extract_keywords(text: str) -> list[str]:
    counts: dict[str, int] = {}
    for raw in _WORD_RE.findall(text.lower()):
        word = raw.strip("./-_")
        if len(word) < 3 or word in _STOPWORDS:
            continue
        counts[word] = counts.get(word, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [word for word, _ in ranked[:MAX_KEYWORDS]]


def _extract_files(text: str) -> list[str]:
    seen: list[str] = []
    for match in _FILE_RE.finditer(text):
        path = match.group(1).strip()
        if path not in seen:
            seen.append(path)
    return seen[:25]


def _topic_key(session: sqlite3.Row, keywords: list[str]) -> str | None:
    title = (session["title"] or "").strip().lower()
    if title:
        return title
    cwd_name = Path(session["cwd"] or "").name.lower()
    if cwd_name:
        return cwd_name
    return keywords[0] if keywords else None


def _last_payload(rows: list[sqlite3.Row], direction: str) -> str | None:
    for row in reversed(rows):
        if row["direction"] == direction and row["payload_text"]:
            return str(row["payload_text"])[-500:]
    return None

