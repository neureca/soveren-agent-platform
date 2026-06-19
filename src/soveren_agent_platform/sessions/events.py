"""Runtime session event audit helpers."""
from __future__ import annotations

import sqlite3
import time
import uuid


def record_session_event(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    direction: str,
    payload_text: str,
    action_id: str | None = None,
    marker: str | None = None,
    now: int | None = None,
) -> str:
    now = now if now is not None else int(time.time())
    event_id = "rse_" + uuid.uuid4().hex
    conn.execute(
        "INSERT INTO runtime_session_events"
        " (id, session_id, action_id, direction, payload_text, marker, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        (event_id, session_id, action_id, direction, payload_text, marker, now),
    )
    return event_id

