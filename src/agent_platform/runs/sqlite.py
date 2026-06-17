"""SQLite adapter for agent run persistence."""
from __future__ import annotations

import asyncio
import sqlite3
from typing import Any

import agent_platform.runs.store as run_store


class SQLiteRunStore:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    async def insert(
        self,
        *,
        tenant_id: str,
        trigger_event_id: str,
        model: str,
        prompt_version: str,
        input_summary: str | None,
    ) -> str:
        return await asyncio.to_thread(
            run_store.insert_run,
            self.conn,
            tenant_id=tenant_id,
            trigger_event_id=trigger_event_id,
            model=model,
            prompt_version=prompt_version,
            input_summary=input_summary,
        )

    async def finalize(
        self,
        run_id: str,
        *,
        status: str,
        output: dict[str, Any] | None,
    ) -> None:
        await asyncio.to_thread(
            run_store.finalize_run,
            self.conn,
            run_id,
            status=status,
            output=output,
        )
