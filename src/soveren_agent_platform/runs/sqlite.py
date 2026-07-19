"""SQLite adapter for agent run persistence."""

from __future__ import annotations

from typing import Any

import soveren_agent_platform.runs.store as run_store
from soveren_agent_platform.runs.contracts import PlannerRunClaim
from soveren_agent_platform.storage.adapter import SQLiteAdapter
from soveren_agent_platform.storage.sqlite import run_sqlite


class SQLiteRunStore(SQLiteAdapter):
    async def claim(
        self,
        *,
        tenant_id: str,
        source_id: str,
        trigger_event_id: str,
        model: str,
        prompt_version: str,
        input_summary: str | None,
        input_fingerprint: str,
        stale_after_s: int,
    ) -> PlannerRunClaim:
        return await run_sqlite(
            self._conn,
            run_store.claim_run,
            tenant_id=tenant_id,
            source_id=source_id,
            trigger_event_id=trigger_event_id,
            model=model,
            prompt_version=prompt_version,
            input_summary=input_summary,
            input_fingerprint=input_fingerprint,
            stale_after_s=stale_after_s,
        )

    async def finalize(
        self,
        run_id: str,
        *,
        lease_token: str,
        status: str,
        output: dict[str, Any] | None,
    ) -> bool:
        return await run_sqlite(
            self._conn,
            run_store.finalize_run,
            run_id,
            lease_token=lease_token,
            status=status,
            output=output,
        )
