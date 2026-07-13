"""Async SQLite adapter for explicit effect reconciliation."""

from __future__ import annotations

from typing import Any

from soveren_agent_platform.reconciliation import store
from soveren_agent_platform.reconciliation.contracts import (
    ActionResolution,
    CronResolution,
    OutboundResolution,
    ReconciliationResult,
)
from soveren_agent_platform.storage.adapter import SQLiteAdapter
from soveren_agent_platform.storage.sqlite import run_sqlite


class SQLiteEffectReconciler(SQLiteAdapter):
    async def resolve_action(
        self,
        action_id: str,
        *,
        tenant_id: str,
        source_id: str,
        resolution: ActionResolution,
        request_key: str,
        actor_id: str,
        evidence: dict[str, Any],
    ) -> ReconciliationResult:
        return await run_sqlite(
            self._conn,
            store.resolve_action,
            action_id,
            tenant_id=tenant_id,
            source_id=source_id,
            resolution=resolution,
            request_key=request_key,
            actor_id=actor_id,
            evidence=evidence,
        )

    async def resolve_outbound(
        self,
        message_id: str,
        *,
        tenant_id: str,
        source_id: str,
        resolution: OutboundResolution,
        request_key: str,
        actor_id: str,
        evidence: dict[str, Any],
        effect_at: int | None = None,
        retry_at: int | None = None,
    ) -> ReconciliationResult:
        return await run_sqlite(
            self._conn,
            store.resolve_outbound,
            message_id,
            tenant_id=tenant_id,
            source_id=source_id,
            resolution=resolution,
            request_key=request_key,
            actor_id=actor_id,
            evidence=evidence,
            effect_at=effect_at,
            retry_at=retry_at,
        )

    async def resolve_cron(
        self,
        job_id: str,
        *,
        tenant_id: str,
        source_id: str,
        resolution: CronResolution,
        request_key: str,
        actor_id: str,
        evidence: dict[str, Any],
        effect_at: int | None = None,
        retry_at: int | None = None,
    ) -> ReconciliationResult:
        return await run_sqlite(
            self._conn,
            store.resolve_cron,
            job_id,
            tenant_id=tenant_id,
            source_id=source_id,
            resolution=resolution,
            request_key=request_key,
            actor_id=actor_id,
            evidence=evidence,
            effect_at=effect_at,
            retry_at=retry_at,
        )
