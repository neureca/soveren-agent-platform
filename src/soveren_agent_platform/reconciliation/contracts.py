"""Contracts for explicitly resolving uncertain external effects."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Protocol

ActionResolution = Literal["executed", "failed", "not_executed"]
OutboundResolution = Literal["sent", "failed", "not_sent"]
CronResolution = Literal["fired", "failed", "not_fired"]


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    effect_id: str
    status: str
    applied: bool


class EffectReconciler(Protocol):
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
    ) -> ReconciliationResult: ...

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
    ) -> ReconciliationResult: ...

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
    ) -> ReconciliationResult: ...
