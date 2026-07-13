"""Storage bootstrap helpers for platform consumers."""

from __future__ import annotations

import asyncio
from pathlib import Path

from soveren_agent_platform.storage.migrations import (
    apply_platform_migrations,
    assert_platform_schema,
    baseline_platform_migrations,
)
from soveren_agent_platform.storage.sqlite import open_sqlite


def _bootstrap_platform_storage(db_path: Path) -> list[str]:
    """Apply bundled platform migrations and validate the resulting schema.

    The helper is intentionally limited to platform-owned schema. Application
    migrations must still run in the application repo under their own namespace.
    """
    conn = open_sqlite(db_path)
    try:
        baselined = baseline_platform_migrations(conn)
        applied = baselined or apply_platform_migrations(conn)
        assert_platform_schema(conn)
        return applied
    finally:
        conn.close()


async def bootstrap_platform_storage(db_path: Path) -> list[str]:
    """Apply and validate platform migrations outside the event-loop thread."""
    return await asyncio.to_thread(_bootstrap_platform_storage, db_path)
