"""Storage bootstrap helpers for platform consumers."""
from __future__ import annotations

from pathlib import Path

from agent_platform.storage.migrations import apply_platform_migrations, assert_platform_schema
from agent_platform.storage.sqlite import open_sqlite


def bootstrap_platform_storage(db_path: Path) -> list[str]:
    """Apply bundled platform migrations and validate the resulting schema.

    The helper is intentionally limited to platform-owned schema. Application
    migrations must still run in the application repo under their own namespace.
    """
    conn = open_sqlite(db_path)
    try:
        applied = apply_platform_migrations(conn)
        assert_platform_schema(conn)
        return applied
    finally:
        conn.close()
