"""Forward-only SQL migrations with platform/app namespaces."""
from __future__ import annotations

import sqlite3
import time
from importlib.resources import as_file, files
from pathlib import Path


def _ensure_meta(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        "  namespace TEXT NOT NULL,"
        "  version TEXT NOT NULL,"
        "  applied_at INTEGER NOT NULL,"
        "  PRIMARY KEY(namespace, version)"
        ")"
    )


def _applied(conn: sqlite3.Connection, namespace: str) -> set[str]:
    rows = conn.execute(
        "SELECT version FROM schema_migrations WHERE namespace = ?",
        (namespace,),
    ).fetchall()
    return {r["version"] for r in rows}


def apply_migrations_from_dir(
    conn: sqlite3.Connection,
    migration_dir: Path,
    *,
    namespace: str,
) -> list[str]:
    """Apply pending `*.sql` files in lexicographic order for one namespace."""
    _ensure_meta(conn)
    applied = _applied(conn, namespace)
    fresh: list[str] = []
    for path in sorted(migration_dir.glob("*.sql")):
        version = path.stem
        if version in applied:
            continue
        body = path.read_text()
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.executescript(body)
            conn.execute(
                "INSERT INTO schema_migrations(namespace, version, applied_at)"
                " VALUES (?, ?, ?)",
                (namespace, version, int(time.time())),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        fresh.append(version)
    return fresh


def apply_platform_migrations(conn: sqlite3.Connection) -> list[str]:
    """Apply bundled platform migrations."""
    migration_root = files("agent_platform.storage.migrations").joinpath("platform")
    with as_file(migration_root) as path:
        return apply_migrations_from_dir(conn, path, namespace="platform")

