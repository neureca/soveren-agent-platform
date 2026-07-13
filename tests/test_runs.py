import json

from soveren_agent_platform.runs.sqlite import SQLiteRunStore
from soveren_agent_platform.runs.store import claim_run, finalize_run
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


def test_claim_and_finalize_run(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)

    claim = claim_run(
        conn,
        tenant_id="tenant-a",
        trigger_event_id="evt_1",
        model="test-model",
        prompt_version="v1",
        input_summary="summary",
        stale_after_s=60,
        now=100,
    )
    assert claim.acquired
    assert claim.lease_token is not None
    assert finalize_run(
        conn,
        claim.id,
        lease_token=claim.lease_token,
        status="completed",
        output={"kind": "reply", "text": "готово"},
        now=101,
    )

    row = conn.execute("SELECT * FROM agent_runs WHERE id = ?", (claim.id,)).fetchone()
    assert row["tenant_id"] == "tenant-a"
    assert row["status"] == "completed"
    assert row["updated_at"] == 101
    assert json.loads(row["output_json"]) == {"kind": "reply", "text": "готово"}


def test_sqlite_run_store_adapter(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    store = SQLiteRunStore._from_connection(conn)

    import asyncio

    async def run():
        claim = await store.claim(
            tenant_id="tenant-a",
            trigger_event_id="evt_1",
            model="test-model",
            prompt_version="v1",
            input_summary="summary",
            stale_after_s=60,
        )
        assert claim.lease_token is not None
        assert await store.finalize(
            claim.id,
            lease_token=claim.lease_token,
            status="completed",
            output={"ok": True},
        )
        return claim.id

    run_id = asyncio.run(run())

    row = conn.execute("SELECT status, output_json FROM agent_runs WHERE id = ?", (run_id,)).fetchone()
    assert row["status"] == "completed"
    assert json.loads(row["output_json"]) == {"ok": True}


def test_run_claim_is_cached_and_stale_owner_is_fenced(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    first = claim_run(
        conn,
        tenant_id="tenant-a",
        trigger_event_id="evt_1",
        model="test-model",
        prompt_version="v1",
        input_summary="summary",
        stale_after_s=60,
        now=100,
    )
    active = claim_run(
        conn,
        tenant_id="tenant-a",
        trigger_event_id="evt_1",
        model="test-model",
        prompt_version="v1",
        input_summary="summary",
        stale_after_s=60,
        now=120,
    )
    taken_over = claim_run(
        conn,
        tenant_id="tenant-a",
        trigger_event_id="evt_1",
        model="test-model",
        prompt_version="v1",
        input_summary="summary",
        stale_after_s=60,
        now=161,
    )

    assert not active.acquired
    assert taken_over.acquired
    assert first.lease_token is not None
    assert taken_over.lease_token is not None
    assert not finalize_run(
        conn,
        first.id,
        lease_token=first.lease_token,
        status="completed",
        output={"owner": "stale"},
        now=162,
    )
    assert finalize_run(
        conn,
        taken_over.id,
        lease_token=taken_over.lease_token,
        status="completed",
        output={"owner": "current"},
        now=163,
    )
    cached = claim_run(
        conn,
        tenant_id="tenant-a",
        trigger_event_id="evt_1",
        model="test-model",
        prompt_version="v1",
        input_summary="summary",
        stale_after_s=60,
        now=200,
    )

    assert not cached.acquired
    assert cached.output == {"owner": "current"}
