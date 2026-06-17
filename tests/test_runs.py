import json

from agent_platform.runs.store import finalize_run, insert_run
from agent_platform.storage.migrations import apply_platform_migrations
from agent_platform.storage.sqlite import open_sqlite


def test_insert_and_finalize_run(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)

    run_id = insert_run(
        conn,
        tenant_id="tenant-a",
        trigger_event_id="evt_1",
        model="test-model",
        prompt_version="v1",
        input_summary="summary",
        now=100,
    )
    finalize_run(
        conn,
        run_id,
        status="completed",
        output={"kind": "reply", "text": "готово"},
        now=101,
    )

    row = conn.execute("SELECT * FROM agent_runs WHERE id = ?", (run_id,)).fetchone()
    assert row["tenant_id"] == "tenant-a"
    assert row["status"] == "completed"
    assert row["updated_at"] == 101
    assert json.loads(row["output_json"]) == {"kind": "reply", "text": "готово"}

