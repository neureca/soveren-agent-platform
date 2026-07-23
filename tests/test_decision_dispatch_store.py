from soveren_agent_platform.decisions.receipt_store import (
    accept_decision_dispatch,
    claim_decision_dispatch,
    complete_decision_dispatch,
)
from soveren_agent_platform.storage.migrations import apply_platform_migrations
from soveren_agent_platform.storage.sqlite import open_sqlite


def test_stale_decision_dispatch_owner_cannot_accept_or_complete_after_reclaim(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)
    first = claim_decision_dispatch(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        trigger_event_id="evt-1",
        input_fingerprint="input-fingerprint",
        stale_after_s=10,
        now=100,
    )
    reclaimed = claim_decision_dispatch(
        conn,
        tenant_id="tenant-a",
        source_id="chat-1",
        trigger_event_id="evt-1",
        input_fingerprint="input-fingerprint",
        stale_after_s=10,
        now=111,
    )

    assert first.lease_token is not None
    assert reclaimed.lease_token is not None
    assert reclaimed.acquired is True
    assert reclaimed.lease_token != first.lease_token
    assert (
        accept_decision_dispatch(
            conn,
            first.id,
            lease_token=first.lease_token,
            run_id="run-stale",
            model="model-a",
            prompt_version="v1",
            decision={"kind": "reply", "text": "stale"},
            planner_result={"run_id": "run-stale"},
            dispatch_context={"tenant_id": "tenant-a", "source_id": "chat-1"},
            now=112,
        )
        is False
    )
    assert (
        accept_decision_dispatch(
            conn,
            reclaimed.id,
            lease_token=reclaimed.lease_token,
            run_id="run-winner",
            model="model-b",
            prompt_version="v2",
            decision={"kind": "reply", "text": "winner"},
            planner_result={"run_id": "run-winner"},
            dispatch_context={"tenant_id": "tenant-a", "source_id": "chat-1"},
            now=112,
        )
        is True
    )
    assert (
        complete_decision_dispatch(
            conn,
            reclaimed.id,
            lease_token=first.lease_token,
            dispatch_result={
                "target": "outbound",
                "id": "out-stale",
                "created": True,
                "status": None,
                "metadata": {},
            },
            now=113,
        )
        is False
    )
    assert (
        complete_decision_dispatch(
            conn,
            reclaimed.id,
            lease_token=reclaimed.lease_token,
            dispatch_result={
                "target": "outbound",
                "id": "out-winner",
                "created": True,
                "status": None,
                "metadata": {},
            },
            now=113,
        )
        is True
    )
