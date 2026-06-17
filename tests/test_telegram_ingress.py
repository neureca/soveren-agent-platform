import json

from agent_platform.storage.migrations import apply_platform_migrations
from agent_platform.storage.sqlite import open_sqlite
from agent_platform.telegram import TelegramInboundMessage, enqueue_telegram_message


def test_telegram_ingress_enqueues_agent_event(tmp_path):
    conn = open_sqlite(tmp_path / "app.db")
    apply_platform_migrations(conn)

    event_id = enqueue_telegram_message(
        conn,
        TelegramInboundMessage(
            tenant_id="tenant-a",
            chat_id=10,
            update_id=20,
            user_id=30,
            username="ivan",
            text="привет",
            payload={"raw": True},
        ),
    )
    assert event_id is not None

    row = conn.execute("SELECT * FROM event_queue WHERE id = ?", (event_id,)).fetchone()
    payload = json.loads(row["payload_json"])
    assert row["recipient"] == "batching"
    assert row["message_type"] == "InboundMessageReceived"
    assert payload["channel"] == "telegram"
    assert payload["source_id"] == "10"
    assert payload["text"] == "привет"

