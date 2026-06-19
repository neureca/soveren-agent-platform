from soveren_agent_platform.context import (
    ContextFormattingLimits,
    PlannerContext,
    format_planner_context,
)


def test_format_planner_context_renders_compact_platform_context():
    context = PlannerContext(
        trigger={
            "event_id": "evt_1",
            "message_type": "ChatBatchReady",
            "source_id": "chat-1",
            "channel": "telegram",
        },
        batch={
            "batch_id": "batch-1",
            "message_count": 2,
            "text": "first\nsecond",
            "messages": [{"text": "first"}, {"text": "second"}],
        },
        session_routing={
            "route_hint": {
                "action": "route_existing",
                "session_id": "rs_1",
                "confidence": 0.9,
                "reasons": ["metadata match"],
            },
            "sessions": [],
        },
        sessions=[{"session_id": "rs_1", "status": "busy", "mailbox": {"queued": 1}}],
        mailbox=[{"id": "sm_1", "session_id": "rs_1", "status": "queued", "prompt": "continue"}],
        actions=[{"id": "act_1", "kind": "send_cli_prompt", "status": "pending"}],
        outbound=[{"id": "out_1", "status": "queued", "text": "approval"}],
        cron=[{"id": "cron_1", "name": "daily_digest", "status": "pending"}],
    )

    rendered = format_planner_context(context)

    assert "PLATFORM CONTEXT" in rendered
    assert "Trigger:" in rendered
    assert "Inbound batch:" in rendered
    assert "Session routing:" in rendered
    assert "Sessions:" in rendered
    assert "Actions:" in rendered
    assert "daily_digest" in rendered


def test_format_planner_context_honors_limits():
    context = PlannerContext(
        trigger={"event_id": "evt_1", "message_type": "x", "source_id": "chat-1", "channel": "telegram"},
        batch={"batch_id": None, "message_count": 1, "text": "abcdef", "messages": []},
        session_routing={"route_hint": {"action": "no_match", "reasons": []}, "sessions": []},
        actions=[
            {"id": "act_1", "text": "abcdef"},
            {"id": "act_2", "text": "ghijkl"},
        ],
    )

    rendered = format_planner_context(
        context,
        limits=ContextFormattingLimits(max_items_per_section=1, max_text_chars=5),
    )

    assert "ab..." in rendered
    assert "... 1 more" in rendered
