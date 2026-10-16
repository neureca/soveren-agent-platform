"""Event clocks are tested against a real JSON-RPC stdio process, without Codex/OAuth."""

import asyncio
import logging
import sys
from types import SimpleNamespace

import pytest
from test_telegram_ptb_adapter import FakeUpdate
from test_turn_latency import SECRET, Handler, composition, summaries, until

from soveren_agent_platform.sessions.backend import OpenSpec, SendReceipt
from soveren_agent_platform.sessions.backends.codex_app_server import CodexAppServerBackend

SERVER = r"""
import json, sys, time
mode = sys.argv[1]
def emit(method, params):
    print(json.dumps({'method': method, 'params': params}), flush=True)
def delta(value):
    emit('item/agentMessage/delta', {'threadId': 'thread-1', 'turnId': 'turn-1', 'delta': value})
def completed(status='completed'):
    emit('turn/completed', {'threadId': 'thread-1', 'turn': {'id': 'turn-1', 'status': status}})
for line in sys.stdin:
    message = json.loads(line)
    method = message['method']
    result = {}
    if method == 'initialize':
        result = {'userAgent': 'Codex CLI/0.143.0'}
    elif method == 'thread/start':
        result = {'thread': {'id': 'thread-1'}}
    elif method == 'turn/start':
        if mode == 'ack_timeout':
            continue
        if mode == 'early':
            delta('{"kind":"reply","text":"private content and secret-token"}')
            time.sleep(.01)
        result = {'turn': {'id': 'turn-1'}}
    elif method == 'thread/turns/list':
        result = {'data': [{'id': 'turn-1', 'status': 'completed', 'items': []}], 'nextCursor': None}
    elif method == 'thread/read':
        result = {'thread': {'turns': [{'id': 'turn-1', 'status': 'completed', 'items': []}]}}
    print(json.dumps({'id': message['id'], 'result': result}), flush=True)
    if method != 'turn/start':
        continue
    if mode in ('capture_timeout', 'no_first_timeout'):
        if mode == 'capture_timeout':
            delta('{"kind":"reply","text":"unfinished"}')
        continue
    if mode == 'transport':
        delta('unfinished output')
        sys.exit(0)
    if mode == 'reconnect':
        emit('error', {'threadId': 'thread-1', 'turnId': 'turn-1', 'willRetry': True,
                      'error': {'codexErrorInfo': {'responseStreamDisconnected': {'httpStatusCode': None}}}})
    if mode not in ('early', 'no_output'):
        delta('')
        emit('item/reasoning/textDelta', {'threadId': 'thread-1', 'turnId': 'turn-1', 'delta': 'secret reasoning'})
        time.sleep(.01)
        delta('{"kind":"reply","text":"private content and secret-token"}')
    time.sleep(.01)
    completed('interrupted' if mode == 'failed' else 'completed')
"""


@pytest.mark.parametrize("mode", ["normal", "early", "reconnect", "failed", "transport"])
def test_native_events_reach_one_content_free_telegram_summary(tmp_path, caplog, mode):
    caplog.set_level(logging.INFO)

    async def run():
        script = tmp_path / "app_server.py"
        script.write_text(SERVER)
        handler = Handler(tmp_path / "app.db")
        backend = CodexAppServerBackend(command=[sys.executable, str(script), mode])
        handler.backend = backend
        app, builder = await composition(tmp_path / "app.db", handler)
        try:
            await builder.app.handlers[0].callback(FakeUpdate(text=SECRET), SimpleNamespace())
            await app.start()
            await until(lambda: summaries(caplog), timeout=3)
            assert len(summaries(caplog)) == 1
            result = summaries(caplog)[0]
            assert result["outcome"] == ("agent_failed" if mode in ("failed", "transport") else "sent")
            details = result["codex_turn_details"]
            assert details["turns_observed"] == 1
            for name in ("start_ack", "start_to_first_output", "first_output_to_completion"):
                if mode == "transport" and name == "first_output_to_completion":
                    assert details[name]["availability"] == "unavailable"
                    assert details[name]["duration_ms"] is None
                    continue
                assert details[name]["availability"] == "measured"
                assert details[name]["duration_ms"] >= 0
                assert details[name]["samples"] == 1
            assert details["reconnects"] == (1 if mode == "reconnect" else 0)
            assert details["reconnects_availability"] == "measured"
            if mode == "early":
                assert details["start_to_first_output"]["duration_ms"] < details["start_ack"]["duration_ms"]
            assert SECRET not in caplog.text
            assert "secret reasoning" not in caplog.text
        finally:
            await app.stop()
            await backend.shutdown()
            handler.conn.close()

    asyncio.run(run())


@pytest.mark.parametrize("mode", ["no_output", "capture_timeout", "no_first_timeout", "ack_timeout", "history"])
def test_missing_native_events_remain_unavailable(tmp_path, mode):
    from soveren_agent_platform.runtime.latency import SQLiteTurnLatencyLogger, _codex_details

    async def run():
        script = tmp_path / "app_server.py"
        script.write_text(SERVER)
        handler = Handler(tmp_path / "app.db")
        logger = SQLiteTurnLatencyLogger._from_connection(handler.conn)
        backend = CodexAppServerBackend(
            command=[sys.executable, str(script), mode], request_timeout_s=0.15, turn_timeout_s=0.05
        )
        try:
            with logger.agent("tenant", "source", "event", None):
                opened = await backend.open(OpenSpec(kind="codex_cli", cwd=str(tmp_path)))
                if mode == "history":
                    await backend.capture_delivery(opened.backend_session_id, SendReceipt("turn-1"))
                elif mode == "ack_timeout":
                    with pytest.raises(TimeoutError):
                        await backend.send(opened.backend_session_id, SECRET)
                else:
                    await backend.send(opened.backend_session_id, SECRET)
                    captured = await backend.capture(opened.backend_session_id)
                    assert captured.timed_out is ("timeout" in mode)
            trace = logger._traces[("tenant", "source", "event")]
            details = _codex_details(trace.codex_turns)
            assert details["turns_observed"] == 1
            if mode in ("ack_timeout", "history"):
                assert details["start_ack"]["duration_ms"] is None
            else:
                assert details["start_ack"]["availability"] == "measured"
            assert details["first_output_to_completion"]["duration_ms"] is None
            assert details["first_output_to_completion"]["availability"] == "unavailable"
            if mode != "capture_timeout":
                assert details["start_to_first_output"]["duration_ms"] is None
        finally:
            await backend.shutdown()
            handler.conn.close()

    asyncio.run(run())


def test_native_reader_is_reused_without_leaking_first_turn_context(tmp_path, caplog):
    caplog.set_level(logging.INFO)

    async def run():
        script = tmp_path / "app_server.py"
        script.write_text(SERVER)
        handler = Handler(tmp_path / "app.db")
        backend = CodexAppServerBackend(command=[sys.executable, str(script), "normal"])
        handler.backend = backend
        app, builder = await composition(tmp_path / "app.db", handler)
        try:
            await builder.app.handlers[0].callback(FakeUpdate(update_id=1), SimpleNamespace())
            await app.start()
            await until(lambda: len(summaries(caplog)) == 1, timeout=3)
            await builder.app.handlers[0].callback(FakeUpdate(update_id=2), SimpleNamespace())
            await until(lambda: len(summaries(caplog)) == 2, timeout=3)
            first, second = summaries(caplog)
            assert first["event_id"] != second["event_id"]
            for result in (first, second):
                details = result["codex_turn_details"]
                assert details["turns_observed"] == 1
                assert details["start_to_first_output"]["availability"] == "measured"
                assert details["first_output_to_completion"]["availability"] == "measured"
        finally:
            await app.stop()
            await backend.shutdown()
            handler.conn.close()

    asyncio.run(run())


def test_duplicate_and_late_notifications_do_not_move_event_clocks(monkeypatch):
    from soveren_agent_platform.sessions.backends import codex_app_server as module

    clock = iter([10.0, 20.0])
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=lambda: next(clock)))
    client = module.JsonRpcStdioClient(command=["unused"], cwd=None, env={}, request_timeout_s=1)
    first = {"method": "item/agentMessage/delta", "params": {"threadId": "thread", "turnId": "turn", "delta": "secret"}}
    done = {"method": "turn/completed", "params": {"threadId": "thread", "turn": {"id": "turn", "status": "completed"}}}
    client._handle_notification(first)
    state = client.set_last_turn("thread", "turn")
    client._handle_notification(done)
    client._handle_notification(done)
    client._handle_notification(first)
    assert state.first_output_at == 10.0
    assert state.completed_at == 20.0


def test_capture_cancellation_keeps_ack_without_inventing_completion(tmp_path):
    from soveren_agent_platform.runtime.latency import SQLiteTurnLatencyLogger, _codex_details

    async def run():
        script = tmp_path / "app_server.py"
        script.write_text(SERVER)
        handler = Handler(tmp_path / "app.db")
        logger = SQLiteTurnLatencyLogger._from_connection(handler.conn)
        backend = CodexAppServerBackend(command=[sys.executable, str(script), "no_first_timeout"])
        try:
            with logger.agent("tenant", "source", "event", None):
                opened = await backend.open(OpenSpec(kind="codex_cli", cwd=str(tmp_path)))
                await backend.send(opened.backend_session_id, "request")
                with pytest.raises(TimeoutError):
                    async with asyncio.timeout(0.01):
                        await backend.capture(opened.backend_session_id)
            result = _codex_details(logger._traces[("tenant", "source", "event")].codex_turns)
            assert result["start_ack"]["availability"] == "measured"
            assert result["first_output_to_completion"]["duration_ms"] is None
        finally:
            await backend.shutdown()
            handler.conn.close()

    asyncio.run(run())
