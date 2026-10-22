"""SQLite operations for durable, scoped dialogue tasks and routing plans."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from dataclasses import asdict
from typing import Any

from soveren_agent_platform.conversation import ConversationScope
from soveren_agent_platform.dialogue.contracts import (
    ConfirmedDialogueReference,
    DialogueAssignment,
    DialogueCandidate,
    DialogueConflictError,
    DialogueInput,
    DialogueInputReference,
    DialogueMessage,
    DialoguePage,
    DialoguePlan,
    DialoguePlanResult,
    DialogueQuestion,
    DialogueQuestionCandidate,
    DialogueQuestionPage,
    DialogueRoute,
    DialogueTask,
    DialogueTaskContext,
)
from soveren_agent_platform.idempotency import idempotency_fingerprint

MAX_PAGE_SIZE = 100
MAX_QUERY_CHARS = 256
MAX_TITLE_CHARS = 256
MAX_INPUT_TEXT_CHARS = 16_000
MAX_QUESTION_TEXT_CHARS = 4_000
MAX_QUESTION_CANDIDATES = 50


def _now() -> int:
    return int(time.time())


def _scope(scope: ConversationScope) -> tuple[str, str]:
    return scope.tenant_id, scope.source_id


def _validate_page(limit: int, offset: int = 0) -> None:
    if type(limit) is not int or not 1 <= limit <= MAX_PAGE_SIZE:
        raise ValueError(f"limit must be between 1 and {MAX_PAGE_SIZE}")
    if type(offset) is not int or not 0 <= offset <= 100_000:
        raise ValueError("offset must be between 0 and 100000")


def _plan_data(plan: DialoguePlan) -> dict[str, Any]:
    return {
        "plan_id": plan.plan_id,
        "scope": asdict(plan.scope),
        "inputs": [asdict(item) for item in plan.inputs],
        "routes": [asdict(item) for item in plan.routes],
    }


def apply_dialogue_plan(conn: sqlite3.Connection, plan: DialoguePlan, *, now: int | None = None) -> DialoguePlanResult:
    """Apply all input assignments and lifecycle changes as one immutable command."""
    _validate_plan(plan)
    now = _now() if now is None else now
    tenant_id, source_id = _scope(plan.scope)
    fingerprint = idempotency_fingerprint(_plan_data(plan))
    route_by_input = {route.input_id: route for route in plan.routes}
    local_tasks: dict[str, str] = {}
    assignments: dict[str, tuple[str | None, str | None]] = {}
    conn.execute("BEGIN IMMEDIATE")
    try:
        previous = conn.execute(
            "SELECT fingerprint, result_json FROM dialogue_plans WHERE tenant_id = ? AND source_id = ? AND plan_id = ?",
            (tenant_id, source_id, plan.plan_id),
        ).fetchone()
        if previous is not None:
            if previous["fingerprint"] != fingerprint:
                raise DialogueConflictError("dialogue plan id was reused with changed input")
            result = _plan_result(json.loads(previous["result_json"]))
            conn.execute("COMMIT")
            return result

        for item in plan.inputs:
            existing = conn.execute(
                "SELECT 1 FROM dialogue_inputs WHERE tenant_id = ? AND source_id = ? AND input_id = ?",
                (tenant_id, source_id, item.input_id),
            ).fetchone()
            if existing is not None:
                raise DialogueConflictError("dialogue input was already assigned by another plan")

        for item in plan.inputs:
            route = route_by_input[item.input_id]
            task_id: str | None = None
            question_id: str | None = None

            if route.action == "create":
                assert route.task_key is not None and route.title is not None
                if route.task_key in local_tasks:
                    raise DialogueConflictError("local task key was created more than once")
                task_id = _new_task_id()
                local_tasks[route.task_key] = task_id
                conn.execute(
                    "INSERT INTO dialogue_tasks"
                    " (id, tenant_id, source_id, title, original_request, status, revision, created_at, updated_at)"
                    " VALUES (?, ?, ?, ?, ?, 'active', 1, ?, ?)",
                    (task_id, tenant_id, source_id, route.title, item.text, now, now),
                )
                _insert_input(conn, plan.scope, item, task_id=task_id, question_id=None, plan_id=plan.plan_id, now=now)
                _insert_input_message(conn, plan.scope, item, task_id=task_id)
                _set_focus(conn, tenant_id, source_id, item.actor_id, task_id, now)

            elif route.action == "attach":
                task_id = _resolve_task_target(route, local_tasks)
                _attach_input(conn, plan.scope, item, task_id, plan.plan_id, now=now)
                _set_focus(conn, tenant_id, source_id, item.actor_id, task_id, now)

            elif route.action == "question":
                assert route.question_id is not None and route.question_text is not None
                question_id = route.question_id
                _create_or_extend_question(
                    conn,
                    scope=plan.scope,
                    item=item,
                    route=route,
                    plan_id=plan.plan_id,
                    now=now,
                )
                _insert_input(
                    conn, plan.scope, item, task_id=None, question_id=question_id, plan_id=plan.plan_id, now=now
                )

            elif route.action == "resolve_question":
                assert route.question_id is not None
                assert route.question_revision is not None
                question_id = route.question_id
                if route.task_key is not None and route.task_key in local_tasks:
                    raise DialogueConflictError("local task key was created more than once")
                task_id = _resolve_question(
                    conn,
                    scope=plan.scope,
                    item=item,
                    route=route,
                    plan_id=plan.plan_id,
                    now=now,
                )
                if route.task_key is not None:
                    local_tasks[route.task_key] = task_id
                _set_focus(conn, tenant_id, source_id, item.actor_id, task_id, now)

            else:
                task_id = _resolve_task_target(route, local_tasks)
                task = _get_task_row(conn, plan.scope, task_id)
                if task is None:
                    raise DialogueConflictError("dialogue task is unavailable in this scope")
                if task["status"] != "active":
                    raise DialogueConflictError("terminal dialogue tasks cannot be changed")
                _attach_input(conn, plan.scope, item, task_id, plan.plan_id, now=now)
                if route.action == "close":
                    conn.execute(
                        "UPDATE dialogue_tasks SET status = 'closed', revision = revision + 1,"
                        " terminal_at = ?, updated_at = ?"
                        " WHERE tenant_id = ? AND source_id = ? AND id = ? AND status = 'active'",
                        (now, now, tenant_id, source_id, task_id),
                    )
                    _suppress_task_outbound(
                        conn,
                        tenant_id,
                        source_id,
                        task_id,
                        reason="dialogue task closed before send admission",
                        now=now,
                    )
                else:
                    conn.execute(
                        "UPDATE dialogue_tasks SET status = 'cancelled', revision = revision + 1,"
                        " terminal_at = ?, updated_at = ?"
                        " WHERE tenant_id = ? AND source_id = ? AND id = ? AND status = 'active'",
                        (now, now, tenant_id, source_id, task_id),
                    )
                    _suppress_task_outbound(
                        conn,
                        tenant_id,
                        source_id,
                        task_id,
                        reason="dialogue task cancelled before send admission",
                        now=now,
                    )

            assignments[item.input_id] = (task_id, question_id)

        finalized = []
        for item in plan.inputs:
            task_id, question_id = assignments[item.input_id]
            revision = None
            if task_id is not None:
                row = _get_task_row(conn, plan.scope, task_id)
                if row is None:
                    raise RuntimeError("dialogue task disappeared while committing its route plan")
                revision = int(row["revision"])
            finalized.append(DialogueAssignment(item.input_id, task_id, question_id, revision))
        result = DialoguePlanResult(plan.plan_id, tuple(finalized))
        conn.execute(
            "INSERT INTO dialogue_plans"
            " (tenant_id, source_id, plan_id, fingerprint, plan_json, result_json, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                tenant_id,
                source_id,
                plan.plan_id,
                fingerprint,
                json.dumps(_plan_data(plan), ensure_ascii=False),
                json.dumps(asdict(result)),
                now,
            ),
        )
        conn.execute("COMMIT")
        return result
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def get_task(conn: sqlite3.Connection, scope: ConversationScope, task_id: str) -> DialogueTask | None:
    row = _get_task_row(conn, scope, task_id)
    return None if row is None else _task(row)


def get_plan(conn: sqlite3.Connection, scope: ConversationScope, plan_id: str) -> DialoguePlan | None:
    row = conn.execute(
        "SELECT plan_json FROM dialogue_plans WHERE tenant_id = ? AND source_id = ? AND plan_id = ?",
        (*_scope(scope), plan_id),
    ).fetchone()
    return None if row is None else _dialogue_plan(json.loads(row["plan_json"]))


def get_task_context(
    conn: sqlite3.Connection,
    scope: ConversationScope,
    task_id: str,
    *,
    limit: int = 50,
    before_message_id: str | None = None,
    through_input_id: str | None = None,
) -> DialogueTaskContext | None:
    if type(limit) is not int or not 1 <= limit <= MAX_PAGE_SIZE:
        raise ValueError(f"limit must be between 1 and {MAX_PAGE_SIZE}")
    task_row = _get_task_row(conn, scope, task_id)
    if task_row is None:
        return None
    tenant_id, source_id = _scope(scope)
    through_sequence: int | None = None
    if through_input_id is not None:
        boundary = conn.execute(
            "SELECT message.sequence FROM dialogue_inputs AS input"
            " JOIN dialogue_messages AS message"
            " ON message.tenant_id = input.tenant_id AND message.source_id = input.source_id"
            " AND message.input_id = input.input_id"
            " WHERE input.tenant_id = ? AND input.source_id = ? AND input.input_id = ?"
            " AND input.task_id = ?",
            (tenant_id, source_id, through_input_id, task_id),
        ).fetchone()
        if boundary is None:
            return None
        through_sequence = int(boundary["sequence"])
    before_sequence: int | None = None
    if before_message_id is not None:
        boundary = conn.execute(
            "SELECT sequence FROM dialogue_messages"
            " WHERE tenant_id = ? AND source_id = ? AND task_id = ? AND message_id = ?",
            (tenant_id, source_id, task_id, before_message_id),
        ).fetchone()
        if boundary is None:
            return None
        before_sequence = int(boundary["sequence"])
    before_sql = " AND sequence < ?" if before_sequence is not None else ""
    through_sql = " AND (role = 'assistant' OR sequence <= ?)" if through_sequence is not None else ""
    parameters: tuple[Any, ...] = (tenant_id, source_id, task_id)
    if before_sequence is not None:
        parameters += (before_sequence,)
    if through_sequence is not None:
        parameters += (through_sequence,)
    parameters += (limit + 1,)
    rows = conn.execute(
        "SELECT * FROM dialogue_messages WHERE tenant_id = ? AND source_id = ? AND task_id = ?"
        + before_sql
        + through_sql
        + " ORDER BY sequence DESC LIMIT ?",
        parameters,
    ).fetchall()
    has_older = len(rows) > limit
    page_rows = rows[:limit]
    page_rows.reverse()
    messages = tuple(_message(row) for row in page_rows)
    next_before = messages[0].message_id if has_older and messages else None
    return DialogueTaskContext(_task(task_row), messages, has_older, next_before)


def list_candidates(
    conn: sqlite3.Connection,
    scope: ConversationScope,
    *,
    limit: int = 20,
    offset: int = 0,
) -> DialoguePage:
    _validate_page(limit, offset)
    tenant_id, source_id = _scope(scope)
    rows = conn.execute(
        "SELECT * FROM dialogue_tasks WHERE tenant_id = ? AND source_id = ? AND status = 'active'"
        " ORDER BY updated_at DESC, id ASC LIMIT ? OFFSET ?",
        (tenant_id, source_id, limit + 1, offset),
    ).fetchall()
    return DialoguePage(tuple(_candidate(row) for row in rows[:limit]), offset, len(rows) > limit)


def search_candidates(
    conn: sqlite3.Connection,
    scope: ConversationScope,
    query: str,
    *,
    limit: int = 20,
    offset: int = 0,
) -> DialoguePage:
    _validate_page(limit, offset)
    if not isinstance(query, str) or not query.strip() or len(query) > MAX_QUERY_CHARS:
        raise ValueError(f"query must contain between 1 and {MAX_QUERY_CHARS} characters")
    tenant_id, source_id = _scope(scope)
    pattern = "%" + query.strip().replace("%", "\\%").replace("_", "\\_") + "%"
    rows = conn.execute(
        "SELECT * FROM dialogue_tasks WHERE tenant_id = ? AND source_id = ? AND status = 'active'"
        " AND (title LIKE ? ESCAPE '\\' OR original_request LIKE ? ESCAPE '\\')"
        " ORDER BY updated_at DESC, id ASC LIMIT ? OFFSET ?",
        (tenant_id, source_id, pattern, pattern, limit + 1, offset),
    ).fetchall()
    return DialoguePage(tuple(_candidate(row) for row in rows[:limit]), offset, len(rows) > limit)


def get_focus(conn: sqlite3.Connection, scope: ConversationScope, actor_id: str) -> str | None:
    row = conn.execute(
        "SELECT task_id FROM dialogue_focus WHERE tenant_id = ? AND source_id = ? AND actor_id = ?",
        (*_scope(scope), actor_id),
    ).fetchone()
    return None if row is None else str(row["task_id"])


def get_question(conn: sqlite3.Connection, scope: ConversationScope, question_id: str) -> DialogueQuestion | None:
    row = conn.execute(
        "SELECT * FROM dialogue_questions WHERE tenant_id = ? AND source_id = ? AND id = ?",
        (*_scope(scope), question_id),
    ).fetchone()
    return None if row is None else _question(conn, scope, row)


def list_pending_questions(
    conn: sqlite3.Connection,
    scope: ConversationScope,
    actor_id: str,
    *,
    limit: int = 20,
) -> DialogueQuestionPage:
    _validate_page(limit)
    rows = conn.execute(
        "SELECT * FROM dialogue_questions WHERE tenant_id = ? AND source_id = ?"
        " AND actor_id = ? AND status = 'pending' ORDER BY created_at DESC, id ASC LIMIT ?",
        (*_scope(scope), actor_id, limit + 1),
    ).fetchall()
    questions = tuple(_question(conn, scope, row) for row in rows[:limit])
    return DialogueQuestionPage(questions, len(rows) > limit)


def get_confirmed_reference(
    conn: sqlite3.Connection,
    scope: ConversationScope,
    channel: str,
    external_message_id: str,
) -> ConfirmedDialogueReference | None:
    row = conn.execute(
        "SELECT * FROM dialogue_confirmed_references"
        " WHERE tenant_id = ? AND source_id = ? AND channel = ? AND external_message_id = ?",
        (*_scope(scope), channel, external_message_id),
    ).fetchone()
    if row is None:
        return None
    return ConfirmedDialogueReference(
        channel=str(row["channel"]),
        external_message_id=str(row["external_message_id"]),
        outbound_message_id=str(row["outbound_message_id"]),
        task_id=row["task_id"],
        question_id=row["question_id"],
        confirmed_at=int(row["confirmed_at"]),
    )


def get_input_reference(
    conn: sqlite3.Connection,
    scope: ConversationScope,
    channel: str,
    external_message_id: str,
) -> DialogueInputReference | None:
    row = conn.execute(
        "SELECT input_id, task_id, question_id FROM dialogue_inputs"
        " WHERE tenant_id = ? AND source_id = ? AND channel = ? AND source_message_id = ?",
        (*_scope(scope), channel, external_message_id),
    ).fetchone()
    if row is None:
        return None
    return DialogueInputReference(
        channel=channel,
        external_message_id=external_message_id,
        input_id=str(row["input_id"]),
        task_id=row["task_id"],
        question_id=row["question_id"],
    )


def _validate_plan(plan: DialoguePlan) -> None:
    if len(plan.plan_id) > 256:
        raise ValueError("plan_id must be at most 256 characters")
    route_by_id = {item.input_id: item for item in plan.routes}
    for item in plan.inputs:
        if len(item.text) > MAX_INPUT_TEXT_CHARS:
            raise ValueError(f"input text exceeds {MAX_INPUT_TEXT_CHARS} characters")
        if (
            len(item.input_id) > 256
            or len(item.actor_id) > 256
            or len(item.channel) > 64
            or any(
                value is not None and len(value) > 256
                for value in (item.source_message_id, item.reply_to_message_id, item.message_thread_id)
            )
        ):
            raise ValueError("dialogue input identifiers exceed their storage bounds")
        route = route_by_id[item.input_id]
        if route.action == "create":
            if route.title is None or not route.title.strip() or len(route.title) > MAX_TITLE_CHARS:
                raise ValueError(f"task title must contain 1 to {MAX_TITLE_CHARS} characters")
        if route.action == "resolve_question" and route.task_key is not None:
            if route.title is None or not route.title.strip() or len(route.title) > MAX_TITLE_CHARS:
                raise ValueError(f"task title must contain 1 to {MAX_TITLE_CHARS} characters")
        if route.action == "question":
            assert route.question_text is not None
            if len(route.question_text) > MAX_QUESTION_TEXT_CHARS:
                raise ValueError(f"routing question exceeds {MAX_QUESTION_TEXT_CHARS} characters")
            if len(route.candidates) > MAX_QUESTION_CANDIDATES:
                raise ValueError(f"at most {MAX_QUESTION_CANDIDATES} candidates may be snapshotted")
            if any(
                len(candidate.task_id) > 256
                or len(candidate.title) > MAX_TITLE_CHARS
                or (candidate.excerpt is not None and len(candidate.excerpt) > 240)
                for candidate in route.candidates
            ):
                raise ValueError("routing question candidate data exceeds its storage bounds")


def _new_task_id() -> str:
    return "task_" + uuid.uuid4().hex


def _resolve_task_target(route: DialogueRoute, local_tasks: dict[str, str]) -> str:
    if route.task_id is not None:
        return route.task_id
    if route.task_key is None or route.task_key not in local_tasks:
        raise DialogueConflictError("local task key does not refer to an earlier creation in this plan")
    return local_tasks[route.task_key]


def _get_task_row(conn: sqlite3.Connection, scope: ConversationScope, task_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM dialogue_tasks WHERE tenant_id = ? AND source_id = ? AND id = ?",
        (*_scope(scope), task_id),
    ).fetchone()


def _insert_input(
    conn: sqlite3.Connection,
    scope: ConversationScope,
    item: DialogueInput,
    *,
    task_id: str | None,
    question_id: str | None,
    plan_id: str,
    now: int,
) -> None:
    conn.execute(
        "INSERT INTO dialogue_inputs"
        " (input_id, tenant_id, source_id, actor_id, text, occurred_at, channel, source_message_id,"
        " reply_to_message_id, message_thread_id, task_id, question_id, plan_id, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            item.input_id,
            *_scope(scope),
            item.actor_id,
            item.text,
            item.occurred_at,
            item.channel,
            item.source_message_id,
            item.reply_to_message_id,
            item.message_thread_id,
            task_id,
            question_id,
            plan_id,
            now,
        ),
    )


def _insert_input_message(
    conn: sqlite3.Connection,
    scope: ConversationScope,
    item: DialogueInput,
    *,
    task_id: str,
) -> None:
    tenant_id, source_id = _scope(scope)
    conn.execute(
        "INSERT INTO dialogue_messages"
        " (message_id, tenant_id, source_id, task_id, role, actor_id, text, occurred_at, input_id, source_message_id)"
        " VALUES (?, ?, ?, ?, 'user', ?, ?, ?, ?, ?)",
        (
            "input_" + item.input_id,
            tenant_id,
            source_id,
            task_id,
            item.actor_id,
            item.text,
            item.occurred_at,
            item.input_id,
            item.source_message_id,
        ),
    )


def _attach_input(
    conn: sqlite3.Connection,
    scope: ConversationScope,
    item: DialogueInput,
    task_id: str,
    plan_id: str,
    *,
    now: int,
) -> None:
    row = _get_task_row(conn, scope, task_id)
    if row is None or row["status"] != "active":
        raise DialogueConflictError("dialogue task is unavailable or terminal")
    _insert_input(conn, scope, item, task_id=task_id, question_id=None, plan_id=plan_id, now=now)
    _insert_input_message(conn, scope, item, task_id=task_id)
    conn.execute(
        "UPDATE dialogue_tasks SET updated_at = ? WHERE tenant_id = ? AND source_id = ? AND id = ?",
        (now, *_scope(scope), task_id),
    )


def _create_or_extend_question(
    conn: sqlite3.Connection,
    *,
    scope: ConversationScope,
    item: DialogueInput,
    route: DialogueRoute,
    plan_id: str,
    now: int,
) -> None:
    assert route.question_id is not None and route.question_text is not None
    tenant_id, source_id = _scope(scope)
    existing = conn.execute(
        "SELECT * FROM dialogue_questions WHERE tenant_id = ? AND source_id = ? AND id = ?",
        (tenant_id, source_id, route.question_id),
    ).fetchone()
    if existing is not None:
        if existing["status"] != "pending":
            raise DialogueConflictError("routing question id was reused with changed or terminal input")
        if existing["actor_id"] != item.actor_id:
            reference = None
            if item.reply_to_message_id is not None:
                reference = conn.execute(
                    "SELECT question_id FROM dialogue_confirmed_references"
                    " WHERE tenant_id = ? AND source_id = ? AND channel = ? AND external_message_id = ?",
                    (tenant_id, source_id, item.channel, item.reply_to_message_id),
                ).fetchone()
            if reference is None or reference["question_id"] != route.question_id:
                raise DialogueConflictError(
                    "another actor may extend a routing question only by replying to its confirmed message"
                )
        expected_candidates = [asdict(candidate) for candidate in _question_candidates(conn, scope, route.candidates)]
        if (
            existing["text"] != route.question_text
            or json.loads(existing["candidates_json"]) != expected_candidates
            or bool(existing["has_more_candidates"]) != route.has_more_candidates
        ):
            raise DialogueConflictError("routing question id was reused with changed or terminal input")
        return
    candidates = _question_candidates(conn, scope, route.candidates)
    conn.execute(
        "INSERT INTO dialogue_questions"
        " (id, tenant_id, source_id, actor_id, text, status, revision, candidates_json,"
        "  has_more_candidates, created_at)"
        " VALUES (?, ?, ?, ?, ?, 'pending', 1, ?, ?, ?)",
        (
            route.question_id,
            tenant_id,
            source_id,
            item.actor_id,
            route.question_text,
            json.dumps([asdict(candidate) for candidate in candidates], ensure_ascii=False),
            int(route.has_more_candidates),
            now,
        ),
    )


def _question_candidates(
    conn: sqlite3.Connection,
    scope: ConversationScope,
    candidates: tuple[DialogueQuestionCandidate, ...],
) -> tuple[DialogueQuestionCandidate, ...]:
    output: list[DialogueQuestionCandidate] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate.task_id in seen:
            raise DialogueConflictError("routing question candidate list contains duplicate tasks")
        seen.add(candidate.task_id)
        row = _get_task_row(conn, scope, candidate.task_id)
        if row is None or row["status"] != "active" or int(row["revision"]) != candidate.revision:
            raise DialogueConflictError("routing question candidate is stale or unavailable")
        output.append(
            DialogueQuestionCandidate(
                task_id=str(row["id"]),
                title=str(row["title"]),
                revision=int(row["revision"]),
                excerpt=str(row["original_request"])[:240],
            )
        )
    return tuple(output)


def _resolve_question(
    conn: sqlite3.Connection,
    *,
    scope: ConversationScope,
    item: DialogueInput,
    route: DialogueRoute,
    plan_id: str,
    now: int,
) -> str:
    assert route.question_id is not None and route.question_revision is not None
    tenant_id, source_id = _scope(scope)
    question = conn.execute(
        "SELECT * FROM dialogue_questions WHERE tenant_id = ? AND source_id = ? AND id = ?",
        (tenant_id, source_id, route.question_id),
    ).fetchone()
    if question is None or question["status"] != "pending" or int(question["revision"]) != route.question_revision:
        raise DialogueConflictError("routing question is missing, stale, or already resolved")
    reference = None
    if item.reply_to_message_id is not None:
        reference = conn.execute(
            "SELECT question_id FROM dialogue_confirmed_references"
            " WHERE tenant_id = ? AND source_id = ? AND channel = ? AND external_message_id = ?",
            (tenant_id, source_id, item.channel, item.reply_to_message_id),
        ).fetchone()
        if reference is None or reference["question_id"] != route.question_id:
            raise DialogueConflictError("reply does not refer to this question's confirmed message")
    elif question["actor_id"] != item.actor_id:
        raise DialogueConflictError("another actor may resolve a question only by replying to its confirmed message")
    else:
        reference = conn.execute(
            "SELECT 1 FROM dialogue_confirmed_references"
            " WHERE tenant_id = ? AND source_id = ? AND question_id = ? LIMIT 1",
            (tenant_id, source_id, route.question_id),
        ).fetchone()
        if reference is None:
            raise DialogueConflictError(
                "semantic routing question resolution requires confirmed delivery"
            )
    original_inputs = conn.execute(
        "SELECT * FROM dialogue_inputs WHERE tenant_id = ? AND source_id = ?"
        " AND question_id = ? AND task_id IS NULL ORDER BY sequence",
        (tenant_id, source_id, route.question_id),
    ).fetchall()
    if not original_inputs:
        raise DialogueConflictError("routing question has no unresolved original input")
    if route.task_key is not None:
        assert route.title is not None
        task_id = _new_task_id()
        conn.execute(
            "INSERT INTO dialogue_tasks"
            " (id, tenant_id, source_id, title, original_request, status, revision, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, 'active', 1, ?, ?)",
            (task_id, tenant_id, source_id, route.title, original_inputs[0]["text"], now, now),
        )
    else:
        assert route.task_id is not None
        task_id = route.task_id
        candidates = json.loads(question["candidates_json"])
        selected = next((entry for entry in candidates if entry["task_id"] == task_id), None)
        if selected is None:
            raise DialogueConflictError("selected task was not offered by this routing question")
        task = _get_task_row(conn, scope, task_id)
        if task is None or task["status"] != "active" or int(task["revision"]) != int(selected["revision"]):
            raise DialogueConflictError("selected task changed after the routing question was issued")
    updated = conn.execute(
        "UPDATE dialogue_questions SET status = 'resolved', revision = revision + 1,"
        " resolved_task_id = ?, resolved_at = ?"
        " WHERE tenant_id = ? AND source_id = ? AND id = ? AND status = 'pending' AND revision = ?",
        (task_id, now, tenant_id, source_id, route.question_id, route.question_revision),
    )
    if updated.rowcount != 1:
        raise DialogueConflictError("routing question was resolved concurrently")
    for old_input in original_inputs:
        changed = conn.execute(
            "UPDATE dialogue_inputs SET task_id = ? WHERE tenant_id = ? AND source_id = ?"
            " AND input_id = ? AND task_id IS NULL",
            (task_id, tenant_id, source_id, old_input["input_id"]),
        )
        if changed.rowcount != 1:
            raise DialogueConflictError("routing question input was assigned concurrently")
        _insert_message_from_input_row(conn, old_input, task_id=task_id)
    _suppress_question_outbound(conn, tenant_id, source_id, route.question_id, now=now)
    _insert_input(conn, scope, item, task_id=task_id, question_id=None, plan_id=plan_id, now=now)
    _insert_input_message(conn, scope, item, task_id=task_id)
    conn.execute(
        "UPDATE dialogue_tasks SET updated_at = ? WHERE tenant_id = ? AND source_id = ? AND id = ?",
        (now, tenant_id, source_id, task_id),
    )
    return task_id


def _insert_message_from_input_row(conn: sqlite3.Connection, row: sqlite3.Row, *, task_id: str) -> None:
    conn.execute(
        "INSERT INTO dialogue_messages"
        " (message_id, tenant_id, source_id, task_id, role, actor_id, text, occurred_at, input_id, source_message_id)"
        " VALUES (?, ?, ?, ?, 'user', ?, ?, ?, ?, ?)",
        (
            "input_" + str(row["input_id"]),
            row["tenant_id"],
            row["source_id"],
            task_id,
            row["actor_id"],
            row["text"],
            row["occurred_at"],
            row["input_id"],
            row["source_message_id"],
        ),
    )


def _set_focus(
    conn: sqlite3.Connection,
    tenant_id: str,
    source_id: str,
    actor_id: str,
    task_id: str,
    now: int,
) -> None:
    conn.execute(
        "INSERT INTO dialogue_focus (tenant_id, source_id, actor_id, task_id, updated_at)"
        " VALUES (?, ?, ?, ?, ?)"
        " ON CONFLICT(tenant_id, source_id, actor_id) DO UPDATE SET"
        " task_id = excluded.task_id, updated_at = excluded.updated_at",
        (tenant_id, source_id, actor_id, task_id, now),
    )


def _suppress_task_outbound(
    conn: sqlite3.Connection,
    tenant_id: str,
    source_id: str,
    task_id: str,
    *,
    reason: str,
    now: int,
) -> None:
    conn.execute(
        "UPDATE outbound_messages SET status = 'cancelled',"
        " last_error = ?,"
        " lease_owner = NULL, lease_until = NULL, lease_token = NULL, updated_at = ?"
        " WHERE tenant_id = ? AND source_id = ? AND task_id = ?"
        " AND status IN ('queued','retrying','leased')",
        (reason, now, tenant_id, source_id, task_id),
    )


def _suppress_question_outbound(
    conn: sqlite3.Connection,
    tenant_id: str,
    source_id: str,
    question_id: str,
    *,
    now: int,
) -> None:
    conn.execute(
        "UPDATE outbound_messages SET status = 'cancelled',"
        " last_error = 'routing question resolved before send admission',"
        " lease_owner = NULL, lease_until = NULL, lease_token = NULL, updated_at = ?"
        " WHERE tenant_id = ? AND source_id = ? AND routing_question_id = ?"
        " AND status IN ('queued','retrying','leased')",
        (now, tenant_id, source_id, question_id),
    )


def _task(row: sqlite3.Row) -> DialogueTask:
    return DialogueTask(
        task_id=str(row["id"]),
        scope=ConversationScope(str(row["tenant_id"]), str(row["source_id"])),
        title=str(row["title"]),
        original_request=str(row["original_request"]),
        status=row["status"],
        revision=int(row["revision"]),
        created_at=int(row["created_at"]),
        updated_at=int(row["updated_at"]),
    )


def _candidate(row: sqlite3.Row) -> DialogueCandidate:
    return DialogueCandidate(
        task_id=str(row["id"]),
        title=str(row["title"]),
        status=row["status"],
        revision=int(row["revision"]),
        updated_at=int(row["updated_at"]),
        excerpt=str(row["original_request"])[:240],
    )


def _message(row: sqlite3.Row) -> DialogueMessage:
    return DialogueMessage(
        message_id=str(row["message_id"]),
        role=row["role"],
        actor_id=row["actor_id"],
        text=str(row["text"]),
        occurred_at=int(row["occurred_at"]),
        input_id=row["input_id"],
        source_message_id=row["source_message_id"],
    )


def _question(conn: sqlite3.Connection, scope: ConversationScope, row: sqlite3.Row) -> DialogueQuestion:
    candidates = tuple(DialogueQuestionCandidate(**entry) for entry in json.loads(row["candidates_json"]))
    inputs = conn.execute(
        "SELECT * FROM dialogue_inputs WHERE tenant_id = ? AND source_id = ? AND question_id = ?"
        " ORDER BY sequence",
        (*_scope(scope), row["id"]),
    ).fetchall()
    delivery_confirmed = conn.execute(
        "SELECT 1 FROM dialogue_confirmed_references"
        " WHERE tenant_id = ? AND source_id = ? AND question_id = ? LIMIT 1",
        (*_scope(scope), row["id"]),
    ).fetchone() is not None
    return DialogueQuestion(
        question_id=str(row["id"]),
        scope=scope,
        actor_id=str(row["actor_id"]),
        text=str(row["text"]),
        inputs=tuple(_input(item) for item in inputs),
        candidates=candidates,
        has_more_candidates=bool(row["has_more_candidates"]),
        delivery_confirmed=delivery_confirmed,
        status=row["status"],
        revision=int(row["revision"]),
        resolved_task_id=row["resolved_task_id"],
        created_at=int(row["created_at"]),
        resolved_at=None if row["resolved_at"] is None else int(row["resolved_at"]),
    )


def _input(row: sqlite3.Row) -> DialogueInput:
    return DialogueInput(
        input_id=str(row["input_id"]),
        actor_id=str(row["actor_id"]),
        text=str(row["text"]),
        occurred_at=int(row["occurred_at"]),
        channel=str(row["channel"]),
        source_message_id=row["source_message_id"],
        reply_to_message_id=row["reply_to_message_id"],
        message_thread_id=row["message_thread_id"],
    )


def _plan_result(value: dict[str, Any]) -> DialoguePlanResult:
    return DialoguePlanResult(
        plan_id=str(value["plan_id"]),
        assignments=tuple(DialogueAssignment(**item) for item in value["assignments"]),
    )


def _dialogue_plan(value: dict[str, Any]) -> DialoguePlan:
    scope = ConversationScope(**value["scope"])
    inputs = tuple(DialogueInput(**item) for item in value["inputs"])
    routes = tuple(
        DialogueRoute(
            **{
                **item,
                "candidates": tuple(DialogueQuestionCandidate(**candidate) for candidate in item["candidates"]),
            }
        )
        for item in value["routes"]
    )
    return DialoguePlan(str(value["plan_id"]), scope, inputs, routes)
