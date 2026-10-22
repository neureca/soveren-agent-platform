from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from soveren_agent_platform.conversation import ConversationScope
from soveren_agent_platform.dialogue import (
    DialogueConflictError,
    DialogueInput,
    DialoguePlan,
    DialogueQuestionCandidate,
    DialogueRoute,
    SQLiteDialogueStore,
)
from soveren_agent_platform.dialogue.store import apply_dialogue_plan
from soveren_agent_platform.outbound import OutboundTaskReference
from soveren_agent_platform.outbound.sqlite import SQLiteOutboundQueue
from soveren_agent_platform.storage import bootstrap_platform_storage
from soveren_agent_platform.storage.sqlite import open_sqlite

SCOPE = ConversationScope("tenant-a", "telegram:42")


def _input(input_id: str, actor: str, text: str, *, reply_to: str | None = None) -> DialogueInput:
    return DialogueInput(
        input_id,
        actor,
        text,
        100,
        "telegram",
        source_message_id=f"in-{input_id}",
        reply_to_message_id=reply_to,
        message_thread_id="7",
    )


def _plan(plan_id: str, inputs: tuple[DialogueInput, ...], routes: tuple[DialogueRoute, ...]) -> DialoguePlan:
    return DialoguePlan(plan_id, SCOPE, inputs, routes)


def test_scoped_mixed_assignments_replay_and_input_boundary(tmp_path):
    async def scenario():
        db_path = tmp_path / "dialogue.db"
        await bootstrap_platform_storage(db_path)
        async with await SQLiteDialogueStore.open(db_path) as store:
            plan = _plan(
                "mixed-plan",
                (_input("a1", "actor-a", "first A"), _input("b1", "actor-b", "B"), _input("a2", "actor-a", "second A")),
                (
                    DialogueRoute("a1", "create", task_key="new:a1", title="Task A"),
                    DialogueRoute("b1", "create", task_key="new:b1", title="Task B"),
                    DialogueRoute("a2", "attach", task_key="new:a1"),
                ),
            )
            result = await store.apply_plan(plan)
            replay = await store.apply_plan(plan)
            assert result == replay
            assert result.assignments[0].task_id == result.assignments[2].task_id
            assert result.assignments[0].task_id != result.assignments[1].task_id
            assert result.assignments[0].task_revision == result.assignments[2].task_revision == 1
            assert await store.get_plan(SCOPE, "mixed-plan") == plan
            first_page = await store.list_candidates(SCOPE, limit=1)
            next_page = await store.list_candidates(SCOPE, limit=1, offset=1)
            search_page = await store.search_candidates(SCOPE, "Task", limit=1)
            assert len(first_page.items) == len(next_page.items) == len(search_page.items) == 1
            assert first_page.has_more and not next_page.has_more
            assert search_page.has_more
            assert (
                await store.get_task(ConversationScope("tenant-a", "other-chat"), result.assignments[0].task_id) is None
            )
            assert (
                await store.get_task(ConversationScope("tenant-b", "telegram:42"), result.assignments[0].task_id)
                is None
            )
            assert await store.get_focus(SCOPE, "actor-a") == result.assignments[2].task_id

            context = await store.get_context(SCOPE, result.assignments[0].task_id, through_input_id="a1")
            assert context is not None
            assert [message.text for message in context.messages] == ["first A"]
            full = await store.get_context(SCOPE, result.assignments[0].task_id, limit=1)
            assert full is not None and full.has_older
            assert full.messages[0].text == "second A"
            older = await store.get_context(
                SCOPE,
                result.assignments[0].task_id,
                limit=1,
                before_message_id=full.next_before_message_id,
            )
            assert older is not None and older.messages[0].text == "first A"

            changed = _plan(
                "mixed-plan",
                (_input("a1", "actor-a", "changed"), _input("b1", "actor-b", "B"), _input("a2", "actor-a", "second A")),
                plan.routes,
            )
            with pytest.raises(DialogueConflictError, match="reused with changed input"):
                await store.apply_plan(changed)

    asyncio.run(scenario())


def test_question_survives_restart_and_resolves_with_original_request(tmp_path):
    async def scenario():
        db_path = tmp_path / "questions.db"
        await bootstrap_platform_storage(db_path)
        async with await SQLiteDialogueStore.open(db_path) as store:
            created = await store.apply_plan(
                _plan(
                    "create",
                    (_input("seed", "actor-a", "original request"),),
                    (DialogueRoute("seed", "create", task_key="new:seed", title="Chosen task"),),
                )
            )
            candidate_id = created.assignments[0].task_id
            assert candidate_id is not None
            question_plan = _plan(
                "ask",
                (_input("ambiguous", "actor-b", "maybe this task"),),
                (
                    DialogueRoute(
                        "ambiguous",
                        "question",
                        question_id="question-1",
                        question_text="Which task should receive this?",
                        candidates=(DialogueQuestionCandidate(candidate_id, "untrusted title", 1),),
                    ),
                ),
            )
            asked = await store.apply_plan(question_plan)
            assert asked.assignments[0].task_id is None
            assert asked.assignments[0].question_id == "question-1"
            assert await store.get_focus(SCOPE, "actor-b") is None
            follow_up = _plan(
                "ask-more",
                (_input("ambiguous-2", "actor-b", "and this follow-up"),),
                (
                    DialogueRoute(
                        "ambiguous-2",
                        "question",
                        question_id="question-1",
                        question_text="Which task should receive this?",
                        candidates=(DialogueQuestionCandidate(candidate_id, "untrusted title", 1),),
                    ),
                ),
            )
            await store.apply_plan(follow_up)
            question = await store.get_question(SCOPE, "question-1")
            assert question is not None
            assert question.text == "Which task should receive this?"
            assert question.candidates[0].title == "Chosen task"
            assert [item.text for item in question.inputs] == ["maybe this task", "and this follow-up"]

        async with await SQLiteDialogueStore.open(db_path) as store:
            recovered = await store.get_plan(SCOPE, "ask")
            recovered_follow_up = await store.get_plan(SCOPE, "ask-more")
            question = await store.get_question(SCOPE, "question-1")
            assert recovered == question_plan
            assert recovered_follow_up == follow_up
            assert question is not None and question.status == "pending" and question.revision == 1
            pending = await store.list_pending_questions(SCOPE, "actor-b", limit=1)
            assert not pending.has_more and pending.items == (question,)
            assert (await store.list_pending_questions(SCOPE, "actor-a", limit=1)).items == ()
            resolved = await store.apply_plan(
                _plan(
                    "resolve",
                    (_input("answer", "actor-b", "the chosen task"),),
                    (
                        DialogueRoute(
                            "answer",
                            "resolve_question",
                            task_id=candidate_id,
                            question_id="question-1",
                            question_revision=1,
                        ),
                    ),
                )
            )
            assert resolved.assignments[0].task_id == candidate_id
            task = await store.get_task(SCOPE, candidate_id)
            context = await store.get_context(SCOPE, candidate_id)
            question = await store.get_question(SCOPE, "question-1")
            assert task is not None and task.original_request == "original request"
            assert question is not None and question.status == "resolved" and question.revision == 2
            assert [message.text for message in context.messages] == [
                "original request",
                "maybe this task",
                "and this follow-up",
                "the chosen task",
            ]

    asyncio.run(scenario())


def test_confirmed_references_outbound_projection_and_terminal_suppression(tmp_path):
    async def scenario():
        db_path = tmp_path / "outbound.db"
        await bootstrap_platform_storage(db_path)
        async with await SQLiteDialogueStore.open(db_path) as dialogue:
            created = await dialogue.apply_plan(
                _plan(
                    "create-task",
                    (_input("in1", "actor-a", "start work"),),
                    (DialogueRoute("in1", "create", task_key="new:in1", title="Work"),),
                )
            )
            task_id = created.assignments[0].task_id
            assert task_id is not None
            inbound = await dialogue.get_input_reference(SCOPE, "telegram", "in-in1")
            assert inbound is not None and inbound.task_id == task_id

            async with await SQLiteOutboundQueue.open(db_path) as queue:
                outbound_id = await queue.enqueue(
                    tenant_id=SCOPE.tenant_id,
                    source_id=SCOPE.source_id,
                    channel="telegram",
                    destination_id="42",
                    text="task answer",
                    idempotency_key="answer-1",
                    task_reference=OutboundTaskReference(task_id=task_id, task_revision=1),
                    reply_to_message_id=902,
                    message_thread_id=7,
                )
                assert outbound_id is not None
                leased = await queue.claim_due(channel="telegram", limit=1, lease_owner="worker", lease_seconds=30)
                assert len(leased) == 1
                assert leased[0].task_reference == OutboundTaskReference(task_id=task_id, task_revision=1)
                assert leased[0].reply_to_message_id == 902 and leased[0].message_thread_id == 7
                assert await queue.mark_sending(outbound_id, lease_token=leased[0].lease_token)
                assert await queue.mark_sent(
                    outbound_id,
                    lease_token=leased[0].lease_token,
                    result={"external_message_id": "bot-77"},
                )
                assert not await queue.mark_sent(
                    outbound_id,
                    lease_token=leased[0].lease_token,
                    result={"external_message_id": "bot-77"},
                )

                reference = await dialogue.get_confirmed_reference(SCOPE, "telegram", "bot-77")
                assert reference is not None and reference.task_id == task_id
                assert await dialogue.get_confirmed_reference(
                    ConversationScope("tenant-a", "other-chat"), "telegram", "bot-77"
                ) is None
                assert await dialogue.get_input_reference(
                    ConversationScope("tenant-b", "telegram:42"), "telegram", "in-in1"
                ) is None
                context = await dialogue.get_context(SCOPE, task_id)
                assert context is not None
                assert [message.text for message in context.messages] == ["start work", "task answer"]

                queued_result = await queue.enqueue_with_result(
                    tenant_id=SCOPE.tenant_id,
                    source_id=SCOPE.source_id,
                    channel="telegram",
                    destination_id="42",
                    text="pending reply",
                    idempotency_key="answer-2",
                    task_reference=OutboundTaskReference(task_id=task_id, task_revision=1),
                )
                assert queued_result.created
                queued_id = queued_result.message_id
                queued = await queue.claim_due(channel="telegram", limit=1, lease_owner="worker", lease_seconds=30)
                assert len(queued) == 1 and queued[0].id == queued_id
                await dialogue.apply_plan(
                    _plan(
                        "cancel-task",
                        (_input("cancel-input", "actor-a", "cancel"),),
                        (DialogueRoute("cancel-input", "cancel", task_id=task_id),),
                    )
                )
                assert not await queue.mark_sending(queued_id, lease_token=queued[0].lease_token)
                row = queue._conn.execute(
                    "SELECT status, last_error, lease_token FROM outbound_messages WHERE id = ?", (queued_id,)
                ).fetchone()
                assert tuple(row) == (
                    "cancelled",
                    "dialogue task cancelled before send admission",
                    None,
                )
                replay = await queue.enqueue_with_result(
                    tenant_id=SCOPE.tenant_id,
                    source_id=SCOPE.source_id,
                    channel="telegram",
                    destination_id="42",
                    text="pending reply",
                    idempotency_key="answer-2",
                    task_reference=OutboundTaskReference(task_id=task_id, task_revision=1),
                )
                assert not replay.created and replay.message_id == queued_id
                with pytest.raises(DialogueConflictError, match="stale or outside"):
                    await queue.enqueue(
                        tenant_id=SCOPE.tenant_id,
                        source_id=SCOPE.source_id,
                        channel="telegram",
                        destination_id="42",
                        text="too late",
                        idempotency_key="answer-3",
                        task_reference=OutboundTaskReference(task_id=task_id, task_revision=1),
                    )
                sent = queue._conn.execute(
                    "SELECT status FROM outbound_messages WHERE id = ?", (outbound_id,)
                ).fetchone()
                assert sent["status"] == "sent"

    asyncio.run(scenario())


def test_other_actor_must_reply_to_confirmed_question_message_to_resolve(tmp_path):
    async def scenario():
        db_path = tmp_path / "question-reply.db"
        await bootstrap_platform_storage(db_path)
        async with await SQLiteDialogueStore.open(db_path) as dialogue:
            task = await dialogue.apply_plan(
                _plan(
                    "seed-task",
                    (_input("seed", "actor-a", "seed"),),
                    (DialogueRoute("seed", "create", task_key="new:seed", title="Task"),),
                )
            )
            task_id = task.assignments[0].task_id
            await dialogue.apply_plan(
                _plan(
                    "ask-question",
                    (_input("question-input", "actor-a", "ambiguous"),),
                    (
                        DialogueRoute(
                            "question-input",
                            "question",
                            question_id="q",
                            question_text="Which?",
                            candidates=(DialogueQuestionCandidate(task_id, "Task", 1),),
                        ),
                    ),
                )
            )
            async with await SQLiteOutboundQueue.open(db_path) as queue:
                outbound_id = await queue.enqueue(
                    tenant_id=SCOPE.tenant_id,
                    source_id=SCOPE.source_id,
                    channel="telegram",
                    destination_id="42",
                    text="Which?",
                    idempotency_key="question-out",
                    task_reference=OutboundTaskReference(routing_question_id="q"),
                )
                lease = (await queue.claim_due(channel="telegram", limit=1, lease_owner="worker", lease_seconds=30))[0]
                assert await queue.mark_sending(outbound_id, lease_token=lease.lease_token)
                assert await queue.mark_sent(outbound_id, lease_token=lease.lease_token, result={"message_id": 88})
            unauthorized = _plan(
                "wrong-actor",
                (_input("wrong-answer", "actor-b", "Task"),),
                (
                    DialogueRoute(
                        "wrong-answer", "resolve_question", task_id=task_id, question_id="q", question_revision=1
                    ),
                ),
            )
            with pytest.raises(DialogueConflictError, match="only by replying"):
                await dialogue.apply_plan(unauthorized)
            assert (await dialogue.get_question(SCOPE, "q")).status == "pending"
            authorized = _plan(
                "right-actor",
                (_input("right-answer", "actor-b", "Task", reply_to="88"),),
                (
                    DialogueRoute(
                        "right-answer", "resolve_question", task_id=task_id, question_id="q", question_revision=1
                    ),
                ),
            )
            resolved = await dialogue.apply_plan(authorized)
            assert resolved.assignments[0].task_id == task_id

    asyncio.run(scenario())


def test_stale_question_candidate_does_not_resolve_or_change_focus(tmp_path):
    async def scenario():
        db_path = tmp_path / "stale-question.db"
        await bootstrap_platform_storage(db_path)
        async with await SQLiteDialogueStore.open(db_path) as store:
            created = await store.apply_plan(
                _plan(
                    "create",
                    (_input("seed", "actor-a", "seed"),),
                    (DialogueRoute("seed", "create", task_key="new:seed", title="Task"),),
                )
            )
            task_id = created.assignments[0].task_id
            await store.apply_plan(
                _plan(
                    "ask",
                    (_input("q-input", "actor-b", "unresolved"),),
                    (
                        DialogueRoute(
                            "q-input",
                            "question",
                            question_id="q",
                            question_text="Which?",
                            candidates=(DialogueQuestionCandidate(task_id, "Task", 1),),
                        ),
                    ),
                )
            )
            await store.apply_plan(
                _plan(
                    "close",
                    (_input("close-input", "actor-a", "close"),),
                    (DialogueRoute("close-input", "close", task_id=task_id),),
                )
            )
            with pytest.raises(DialogueConflictError, match="changed after"):
                await store.apply_plan(
                    _plan(
                        "resolve",
                        (_input("answer", "actor-b", "Task"),),
                        (
                            DialogueRoute(
                                "answer", "resolve_question", task_id=task_id, question_id="q", question_revision=1
                            ),
                        ),
                    )
                )
            question = await store.get_question(SCOPE, "q")
            assert question is not None and question.status == "pending"
            assert await store.get_focus(SCOPE, "actor-b") is None

    asyncio.run(scenario())


def test_outbound_enqueue_is_fenced_by_concurrent_task_cancellation(tmp_path):
    db_path = tmp_path / "enqueue-cancel-race.db"
    asyncio.run(bootstrap_platform_storage(db_path))
    conn = open_sqlite(db_path)
    result = apply_dialogue_plan(
        conn,
        _plan(
            "create",
            (_input("create", "actor-a", "request"),),
            (DialogueRoute("create", "create", task_key="new:create", title="Task"),),
        ),
    )
    task_id = result.assignments[0].task_id
    assert task_id is not None
    conn.close()

    cancelling = open_sqlite(db_path)
    enqueueing = open_sqlite(db_path)
    insert_started = threading.Event()
    release_insert = threading.Event()

    def pause_before_insert() -> int:
        insert_started.set()
        if not release_insert.wait(timeout=5):
            raise TimeoutError("test did not release outbound insert")
        return 1

    enqueueing.create_function("pause_before_outbound_insert", 0, pause_before_insert)
    enqueueing.execute(
        "CREATE TEMP TRIGGER pause_outbound_insert BEFORE INSERT ON main.outbound_messages"
        " BEGIN SELECT pause_before_outbound_insert(); END"
    )
    cancelling.execute("BEGIN IMMEDIATE")
    cancelling.execute(
        "UPDATE dialogue_tasks SET status = 'cancelled', revision = revision + 1"
        " WHERE tenant_id = ? AND source_id = ? AND id = ?",
        (SCOPE.tenant_id, SCOPE.source_id, task_id),
    )

    def enqueue():
        from soveren_agent_platform.outbound.store import enqueue_outbound_with_result

        return enqueue_outbound_with_result(
            enqueueing,
            tenant_id=SCOPE.tenant_id,
            source_id=SCOPE.source_id,
            channel="telegram",
            destination_id="42",
            text="assistant response",
            idempotency_key="cancel-race",
            task_reference=OutboundTaskReference(task_id=task_id, task_revision=1),
        )

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(enqueue)
            assert future.done() is False
            reached_insert_before_cancel_commit = insert_started.wait(timeout=0.2)
            cancelling.execute("COMMIT")
            release_insert.set()
            with pytest.raises(DialogueConflictError, match="stale or outside its conversation"):
                future.result(timeout=5)
            assert not reached_insert_before_cancel_commit
    finally:
        release_insert.set()
        if cancelling.in_transaction:
            cancelling.execute("ROLLBACK")
        enqueueing.close()
        cancelling.close()
