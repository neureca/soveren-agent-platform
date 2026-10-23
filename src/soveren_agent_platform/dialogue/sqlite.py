"""Async SQLite adapter for scoped dialogue persistence."""

from __future__ import annotations

from soveren_agent_platform.conversation import ConversationScope
from soveren_agent_platform.dialogue import store
from soveren_agent_platform.dialogue.contracts import (
    ConfirmedDialogueReference,
    DialogueInputReference,
    DialoguePage,
    DialoguePlan,
    DialoguePlanResult,
    DialogueQuestion,
    DialogueQuestionPage,
    DialogueTask,
    DialogueTaskContext,
)
from soveren_agent_platform.storage.adapter import SQLiteAdapter
from soveren_agent_platform.storage.sqlite import run_sqlite


class SQLiteDialogueStore(SQLiteAdapter):
    async def apply_plan(self, plan: DialoguePlan) -> DialoguePlanResult:
        return await run_sqlite(self._conn, store.apply_dialogue_plan, plan)

    async def get_plan(self, scope: ConversationScope, plan_id: str) -> DialoguePlan | None:
        return await run_sqlite(self._conn, store.get_plan, scope, plan_id)

    async def get_task(self, scope: ConversationScope, task_id: str) -> DialogueTask | None:
        return await run_sqlite(self._conn, store.get_task, scope, task_id)

    async def get_context(
        self,
        scope: ConversationScope,
        task_id: str,
        *,
        limit: int = 50,
        before_message_id: str | None = None,
        through_input_id: str | None = None,
    ) -> DialogueTaskContext | None:
        return await run_sqlite(
            self._conn,
            store.get_task_context,
            scope,
            task_id,
            limit=limit,
            before_message_id=before_message_id,
            through_input_id=through_input_id,
        )

    async def list_candidates(
        self,
        scope: ConversationScope,
        *,
        limit: int = 20,
        offset: int = 0,
    ) -> DialoguePage:
        return await run_sqlite(self._conn, store.list_candidates, scope, limit=limit, offset=offset)

    async def search_candidates(
        self,
        scope: ConversationScope,
        query: str,
        *,
        limit: int = 20,
        offset: int = 0,
    ) -> DialoguePage:
        return await run_sqlite(
            self._conn,
            store.search_candidates,
            scope,
            query,
            limit=limit,
            offset=offset,
        )

    async def get_focus(self, scope: ConversationScope, actor_id: str) -> str | None:
        return await run_sqlite(self._conn, store.get_focus, scope, actor_id)

    async def get_question(self, scope: ConversationScope, question_id: str) -> DialogueQuestion | None:
        return await run_sqlite(self._conn, store.get_question, scope, question_id)

    async def list_pending_questions(
        self,
        scope: ConversationScope,
        actor_id: str,
        *,
        limit: int = 20,
    ) -> DialogueQuestionPage:
        return await run_sqlite(
            self._conn,
            store.list_pending_questions,
            scope,
            actor_id,
            limit=limit,
        )

    async def get_confirmed_reference(
        self,
        scope: ConversationScope,
        channel: str,
        external_message_id: str,
    ) -> ConfirmedDialogueReference | None:
        return await run_sqlite(
            self._conn,
            store.get_confirmed_reference,
            scope,
            channel,
            external_message_id,
        )

    async def get_input_reference(
        self,
        scope: ConversationScope,
        channel: str,
        external_message_id: str,
    ) -> DialogueInputReference | None:
        return await run_sqlite(
            self._conn,
            store.get_input_reference,
            scope,
            channel,
            external_message_id,
        )
