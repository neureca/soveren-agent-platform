"""Typed, conversation-scoped dialogue persistence contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol

from soveren_agent_platform.conversation import ConversationScope

DialogueTaskStatus = Literal["active", "closed", "cancelled"]
DialogueRouteAction = Literal["create", "attach", "question", "resolve_question", "close", "cancel"]


class DialogueConflictError(RuntimeError):
    """A dialogue command conflicts with an immutable or stale persisted fact."""


@dataclass(frozen=True, slots=True)
class DialogueInput:
    input_id: str
    actor_id: str
    text: str
    occurred_at: int
    channel: str
    source_message_id: str | None = None
    reply_to_message_id: str | None = None
    message_thread_id: str | None = None

    def __post_init__(self) -> None:
        for name, value in (("input_id", self.input_id), ("actor_id", self.actor_id), ("channel", self.channel)):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        if not isinstance(self.text, str) or not self.text.strip():
            raise ValueError("text must be a non-empty string")
        if type(self.occurred_at) is not int:
            raise ValueError("occurred_at must be an integer timestamp")
        for name, optional_value in (
            ("source_message_id", self.source_message_id),
            ("reply_to_message_id", self.reply_to_message_id),
            ("message_thread_id", self.message_thread_id),
        ):
            if optional_value is not None and (not isinstance(optional_value, str) or not optional_value.strip()):
                raise ValueError(f"{name} must be a non-empty string when provided")


@dataclass(frozen=True, slots=True)
class DialogueCandidate:
    task_id: str
    title: str
    status: DialogueTaskStatus
    revision: int
    updated_at: int
    excerpt: str | None = None


@dataclass(frozen=True, slots=True)
class DialogueQuestionCandidate:
    task_id: str
    title: str
    revision: int
    excerpt: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.task_id, str) or not self.task_id.strip():
            raise ValueError("candidate task_id must be non-empty")
        if not isinstance(self.title, str) or not self.title.strip():
            raise ValueError("candidate title must be non-empty")
        if type(self.revision) is not int or self.revision < 1:
            raise ValueError("candidate revision must be positive")
        if self.excerpt is not None and not isinstance(self.excerpt, str):
            raise ValueError("candidate excerpt must be a string when provided")


@dataclass(frozen=True, slots=True)
class DialogueTask:
    task_id: str
    scope: ConversationScope
    title: str
    original_request: str
    status: DialogueTaskStatus
    revision: int
    created_at: int
    updated_at: int


@dataclass(frozen=True, slots=True)
class DialogueMessage:
    message_id: str
    role: Literal["user", "assistant"]
    actor_id: str | None
    text: str
    occurred_at: int
    input_id: str | None = None
    source_message_id: str | None = None


@dataclass(frozen=True, slots=True)
class DialogueTaskContext:
    task: DialogueTask
    messages: tuple[DialogueMessage, ...]
    has_older: bool
    next_before_message_id: str | None


@dataclass(frozen=True, slots=True)
class DialogueQuestion:
    question_id: str
    scope: ConversationScope
    actor_id: str
    text: str
    inputs: tuple[DialogueInput, ...]
    candidates: tuple[DialogueQuestionCandidate, ...]
    has_more_candidates: bool
    delivery_confirmed: bool
    status: Literal["pending", "resolved"]
    revision: int
    resolved_task_id: str | None
    created_at: int
    resolved_at: int | None


@dataclass(frozen=True, slots=True)
class DialogueRoute:
    input_id: str
    action: DialogueRouteAction
    task_id: str | None = None
    task_key: str | None = None
    title: str | None = None
    question_id: str | None = None
    question_revision: int | None = None
    question_text: str | None = None
    candidates: tuple[DialogueQuestionCandidate, ...] = ()
    has_more_candidates: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.input_id, str) or not self.input_id.strip():
            raise ValueError("input_id must be non-empty")
        if self.action not in {"create", "attach", "question", "resolve_question", "close", "cancel"}:
            raise ValueError("unsupported dialogue route action")
        if self.action == "create":
            if (
                not isinstance(self.task_key, str)
                or not self.task_key.strip()
                or not isinstance(self.title, str)
                or not self.title.strip()
                or self.task_id is not None
            ):
                raise ValueError("create route requires task_key and title only")
            if self.task_key != f"new:{self.input_id}":
                raise ValueError("new task key must be derived from the first input id")
        elif self.action == "attach":
            if (self.task_id is None) == (self.task_key is None):
                raise ValueError("attach route requires exactly one of task_id or task_key")
            target = self.task_id if self.task_id is not None else self.task_key
            if not isinstance(target, str) or not target.strip():
                raise ValueError("attach target must be non-empty")
        elif self.action == "question":
            if (
                not self.question_id
                or not isinstance(self.question_id, str)
                or not isinstance(self.question_text, str)
                or not self.question_text
                or not self.question_text.strip()
                or self.task_id is not None
                or self.task_key is not None
                or (
                    self.question_revision is not None
                    and (type(self.question_revision) is not int or self.question_revision < 1)
                )
            ):
                raise ValueError("question route requires question_id and question_text and no task target")
        elif self.action == "resolve_question":
            question_valid = isinstance(self.question_id, str) and bool(self.question_id.strip())
            revision_valid = type(self.question_revision) is int and self.question_revision >= 1
            existing_task = (
                isinstance(self.task_id, str)
                and bool(self.task_id.strip())
                and self.task_key is None
                and self.title is None
            )
            new_task = (
                self.task_id is None
                and isinstance(self.task_key, str)
                and bool(self.task_key.strip())
                and self.task_key == f"new:{self.input_id}"
                and isinstance(self.title, str)
                and bool(self.title.strip())
            )
            if not question_valid or not revision_valid or not (existing_task or new_task):
                raise ValueError(
                    "resolve_question route requires question_id/revision and either an existing task_id "
                    "or a new task_key/title derived from this input"
                )
        elif self.action in {"close", "cancel"} and (self.task_id is None) == (self.task_key is None):
            raise ValueError(f"{self.action} route requires exactly one of task_id or task_key")
        elif self.action in {"close", "cancel"}:
            target = self.task_id if self.task_id is not None else self.task_key
            if not isinstance(target, str) or not target.strip():
                raise ValueError(f"{self.action} target must be non-empty")


@dataclass(frozen=True, slots=True)
class DialoguePlan:
    plan_id: str
    scope: ConversationScope
    inputs: tuple[DialogueInput, ...]
    routes: tuple[DialogueRoute, ...]

    def __post_init__(self) -> None:
        if not self.plan_id.strip():
            raise ValueError("plan_id must be non-empty")
        if not self.inputs:
            raise ValueError("at least one dialogue input is required")
        input_ids = [item.input_id for item in self.inputs]
        route_ids = [item.input_id for item in self.routes]
        if len(input_ids) != len(set(input_ids)) or len(route_ids) != len(set(route_ids)):
            raise ValueError("dialogue input and route ids must be unique within a plan")
        if set(input_ids) != set(route_ids):
            raise ValueError("every input must have exactly one route")


@dataclass(frozen=True, slots=True)
class DialogueAssignment:
    input_id: str
    task_id: str | None
    question_id: str | None
    task_revision: int | None
    question_revision: int | None = None


@dataclass(frozen=True, slots=True)
class DialoguePlanResult:
    plan_id: str
    assignments: tuple[DialogueAssignment, ...]


@dataclass(frozen=True, slots=True)
class DialoguePage:
    items: tuple[DialogueCandidate, ...]
    offset: int
    has_more: bool


@dataclass(frozen=True, slots=True)
class DialogueQuestionPage:
    items: tuple[DialogueQuestion, ...]
    has_more: bool


@dataclass(frozen=True, slots=True)
class ConfirmedDialogueReference:
    channel: str
    external_message_id: str
    outbound_message_id: str
    task_id: str | None
    question_id: str | None
    confirmed_at: int
    question_revision: int | None = None


@dataclass(frozen=True, slots=True)
class DialogueInputReference:
    channel: str
    external_message_id: str
    input_id: str
    task_id: str | None
    question_id: str | None


class DialogueStore(Protocol):
    """Atomic route-plan and task-context operations for one trusted scope."""

    async def apply_plan(self, plan: DialoguePlan) -> DialoguePlanResult: ...

    async def get_plan(self, scope: ConversationScope, plan_id: str) -> DialoguePlan | None: ...

    async def get_task(self, scope: ConversationScope, task_id: str) -> DialogueTask | None: ...

    async def get_context(
        self,
        scope: ConversationScope,
        task_id: str,
        *,
        limit: int = 50,
        before_message_id: str | None = None,
        through_input_id: str | None = None,
    ) -> DialogueTaskContext | None: ...

    async def list_candidates(self, scope: ConversationScope, *, limit: int = 20, offset: int = 0) -> DialoguePage: ...

    async def search_candidates(
        self, scope: ConversationScope, query: str, *, limit: int = 20, offset: int = 0
    ) -> DialoguePage: ...

    async def get_focus(self, scope: ConversationScope, actor_id: str) -> str | None: ...

    async def get_question(self, scope: ConversationScope, question_id: str) -> DialogueQuestion | None: ...

    async def list_pending_questions(
        self, scope: ConversationScope, actor_id: str, *, limit: int = 20
    ) -> DialogueQuestionPage: ...

    async def get_confirmed_reference(
        self, scope: ConversationScope, channel: str, external_message_id: str
    ) -> ConfirmedDialogueReference | None: ...

    async def get_input_reference(
        self, scope: ConversationScope, channel: str, external_message_id: str
    ) -> DialogueInputReference | None: ...
