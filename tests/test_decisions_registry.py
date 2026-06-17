from typing import Literal

import pytest
from pydantic import ConfigDict

from agent_platform.decisions import (
    BaseDecision,
    DecisionParseError,
    DecisionRegistry,
    DecisionValidationError,
    UnknownDecisionKindError,
)


class ReplyDecision(BaseDecision):
    kind: Literal["reply"]
    text: str


class CreateTaskDecision(BaseDecision):
    kind: Literal["create_task"]
    title: str
    description: str | None = None


def test_decision_registry_returns_typed_decision():
    registry = DecisionRegistry()
    registry.register("reply", ReplyDecision)
    registry.register("create_task", CreateTaskDecision)

    decision = registry.parse('{"kind":"create_task","title":"Call client"}')

    assert isinstance(decision, CreateTaskDecision)
    assert decision.kind == "create_task"
    assert decision.payload == {"title": "Call client", "description": None}
    assert registry.registered_kinds() == ("create_task", "reply")


def test_decision_registry_rejects_unknown_kind():
    registry = DecisionRegistry()
    registry.register("reply", ReplyDecision)

    with pytest.raises(UnknownDecisionKindError):
        registry.parse('{"kind":"save_note","title":"x"}')


def test_decision_registry_rejects_code_fence_and_prose():
    registry = DecisionRegistry()
    registry.register("reply", ReplyDecision)

    with pytest.raises(DecisionParseError):
        registry.parse('```json\n{"kind":"reply","text":"hi"}\n```')
    with pytest.raises(DecisionParseError):
        registry.parse('Here is JSON: {"kind":"reply","text":"hi"}')


def test_decision_registry_rejects_extra_fields_by_default():
    registry = DecisionRegistry()
    registry.register("reply", ReplyDecision)

    with pytest.raises(DecisionValidationError):
        registry.parse('{"kind":"reply","text":"hi","extra":true}')


def test_decision_registry_allows_app_to_choose_lenient_model_if_needed():
    class LenientDecision(BaseDecision):
        model_config = ConfigDict(extra="ignore")

        kind: Literal["lenient"]
        value: int

    registry = DecisionRegistry()
    registry.register("lenient", LenientDecision)

    decision = registry.parse('{"kind":"lenient","value":1,"ignored":true}')

    assert isinstance(decision, LenientDecision)
    assert decision.model_dump() == {"kind": "lenient", "value": 1}

