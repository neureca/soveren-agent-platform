"""Strict JSON decision registry for LLM planner output."""
from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, ConfigDict, ValidationError


class DecisionParseError(ValueError):
    """Raised when LLM output is not a clean JSON object."""


class UnknownDecisionKindError(DecisionParseError):
    """Raised when the JSON `kind` is not registered by the app."""


class DecisionValidationError(DecisionParseError):
    """Raised when JSON matches a registered kind but fails its schema."""


class BaseDecision(BaseModel):
    """Base class for app-defined decisions.

    App schemas can subclass this and narrow `kind` with `Literal[...]`.
    Extra fields are forbidden by default so planner prompts stay contractual.
    """

    model_config = ConfigDict(extra="forbid")

    kind: str

    @property
    def payload(self) -> dict[str, Any]:
        return self.model_dump(exclude={"kind"})


class DecisionRegistry:
    """Registry of app-provided decision schemas keyed by `kind`."""

    def __init__(self) -> None:
        self._models: dict[str, type[BaseModel]] = {}

    def register(self, kind: str, model: type[BaseModel]) -> None:
        if not kind or not isinstance(kind, str):
            raise ValueError("decision kind must be a non-empty string")
        if kind in self._models:
            raise ValueError(f"decision kind already registered: {kind!r}")
        if not issubclass(model, BaseModel):
            raise TypeError("decision model must subclass pydantic.BaseModel")
        self._models[kind] = model

    def parse(self, raw_text: str) -> BaseModel:
        data = self.parse_json_object(raw_text)
        kind = data.get("kind")
        if not isinstance(kind, str) or not kind:
            raise DecisionParseError("decision JSON must contain a non-empty string field `kind`")
        model = self._models.get(kind)
        if model is None:
            raise UnknownDecisionKindError(f"unknown decision kind: {kind!r}")
        try:
            decision = model.model_validate(data)
        except ValidationError as exc:
            raise DecisionValidationError(str(exc)) from exc
        parsed_kind = getattr(decision, "kind", None)
        if parsed_kind != kind:
            raise DecisionValidationError(
                f"decision model returned kind={parsed_kind!r}, expected {kind!r}"
            )
        return decision

    def registered_kinds(self) -> tuple[str, ...]:
        return tuple(sorted(self._models))

    @staticmethod
    def parse_json_object(raw_text: str) -> dict[str, Any]:
        text = raw_text.strip()
        if not text:
            raise DecisionParseError("empty planner output")
        if text.startswith("```") or text.endswith("```"):
            raise DecisionParseError("planner output must be raw JSON, not a code fence")
        if not text.startswith("{") or not text.endswith("}"):
            raise DecisionParseError("planner output must be a single JSON object")
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise DecisionParseError(f"invalid decision JSON: {exc.msg}") from exc
        if not isinstance(parsed, dict):
            raise DecisionParseError("planner output must be a JSON object")
        return parsed

