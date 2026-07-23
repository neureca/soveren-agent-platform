"""Strict JSON value types for durable platform boundaries."""

from __future__ import annotations

import math

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]
type JsonObject = dict[str, JsonValue]


def require_json_object(value: object, *, label: str) -> JsonObject:
    if not isinstance(value, dict):
        raise TypeError(f"{label} must be a JSON object")
    result: JsonObject = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise TypeError(f"{label} keys must be strings")
        result[key] = _require_json_value(item, label=f"{label}.{key}")
    return result


def _require_json_value(value: object, *, label: str) -> JsonValue:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{label} must be a finite JSON number")
        return value
    if isinstance(value, list):
        return [
            _require_json_value(item, label=f"{label}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, dict):
        return require_json_object(value, label=label)
    raise TypeError(f"{label} contains non-JSON value {type(value).__name__}")
