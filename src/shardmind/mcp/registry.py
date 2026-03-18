"""Single source of truth for the MCP tool surface."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from shardmind.errors import InvalidInputError


@dataclass(frozen=True)
class ToolSpec:
    method_name: str
    exported_name: str
    aliases: tuple[str, ...]

    def all_names(self) -> tuple[str, ...]:
        return (self.exported_name, *self.aliases)


def tool_spec(
    exported_name: str, *aliases: str
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        fn.__tool_spec__ = ToolSpec(  # type: ignore[attr-defined]
            method_name=fn.__name__,
            exported_name=exported_name,
            aliases=aliases,
        )
        return fn

    return decorator


def iter_tool_specs(tool_cls: type) -> tuple[ToolSpec, ...]:
    specs: list[ToolSpec] = []
    for _, member in inspect.getmembers(tool_cls):
        spec = getattr(member, "__tool_spec__", None)
        if isinstance(spec, ToolSpec):
            specs.append(spec)
    return tuple(sorted(specs, key=lambda spec: spec.exported_name))


def dispatch_table(tools: object) -> dict[str, Callable[..., Any]]:
    mapping: dict[str, Callable[..., Any]] = {}
    for spec in iter_tool_specs(type(tools)):
        method = getattr(tools, spec.method_name)
        for name in spec.all_names():
            mapping[name] = method
    return mapping


def invoke_registered_tool(tools: object, tool_name: str, payload: dict[str, Any]) -> Any:
    method = dispatch_table(tools).get(tool_name)
    if method is None:
        raise InvalidInputError(f"Unsupported tool '{tool_name}'.")
    signature = inspect.signature(method)
    try:
        bound = signature.bind(**payload)
    except TypeError as exc:
        raise InvalidInputError(str(exc)) from exc
    return method(*bound.args, **bound.kwargs)
