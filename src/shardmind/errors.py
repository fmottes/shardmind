"""Domain errors and MCP-style error mapping."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class ShardMindError(Exception):
    code: str
    message: str

    def to_response(self) -> dict[str, object]:
        return {"ok": False, "error": {"code": self.code, "message": self.message}}

    def __str__(self) -> str:
        return self.message


class InvalidInputError(ShardMindError):
    def __init__(self, message: str):
        super().__init__("INVALID_INPUT", message)


class NotFoundError(ShardMindError):
    def __init__(self, message: str):
        super().__init__("OBJECT_NOT_FOUND", message)


class WriteFailedError(ShardMindError):
    def __init__(self, message: str):
        super().__init__("WRITE_FAILED", message)


class SchemaValidationError(ShardMindError):
    def __init__(self, message: str):
        super().__init__("SCHEMA_VALIDATION_FAILED", message)


class DuplicateObjectError(ShardMindError):
    def __init__(self, message: str):
        super().__init__("DUPLICATE_OBJECT", message)


class InternalError(ShardMindError):
    def __init__(self, message: str = "Unexpected server error."):
        super().__init__("INTERNAL_ERROR", message)
