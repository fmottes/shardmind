"""Schema loading and validation."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from jsonschema import Draft202012Validator

from shardmind.errors import SchemaValidationError
from shardmind.models import Note, PaperCard


class SchemaStore:
    def __init__(self, shared_path: Path):
        self.shared_path = shared_path
        self._schemas: dict[str, dict[str, Any]] = {}
        self._validators: dict[str, Draft202012Validator] = {}

    def load(self, name: str) -> dict[str, Any]:
        if name not in self._schemas:
            schema_path = self.shared_path / "schemas" / f"{name}.schema.json"
            self._schemas[name] = json.loads(schema_path.read_text(encoding="utf-8"))
        return self._schemas[name]

    def _validator(self, name: str) -> Draft202012Validator:
        if name not in self._validators:
            self._validators[name] = Draft202012Validator(self.load(name))
        return self._validators[name]

    def validate_note(self, note: Note) -> None:
        payload = asdict(note)
        self._validate_payload("note", payload)
        self._validate_datetime("created_at", note.created_at)
        self._validate_datetime("updated_at", note.updated_at)

    def validate_paper_card(self, paper_card: PaperCard) -> None:
        payload = asdict(paper_card)
        self._validate_payload("paper_card", payload)
        self._validate_datetime("created_at", paper_card.created_at)
        self._validate_datetime("updated_at", paper_card.updated_at)
        self._validate_optional_uri("url", paper_card.url)

    def _validate_payload(self, name: str, payload: dict[str, Any]) -> None:
        validator = self._validator(name)
        errors = sorted(validator.iter_errors(payload), key=lambda error: list(error.absolute_path))
        if errors:
            first = errors[0]
            path = ".".join(str(part) for part in first.absolute_path)
            prefix = f"{path}: " if path else ""
            raise SchemaValidationError(f"{prefix}{first.message}")

    def _validate_datetime(self, field_name: str, value: str) -> None:
        candidate = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError as exc:
            raise SchemaValidationError(
                f"{field_name} must be an ISO 8601 date-time string."
            ) from exc
        if parsed.tzinfo is None:
            raise SchemaValidationError(f"{field_name} must include a timezone.")

    def _validate_optional_uri(self, field_name: str, value: str) -> None:
        if value == "":
            return
        parsed = urlparse(value)
        if not parsed.scheme:
            raise SchemaValidationError(f"{field_name} must be a valid URI.")
        if parsed.scheme in {"http", "https"} and not parsed.netloc:
            raise SchemaValidationError(f"{field_name} must be a valid URI.")
