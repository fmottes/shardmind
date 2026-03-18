"""Canonical typed-object storage service."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile

from shardmind.errors import (
    DuplicateObjectError,
    InvalidInputError,
    NotFoundError,
    WriteFailedError,
)
from shardmind.index.service import IndexService
from shardmind.models import (
    Note,
    NoteProvenance,
    NoteSections,
    ObjectRecord,
    PaperCard,
    PaperCardProvenance,
    PaperCardSections,
)
from shardmind.paper_cards import ENRICHABLE_PAPER_CARD_SECTIONS
from shardmind.schemas import SchemaStore
from shardmind.vault.bootstrap import bootstrap_vault
from shardmind.vault.ids import note_id, paper_card_id, slugify
from shardmind.vault.markdown import (
    parse_note,
    parse_object,
    parse_paper_card,
    render_note,
    render_paper_card,
)

DESTINATIONS = {"inbox", "scratch", "daily"}
SAFE_PAPER_CARD_METADATA_FIELDS = {
    "authors",
    "year",
    "source",
    "url",
    "citekey",
    "tags",
    "status",
}


class VaultService:
    def __init__(
        self,
        vault_path: Path,
        schema_store: SchemaStore,
        index: IndexService | None = None,
    ):
        self.vault_path = vault_path
        self.schema_store = schema_store
        self.index = index
        bootstrap_vault(vault_path)

    def create_note(
        self,
        content: str,
        title: str | None = None,
        destination: str | None = None,
        tags: list[str] | None = None,
        created_from: str = "mcp",
    ) -> tuple[Note, str]:
        if not content.strip():
            raise InvalidInputError("Note content must not be empty.")
        destination_name = self._normalize_destination(destination)
        now = self._now()
        normalized_title = (title or self._title_from_content(content)).strip()
        note = Note(
            id=note_id(normalized_title, timestamp=now),
            title=normalized_title,
            tags=list(tags or []),
            provenance=NoteProvenance(created_from=created_from),
            created_at=self._timestamp(now),
            updated_at=self._timestamp(now),
            sections=NoteSections(content=content.strip()),
        )
        self.schema_store.validate_note(note)
        relative_path = f"notes/{destination_name}/{slugify(normalized_title)}.md"
        self._write_object(relative_path, render_note(note))
        self.log_write("knowledge.create_note", note.id, "create", True, relative_path)
        return note, relative_path

    def create_paper_card(
        self,
        *,
        title: str | None = None,
        authors: list[str] | None = None,
        year: int | None = None,
        url: str | None = None,
        source_text: str | None = None,
        tags: list[str] | None = None,
        created_from: str = "mcp",
    ) -> tuple[PaperCard, str]:
        if not any(value for value in (title, url, source_text)):
            raise InvalidInputError("At least one of title, url, or source_text must be provided.")
        canonical_title = (title or self._paper_card_title(source_text, url)).strip()
        duplicate_of = self._duplicate_paper_card_id(canonical_title, url or "", "")
        if duplicate_of is not None:
            raise DuplicateObjectError("A paper card with matching title or URL already exists.")
        now = self._now()
        object_id = paper_card_id(canonical_title, self._existing_paper_card_ids())
        paper_card = PaperCard(
            id=object_id,
            title=canonical_title,
            authors=list(authors or []),
            year=year,
            url=url or "",
            tags=list(tags or []),
            provenance=PaperCardProvenance(created_from=created_from),
            created_at=self._timestamp(now),
            updated_at=self._timestamp(now),
            sections=PaperCardSections(source_notes=(source_text or "").strip()),
        )
        self.schema_store.validate_paper_card(paper_card)
        relative_path = f"library/papers/{object_id.removeprefix('paper-')}.md"
        self._write_object(relative_path, render_paper_card(paper_card))
        self.log_write("knowledge.create_paper_card", paper_card.id, "create", True, relative_path)
        return paper_card, relative_path

    def append_to_note(
        self,
        note_id_value: str,
        content: str,
        section: str | None = None,
    ) -> tuple[Note, str]:
        if section not in (None, "", "content", "Content"):
            raise InvalidInputError("Milestone 2 only supports appending to the Content section.")
        note, relative_path = self.read_note(note_id_value)
        appended = content.strip()
        if not appended:
            raise InvalidInputError("Append content must not be empty.")
        existing = note.sections.content.rstrip()
        note.sections.content = f"{existing}\n\n{appended}".strip()
        note.updated_at = self._timestamp(self._now())
        self.schema_store.validate_note(note)
        self._write_object(relative_path, render_note(note))
        self.log_write("knowledge.append_to_note", note.id, "append", True, relative_path)
        return note, relative_path

    def update_paper_card_sections(
        self,
        paper_card_id_value: str,
        *,
        sections: dict[str, str] | None = None,
        metadata: dict[str, object] | None = None,
        mode: str = "fill-empty",
    ) -> tuple[PaperCard, str]:
        if mode not in {"fill-empty", "refresh"}:
            raise InvalidInputError("mode must be one of: fill-empty, refresh.")
        paper_card, relative_path = self.read_paper_card(paper_card_id_value)
        changed = False
        llm_sections_changed = False
        for section_name, value in (sections or {}).items():
            if section_name not in ENRICHABLE_PAPER_CARD_SECTIONS:
                raise InvalidInputError(f"Unsupported paper card section '{section_name}'.")
            if not isinstance(value, str):
                raise InvalidInputError("Paper card section patches must be strings.")
            current_value = getattr(paper_card.sections, section_name)
            next_value = self._merge_field(current_value, value.strip(), mode)
            if next_value != current_value:
                setattr(paper_card.sections, section_name, next_value)
                changed = True
                llm_sections_changed = True
        for field_name, value in (metadata or {}).items():
            if field_name not in SAFE_PAPER_CARD_METADATA_FIELDS:
                raise InvalidInputError(f"Unsupported paper card metadata field '{field_name}'.")
            normalized_value = self._normalize_metadata_value(field_name, value)
            current_value = getattr(paper_card, field_name)
            next_value = self._merge_field(current_value, normalized_value, mode)
            if next_value != current_value:
                setattr(paper_card, field_name, next_value)
                changed = True
        if changed:
            if llm_sections_changed:
                paper_card.provenance.llm_enriched = True
            paper_card.updated_at = self._timestamp(self._now())
            self.schema_store.validate_paper_card(paper_card)
            self._write_object(relative_path, render_paper_card(paper_card))
        self.log_write(
            "knowledge.enrich_paper_card",
            paper_card.id,
            "update",
            True,
            relative_path,
        )
        return paper_card, relative_path

    def read_object(self, object_id: str) -> tuple[ObjectRecord, str]:
        if self.index is not None:
            indexed_path = self.index.get_path(object_id)
            if indexed_path:
                indexed_record = self._read_from_relative_path(indexed_path)
                if indexed_record is not None and indexed_record[0].id == object_id:
                    return indexed_record
        scanned = self._scan_for_object(object_id)
        if scanned is not None:
            if self.index is not None:
                self.index.reindex_object(scanned[0], scanned[1])
            return scanned
        if self.index is not None and self.index.get_path(object_id) is not None:
            self.index.remove_object(object_id)
        raise NotFoundError(f"No object found for id '{object_id}'.")

    def read_note(self, note_id_value: str) -> tuple[Note, str]:
        record, relative_path = self.read_object(note_id_value)
        if isinstance(record, Note):
            return record, relative_path
        raise NotFoundError(f"No object found for id '{note_id_value}'.")

    def read_paper_card(self, paper_card_id_value: str) -> tuple[PaperCard, str]:
        record, relative_path = self.read_object(paper_card_id_value)
        if isinstance(record, PaperCard):
            return record, relative_path
        raise NotFoundError(f"No object found for id '{paper_card_id_value}'.")

    def list_notes(self, path_scope: str | None = None) -> list[tuple[Note, str]]:
        results: list[tuple[Note, str]] = []
        for path in self._note_paths():
            relative_path = path.relative_to(self.vault_path).as_posix()
            if path_scope and not relative_path.startswith(path_scope):
                continue
            results.append((parse_note(path.read_text(encoding="utf-8")), relative_path))
        results.sort(key=lambda item: item[0].updated_at, reverse=True)
        return results

    def list_objects(self) -> list[tuple[ObjectRecord, str]]:
        return [
            (
                parse_object(path.read_text(encoding="utf-8")),
                path.relative_to(self.vault_path).as_posix(),
            )
            for path in self._object_paths()
        ]

    def log_write(
        self,
        tool_name: str,
        object_id: str,
        operation: str,
        success: bool,
        path: str,
    ) -> None:
        log_path = self.vault_path / "system" / "logs" / "operations.log"
        event = {
            "timestamp": self._timestamp(self._now()),
            "tool_name": tool_name,
            "object_id": object_id,
            "operation": operation,
            "success": success,
            "path": path,
        }
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True) + "\n")

    def _note_paths(self) -> list[Path]:
        return sorted((self.vault_path / "notes").glob("*/*.md"))

    def _paper_card_paths(self) -> list[Path]:
        return sorted((self.vault_path / "library" / "papers").glob("*.md"))

    def _object_paths(self) -> list[Path]:
        return sorted([*self._note_paths(), *self._paper_card_paths()])

    def _scan_for_object(self, object_id: str) -> tuple[ObjectRecord, str] | None:
        for path in self._object_paths():
            record = parse_object(path.read_text(encoding="utf-8"))
            if record.id == object_id:
                return record, path.relative_to(self.vault_path).as_posix()
        return None

    def _read_from_relative_path(self, relative_path: str) -> tuple[ObjectRecord, str] | None:
        target = self.vault_path / relative_path
        try:
            payload = target.read_text(encoding="utf-8")
        except OSError:
            return None
        try:
            return parse_object(payload), relative_path
        except ValueError:
            return None

    def _existing_paper_card_ids(self) -> set[str]:
        existing_ids = {card.id for card, _ in self._paper_card_records()}
        if self.index is not None:
            existing_ids.update(self.index.existing_paper_card_ids())
        return existing_ids

    def _paper_card_records(self) -> list[tuple[PaperCard, str]]:
        results: list[tuple[PaperCard, str]] = []
        for path in self._paper_card_paths():
            relative_path = path.relative_to(self.vault_path).as_posix()
            results.append((parse_paper_card(path.read_text(encoding="utf-8")), relative_path))
        return results

    def _duplicate_paper_card_id(self, title: str, url: str, citekey: str) -> str | None:
        normalized_title = slugify(title) if title else ""
        if self.index is not None:
            duplicate = self.index.find_duplicate_paper_card(
                normalized_title=normalized_title,
                url=url,
                citekey=citekey,
            )
            if duplicate is not None:
                return duplicate
        for paper_card, _ in self._paper_card_records():
            if normalized_title and slugify(paper_card.title) == normalized_title:
                return paper_card.id
            if url and paper_card.url and paper_card.url == url:
                return paper_card.id
            if citekey and paper_card.citekey and paper_card.citekey == citekey:
                return paper_card.id
        return None

    def _normalize_destination(self, destination: str | None) -> str:
        candidate = (destination or "inbox").strip().lower()
        if candidate not in DESTINATIONS:
            raise InvalidInputError(f"Unsupported note destination '{candidate}'.")
        return candidate

    def _title_from_content(self, content: str) -> str:
        first_line = next(
            (line.strip() for line in content.splitlines() if line.strip()), "Untitled note"
        )
        return first_line[:80]

    def _paper_card_title(self, source_text: str | None, url: str | None) -> str:
        if source_text:
            return self._title_from_content(source_text)
        if url:
            return url.strip()
        return "Untitled paper card"

    def _write_object(self, relative_path: str, payload: str) -> None:
        target = self.vault_path / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            with NamedTemporaryFile("w", encoding="utf-8", dir=target.parent, delete=False) as tmp:
                tmp.write(payload)
                tmp_path = Path(tmp.name)
            tmp_path.replace(target)
        except OSError as exc:
            raise WriteFailedError(f"Could not write object to '{relative_path}'.") from exc

    def _timestamp(self, value: datetime) -> str:
        return value.isoformat().replace("+00:00", "Z")

    def _merge_field(self, current_value: object, new_value: object, mode: str) -> object:
        if mode == "refresh":
            return new_value
        if self._is_empty(current_value):
            return new_value
        return current_value

    def _normalize_metadata_value(self, field_name: str, value: object) -> object:
        if field_name in {"authors", "tags"}:
            if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
                raise InvalidInputError(f"{field_name} must be a list of strings.")
            return value
        if field_name == "year":
            if value is None or isinstance(value, int):
                return value
            raise InvalidInputError("year must be an integer or null.")
        if field_name == "status":
            if not isinstance(value, str):
                raise InvalidInputError("status must be a string.")
            return value
        if not isinstance(value, str):
            raise InvalidInputError(f"{field_name} must be a string.")
        return value.strip()

    def _is_empty(self, value: object) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            return value.strip() == ""
        if isinstance(value, list):
            return len(value) == 0
        return False

    def _now(self) -> datetime:
        return datetime.now(UTC)
