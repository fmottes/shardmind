"""Canonical typed-object storage service."""

from __future__ import annotations

import json
import re
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
from shardmind.vault.ids import note_id, paper_card_id, short_id, slugify
from shardmind.vault.markdown import (
    parse_object,
    parse_paper_card,
    render_note,
    render_paper_card,
)

DESTINATIONS = {"inbox", "scratch", "daily"}
CITEKEY_PATTERN = re.compile(r"^[a-z]+[0-9]{4}[a-z0-9]+$")
SAFE_PAPER_CARD_METADATA_FIELDS = {
    "authors",
    "year",
    "source",
    "url",
    "citekey",
    "tags",
    "status",
}
SAFE_NOTE_METADATA_FIELDS = {"title", "tags"}
EDIT_MODES = {"fill-empty", "refresh"}


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
        object_id = note_id()
        note = Note(
            id=object_id,
            title=normalized_title,
            tags=list(tags or []),
            provenance=NoteProvenance(created_from=created_from),
            created_at=self._timestamp(now),
            updated_at=self._timestamp(now),
            sections=NoteSections(content=content.strip()),
        )
        self.schema_store.validate_note(note)
        relative_path = (
            f"notes/{destination_name}/{self._object_stem(normalized_title, object_id)}.md"
        )
        self._write_object(relative_path, render_note(note))
        self._reindex_if_available(note, relative_path)
        self.log_write("knowledge.create_note", note.id, "create", True, relative_path)
        return note, relative_path

    def create_paper_card(
        self,
        *,
        title: str | None = None,
        authors: list[str] | None = None,
        year: int | None = None,
        url: str | None = None,
        citekey: str | None = None,
        notes: str | None = None,
        tags: list[str] | None = None,
        created_from: str = "mcp",
    ) -> tuple[PaperCard, str]:
        if not any(value for value in (title, url, notes)):
            raise InvalidInputError("At least one of title, url, or notes must be provided.")
        canonical_title = (title or self._paper_card_title(notes, url)).strip()
        normalized_citekey = self._normalize_citekey(citekey)
        duplicate_of = self._duplicate_paper_card_id(
            canonical_title,
            (url or "").strip(),
            normalized_citekey,
        )
        if duplicate_of is not None:
            raise DuplicateObjectError(
                "A paper card with matching title, URL, or citekey already exists."
            )
        now = self._now()
        object_id = paper_card_id()
        paper_card = PaperCard(
            id=object_id,
            title=canonical_title,
            authors=list(authors or []),
            year=year,
            url=(url or "").strip(),
            citekey=normalized_citekey,
            tags=list(tags or []),
            provenance=PaperCardProvenance(created_from=created_from),
            created_at=self._timestamp(now),
            updated_at=self._timestamp(now),
            sections=PaperCardSections(notes=(notes or "").strip()),
        )
        self.schema_store.validate_paper_card(paper_card)
        relative_path = (
            f"library/papers/"
            f"{self._object_stem(normalized_citekey or canonical_title, object_id)}.md"
        )
        self._write_object(relative_path, render_paper_card(paper_card))
        self._reindex_if_available(paper_card, relative_path)
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
        self._reindex_if_available(note, relative_path)
        self.log_write("knowledge.append_to_note", note.id, "append", True, relative_path)
        return note, relative_path

    def update_note(
        self,
        note_id_value: str,
        *,
        sections: dict[str, str] | None = None,
        metadata: dict[str, object] | None = None,
        mode: str = "refresh",
    ) -> tuple[Note, str]:
        if mode not in EDIT_MODES:
            raise InvalidInputError("mode must be one of: fill-empty, refresh.")
        note, relative_path = self.read_note(note_id_value)
        changed = False
        for section_name, value in (sections or {}).items():
            if section_name != "content":
                raise InvalidInputError(f"Unsupported note section '{section_name}'.")
            if not isinstance(value, str):
                raise InvalidInputError("Note section patches must be strings.")
            current_value = note.sections.content
            next_value = self._merge_field(current_value, value.strip(), mode)
            if next_value != current_value:
                note.sections.content = str(next_value)
                changed = True
        for field_name, value in (metadata or {}).items():
            if field_name not in SAFE_NOTE_METADATA_FIELDS:
                raise InvalidInputError(f"Unsupported note metadata field '{field_name}'.")
            normalized_value = self._normalize_note_metadata_value(field_name, value)
            current_value = getattr(note, field_name)
            next_value = self._merge_field(current_value, normalized_value, mode)
            if next_value != current_value:
                setattr(note, field_name, next_value)
                changed = True
        if changed:
            note.updated_at = self._timestamp(self._now())
            self.schema_store.validate_note(note)
            self._write_object(relative_path, render_note(note))
            self._reindex_if_available(note, relative_path)
        self.log_write(
            "knowledge.edit_note",
            note.id,
            "update",
            True,
            relative_path,
        )
        return note, relative_path

    def update_paper_card_sections(
        self,
        paper_card_id_value: str,
        *,
        sections: dict[str, str] | None = None,
        metadata: dict[str, object] | None = None,
        mode: str = "fill-empty",
    ) -> tuple[PaperCard, str]:
        if mode not in EDIT_MODES:
            raise InvalidInputError("mode must be one of: fill-empty, refresh.")
        paper_card, relative_path = self.read_paper_card(paper_card_id_value)
        changed = False
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
        for field_name, value in (metadata or {}).items():
            if field_name not in SAFE_PAPER_CARD_METADATA_FIELDS:
                raise InvalidInputError(f"Unsupported paper card metadata field '{field_name}'.")
            normalized_value = self._normalize_paper_card_metadata_value(field_name, value)
            current_value = getattr(paper_card, field_name)
            next_value = self._merge_field(current_value, normalized_value, mode)
            if next_value != current_value:
                setattr(paper_card, field_name, next_value)
                changed = True
        if changed:
            paper_card.updated_at = self._timestamp(self._now())
            self.schema_store.validate_paper_card(paper_card)
            self._write_object(relative_path, render_paper_card(paper_card))
            self._reindex_if_available(paper_card, relative_path)
        self.log_write(
            "knowledge.edit_paper_card",
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
                reconciled = self.reconcile_index_entry(object_id, indexed_path)
                if reconciled is not None:
                    return reconciled
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

    def reconcile_index_entry(
        self,
        object_id: str,
        relative_path: str,
    ) -> tuple[ObjectRecord, str] | None:
        indexed_record = self._read_from_relative_path(relative_path)
        if indexed_record is not None:
            if indexed_record[0].id == object_id:
                return indexed_record
            self._reindex_if_available(indexed_record[0], indexed_record[1])
        scanned = self._scan_for_object(object_id)
        if scanned is not None:
            self._reindex_if_available(scanned[0], scanned[1])
            return scanned
        if self.index is not None:
            self.index.remove_object(object_id)
        return None

    def list_objects_strict(self) -> list[tuple[ObjectRecord, str]]:
        """List all vault objects and fail fast on malformed files."""
        return [
            (
                parse_object(path.read_text(encoding="utf-8")),
                path.relative_to(self.vault_path).as_posix(),
            )
            for path in self._object_paths()
        ]

    def list_objects(self) -> list[tuple[ObjectRecord, str]]:
        """Backward-compatible alias for strict full-vault parsing."""
        return self.list_objects_strict()

    def list_indexable_objects(self) -> tuple[list[tuple[ObjectRecord, str]], list[str]]:
        records: list[tuple[ObjectRecord, str]] = []
        skipped_paths: list[str] = []
        for path in self._object_paths():
            parsed = self._safe_parse_object_path(path)
            if parsed is None:
                skipped_paths.append(path.relative_to(self.vault_path).as_posix())
                continue
            records.append(parsed)
        return records, skipped_paths

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
        try:
            with log_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(event, sort_keys=True) + "\n")
        except OSError:
            # Logging is best-effort; a log failure must not turn a completed write into an error.
            return

    def _note_paths(self) -> list[Path]:
        return sorted((self.vault_path / "notes").glob("*/*.md"))

    def _paper_card_paths(self) -> list[Path]:
        return sorted((self.vault_path / "library" / "papers").glob("*.md"))

    def _object_paths(self) -> list[Path]:
        return sorted([*self._note_paths(), *self._paper_card_paths()])

    def _scan_for_object(self, object_id: str) -> tuple[ObjectRecord, str] | None:
        for path in self._object_paths():
            parsed = self._safe_parse_object_path(path)
            if parsed is None:
                continue
            record, relative_path = parsed
            if record.id == object_id:
                return record, relative_path
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

    def _paper_card_records(self) -> list[tuple[PaperCard, str]]:
        results: list[tuple[PaperCard, str]] = []
        for path in self._paper_card_paths():
            parsed = self._safe_parse_paper_card_path(path)
            if parsed is not None:
                results.append(parsed)
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
                indexed_path = self.index.get_path(duplicate)
                if indexed_path is not None:
                    resolved = self.reconcile_index_entry(duplicate, indexed_path)
                    if resolved is not None:
                        return duplicate
        for paper_card, _ in self._paper_card_records():
            if normalized_title and slugify(paper_card.title) == normalized_title:
                return paper_card.id
            if url and paper_card.url and paper_card.url == url:
                return paper_card.id
            if citekey and paper_card.citekey and paper_card.citekey == citekey:
                return paper_card.id
        return None

    def _safe_parse_object_path(self, path: Path) -> tuple[ObjectRecord, str] | None:
        try:
            payload = path.read_text(encoding="utf-8")
        except OSError:
            return None
        try:
            record = parse_object(payload)
        except ValueError:
            return None
        return record, path.relative_to(self.vault_path).as_posix()

    def _safe_parse_paper_card_path(self, path: Path) -> tuple[PaperCard, str] | None:
        try:
            payload = path.read_text(encoding="utf-8")
        except OSError:
            return None
        try:
            record = parse_paper_card(payload)
        except ValueError:
            return None
        return record, path.relative_to(self.vault_path).as_posix()

    def _reindex_if_available(self, record: ObjectRecord, relative_path: str) -> None:
        if self.index is None:
            return
        self.index.reindex_object(record, relative_path)

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

    def _paper_card_title(self, notes: str | None, url: str | None) -> str:
        if notes:
            return self._title_from_content(notes)
        if url:
            return url.strip()
        return "Untitled paper card"

    def _normalize_citekey(self, value: object) -> str:
        if value is None:
            return ""
        if not isinstance(value, str):
            raise InvalidInputError("citekey must be a string.")
        normalized = value.strip()
        if normalized == "":
            return ""
        if not CITEKEY_PATTERN.fullmatch(normalized):
            raise InvalidInputError(
                "citekey must use lowercase authorYearTitleword format, for example "
                "'mottes2026gradient'."
            )
        return normalized

    def _object_stem(self, label: str, object_id: str) -> str:
        return f"{slugify(label)}--{short_id(object_id)}"

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

    def _normalize_note_metadata_value(self, field_name: str, value: object) -> object:
        if field_name == "title":
            if not isinstance(value, str):
                raise InvalidInputError("title must be a string.")
            return value.strip()
        if field_name == "tags":
            if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
                raise InvalidInputError("tags must be a list of strings.")
            return value
        raise InvalidInputError(f"Unsupported note metadata field '{field_name}'.")

    def _normalize_paper_card_metadata_value(self, field_name: str, value: object) -> object:
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
        if field_name == "citekey":
            return self._normalize_citekey(value)
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
