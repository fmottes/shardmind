"""Tool implementations with MCP-style response envelopes."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import Field

from shardmind.errors import InvalidInputError, ShardMindError
from shardmind.index.service import IndexService
from shardmind.mcp.registry import invoke_registered_tool, tool_spec
from shardmind.models import SearchResult, path_reference_fields, titled_fields
from shardmind.vault.service import VaultService

WIKILINK_GUIDANCE = (
    "When another vault file is relevant or mentioned, reference it inline with an Obsidian "
    "wikilink using the file stem returned by retrieval, for example "
    "[[memory-architecture-idea--1a2b3c4d]]. Do not use frontmatter title as the link target."
)


class KnowledgeTools:
    def __init__(self, vault: VaultService, index: IndexService):
        self.vault = vault
        self.index = index

    @tool_spec("knowledge_create_note", "knowledge.create_note")
    def create_note(
        self,
        content: Annotated[str, Field(description=WIKILINK_GUIDANCE)],
        title: Annotated[
            str | None,
            Field(
                description="Optional note title. If omitted, the server derives one from content."
            ),
        ] = None,
        destination: Annotated[
            Literal["inbox", "scratch", "daily"] | None,
            Field(description="Optional destination folder inside notes/."),
        ] = None,
        tags: Annotated[list[str] | None, Field(description="Optional note tags.")] = None,
    ) -> dict[str, object]:
        """Create a deterministic note from freeform text."""
        try:
            self._require_non_empty_string(content, "content")
            note, path = self.vault.create_note(
                title=title,
                content=content,
                destination=destination,
                tags=tags,
            )
            return {
                "ok": True,
                "result": {
                    "id": note.id,
                    "type": note.type,
                    "path": path,
                    "note_title": note.title,
                    "created_at": note.created_at,
                },
            }
        except ShardMindError as exc:
            return exc.to_response()

    @tool_spec("knowledge_create_paper_card", "knowledge.create_paper_card")
    def create_paper_card(
        self,
        title: Annotated[str | None, Field(description="Optional paper title.")] = None,
        authors: Annotated[
            list[str] | None,
            Field(description="Optional ordered author list."),
        ] = None,
        year: Annotated[int | None, Field(description="Optional publication year.")] = None,
        url: Annotated[str | None, Field(description="Optional canonical paper URL.")] = None,
        citekey: Annotated[
            str | None,
            Field(
                description=(
                    "Optional Better BibTeX-style citekey in lowercase authorYearTitleword "
                    "format, for example mottes2026gradient."
                )
            ),
        ] = None,
        notes: Annotated[
            str | None,
            Field(description=f"Optional paper notes, abstract, or excerpts. {WIKILINK_GUIDANCE}"),
        ] = None,
        tags: Annotated[list[str] | None, Field(description="Optional paper tags.")] = None,
    ) -> dict[str, object]:
        """Create a sparse deterministic paper card without server-side LLM generation."""
        try:
            if not any((title, url, notes)):
                raise InvalidInputError("At least one of title, url, or notes must be provided.")
            paper_card, path = self.vault.create_paper_card(
                title=title,
                authors=authors,
                year=year,
                url=url,
                citekey=citekey,
                notes=notes,
                tags=tags,
            )
            return {
                "ok": True,
                "result": {
                    "id": paper_card.id,
                    "type": paper_card.type,
                    "path": path,
                    "paper_title": paper_card.title,
                    "created_at": paper_card.created_at,
                    "duplicate_of": None,
                },
            }
        except ShardMindError as exc:
            return exc.to_response()

    @tool_spec("knowledge_append_to_note", "knowledge.append_to_note")
    def append_to_note(
        self,
        id: str,  # noqa: A002
        content: Annotated[str, Field(description=WIKILINK_GUIDANCE)],
        section: Annotated[
            str | None,
            Field(description="Optional section name. Milestone 2 only supports Content."),
        ] = None,
    ) -> dict[str, object]:
        """Append content to the canonical Content section of an existing note."""
        try:
            self._require_non_empty_string(id, "id")
            self._require_non_empty_string(content, "content")
            note, path = self.vault.append_to_note(
                note_id_value=id,
                content=content,
                section=section,
            )
            return {
                "ok": True,
                "result": {
                    "id": note.id,
                    "type": note.type,
                    "path": path,
                    "updated_at": note.updated_at,
                },
            }
        except ShardMindError as exc:
            return exc.to_response()

    @tool_spec("knowledge_edit_paper_card", "knowledge.edit_paper_card")
    def edit_paper_card(
        self,
        id: str,  # noqa: A002
        sections: Annotated[
            dict[str, str] | None,
            Field(
                description=(
                    "Optional patches for supported paper-card sections except user_notes. "
                    f"{WIKILINK_GUIDANCE}"
                )
            ),
        ] = None,
        metadata: Annotated[
            dict[str, object] | None,
            Field(
                description=(
                    "Optional metadata patches. If citekey is provided, use lowercase "
                    "authorYearTitleword format such as mottes2026gradient."
                )
            ),
        ] = None,
        mode: Annotated[
            Literal["fill-empty", "refresh"] | None,
            Field(description="Patch mode. fill-empty preserves existing non-empty values."),
        ] = None,
    ) -> dict[str, object]:
        """Edit supported sections and metadata on an existing paper card."""
        try:
            self._require_non_empty_string(id, "id")
            next_mode = mode or "fill-empty"
            next_sections = self._optional_dict(sections, "sections")
            next_metadata = self._optional_dict(metadata, "metadata")
            if not next_sections and not next_metadata:
                raise InvalidInputError("At least one of sections or metadata must be provided.")
            paper_card, path = self.vault.update_paper_card_sections(
                id,
                sections=next_sections,
                metadata=next_metadata,
                mode=next_mode,
            )
            return {
                "ok": True,
                "result": {
                    "id": paper_card.id,
                    "type": paper_card.type,
                    "path": path,
                    "updated_at": paper_card.updated_at,
                    "mode": next_mode,
                },
            }
        except ShardMindError as exc:
            return exc.to_response()

    @tool_spec("knowledge_get_object", "knowledge.get_object")
    def get_object(self, id: str) -> dict[str, object]:  # noqa: A002
        try:
            self._require_non_empty_string(id, "id")
            record, path = self.vault.read_object(id)
            return {"ok": True, "result": record.to_document(path)}
        except ShardMindError as exc:
            return exc.to_response()

    @tool_spec("knowledge_list_objects", "knowledge.list_objects")
    def list_objects(
        self,
        object_type: Literal["note", "paper-card"] | None = None,
        path_scope: str | None = None,
        limit: Annotated[int, Field(ge=1, le=200)] = 50,
    ) -> dict[str, object]:
        try:
            objects = self._list_live_objects(
                object_type=object_type,
                path_scope=path_scope,
                limit=limit,
            )
            return {"ok": True, "result": {"objects": objects}}
        except ShardMindError as exc:
            return exc.to_response()

    @tool_spec("knowledge_search", "knowledge.search")
    def search(
        self,
        query: str,
        object_types: list[Literal["note", "paper-card"]] | None = None,
        path_scope: str | None = None,
        top_k: Annotated[int, Field(ge=1, le=50)] = 10,
        tags: list[str] | None = None,
    ) -> dict[str, object]:
        try:
            self._require_non_empty_string(query, "query")
            results = self._search_live_results(
                query=query,
                object_types=object_types,
                path_scope=path_scope,
                top_k=top_k,
                tags=tags,
            )
            return {
                "ok": True,
                "result": {
                    "query": query,
                    "results": [result.to_dict() for result in results],
                    "top_k": top_k,
                },
            }
        except ShardMindError as exc:
            return exc.to_response()

    def invoke(self, tool_name: str, payload: dict[str, Any]) -> dict[str, object]:
        try:
            return invoke_registered_tool(self, tool_name, payload)
        except ShardMindError as exc:
            return exc.to_response()

    def _require_non_empty_string(self, value: object, field_name: str) -> None:
        if not isinstance(value, str) or not value.strip():
            raise InvalidInputError(f"{field_name} must be a non-empty string.")

    def _optional_dict(self, value: object, field_name: str) -> dict[str, Any]:
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise InvalidInputError(f"{field_name} must be an object.")
        return value

    def _list_live_objects(
        self,
        *,
        object_type: Literal["note", "paper-card"] | None,
        path_scope: str | None,
        limit: int,
    ) -> list[dict[str, object]]:
        while True:
            stale_found = False
            live_objects: list[dict[str, object]] = []
            indexed_objects = self.index.list_objects(
                object_type=object_type,
                path_scope=path_scope,
                limit=limit,
            )
            for candidate in indexed_objects:
                resolved = self.vault.reconcile_index_entry(
                    str(candidate["id"]),
                    str(candidate["path"]),
                )
                if resolved is None:
                    stale_found = True
                    continue
                record, path = resolved
                live_objects.append(
                    {
                        "id": record.id,
                        "type": record.type,
                        "path": path,
                        "updated_at": record.updated_at,
                        **titled_fields(record.type, record.title),
                        **path_reference_fields(path),
                    }
                )
            if len(live_objects) >= limit or not stale_found:
                return live_objects[:limit]

    def _search_live_results(
        self,
        *,
        query: str,
        object_types: list[Literal["note", "paper-card"]] | None,
        path_scope: str | None,
        top_k: int,
        tags: list[str] | None,
    ) -> list[SearchResult]:
        while True:
            stale_found = False
            live_results: list[SearchResult] = []
            indexed_results = self.index.search(
                query=query,
                object_types=object_types,
                path_scope=path_scope,
                top_k=top_k,
                tags=tags,
            )
            for candidate in indexed_results:
                resolved = self.vault.reconcile_index_entry(candidate.id, candidate.path)
                if resolved is None:
                    stale_found = True
                    continue
                record, path = resolved
                candidate.path = path
                candidate.title = record.title
                candidate.type = record.type
                live_results.append(candidate)
            if len(live_results) >= top_k or not stale_found:
                return live_results[:top_k]
