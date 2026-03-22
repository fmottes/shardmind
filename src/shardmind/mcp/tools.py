"""Tool implementations with MCP-style response envelopes."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Annotated, Any, Literal

from pydantic import Field

from shardmind.errors import InternalError, InvalidInputError, ShardMindError
from shardmind.index.service import IndexService
from shardmind.mcp.registry import invoke_registered_tool, tool_spec
from shardmind.models import SearchResult, path_reference_fields, titled_fields
from shardmind.vault.service import VaultService

WIKILINK_GUIDANCE = (
    "When another vault file is relevant or mentioned, reference it inline with an Obsidian "
    "wikilink using the file stem returned by retrieval, for example "
    "[[memory-architecture-idea--1a2b3c4d]]. Do not use frontmatter title as the link target."
)
OBJECT_ID_GUIDANCE = (
    "Object id returned by create, list, search, or get tools. Use note-... for notes and "
    "paper-... for paper cards."
)
OBJECT_PATH_GUIDANCE = (
    "Vault-relative Markdown path. Notes may live under notes/, archive/, or library/ except "
    "library/papers/. Paper cards must stay under library/papers/. Paths under assets/ and "
    "system/ are rejected."
)
NOTE_CONTENT_GUIDANCE = (
    "Main note body for the # Content section. Use complete prose or bullets that should live "
    "in the note. "
    f"{WIKILINK_GUIDANCE}"
)
TAG_CREATION_GUIDANCE = (
    "Prefer existing tags and existing casing when known instead of inventing near-duplicate "
    "spellings. If you need to inspect the current tag vocabulary first, call shardmind.list_tags."
)
PAPER_CARD_SECTION_PATCH_GUIDANCE = (
    "Section patch object keyed by any of: summary, main_claims, why_relevant, limitations, "
    "notes, related_links. Intended use: summary=high-level takeaway in 2-4 sentences, "
    "main_claims=distinct core claims or findings, why_relevant=why this matters to your work, "
    "limitations=known caveats or missing evidence, notes=raw source snippets only, "
    "related_links=wikilinks or URLs. Use these structured sections for synthesized content "
    "instead of dumping everything into notes."
)
PAPER_CARD_CREATE_SECTIONS_GUIDANCE = (
    "Optional initial section object for create_paper_card using the same keys as "
    "shardmind_edit_paper_card: summary, main_claims, why_relevant, limitations, notes, "
    "related_links. Prefer putting synthesized content here during creation so the card is "
    "usable in one tool call. Use sections.notes for raw source capture only, such as abstract "
    "text, direct excerpts, bibliographic scraps, or stray observations. Do not put a "
    "synthesized paper summary, claim list, relevance rationale, limitations list, or a second "
    "mini-card in sections.notes. Do not include duplicate headings such as # Summary or ## Main "
    "Claims inside sections.notes. " + PAPER_CARD_SECTION_PATCH_GUIDANCE
)
PAPER_CARD_METADATA_PATCH_GUIDANCE = (
    "Metadata patch object using: authors (list[str]), year (int or null), source (str), "
    "url (str), citekey (str), tags (list[str]), status (str). If citekey is provided, use "
    "lowercase authorYearTitleword format such as mottes2026gradient."
)
NOTE_SECTION_PATCH_GUIDANCE = (
    "Section patch object for notes. Supported key: content. Use this to replace or seed the "
    "main # Content section."
)
NOTE_METADATA_PATCH_GUIDANCE = (
    "Metadata patch object for notes. Supported keys: title (str), tags (list[str])."
)
EDIT_MODE_GUIDANCE = (
    "Patch mode. fill-empty only writes into empty fields and preserves existing non-empty "
    "values. refresh replaces existing values."
)
LOGGER = logging.getLogger(__name__)


class KnowledgeTools:
    def __init__(self, vault: VaultService, index: IndexService):
        self.vault = vault
        self.index = index

    @tool_spec("shardmind_create_note", "shardmind.create_note")
    def create_note(
        self,
        content: Annotated[str, Field(description=NOTE_CONTENT_GUIDANCE)],
        title: Annotated[
            str | None,
            Field(
                description=(
                    "Optional note title. If omitted, the server derives one from the first "
                    "non-empty content line."
                )
            ),
        ] = None,
        destination: Annotated[
            Literal["inbox", "scratch", "daily"] | None,
            Field(
                description=(
                    "Optional note folder under notes/. Use inbox for captured ideas, scratch "
                    "for temporary work, and daily for day-specific notes."
                )
            ),
        ] = None,
        relative_path: Annotated[
            str | None,
            Field(
                description=(
                    "Optional vault-relative Markdown path for the new note, such as "
                    "notes/projects/ideas/memory.md, library/references/topic.md, or "
                    "archive/2026/retrospective.md. Must not be used together with destination. "
                    "Notes may live under notes/, archive/, or library/ except library/papers/. "
                    "Paths under assets/ and system/ are rejected."
                )
            ),
        ] = None,
        tags: Annotated[
            list[str] | None,
            Field(
                description=(
                    "Optional note tags for filtering and retrieval. "
                    f"{TAG_CREATION_GUIDANCE}"
                )
            ),
        ] = None,
    ) -> dict[str, object]:
        """Create a deterministic note from freeform text."""

        def run() -> dict[str, object]:
            self._require_non_empty_string(content, "content")
            note, path = self.vault.create_note(
                title=title,
                content=content,
                destination=destination,
                relative_path=relative_path,
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

        return self._execute_tool("shardmind.create_note", run)

    @tool_spec("shardmind_create_paper_card", "shardmind.create_paper_card")
    def create_paper_card(
        self,
        title: Annotated[
            str | None,
            Field(
                description=(
                    "Optional human-readable paper title. Prefer the canonical published title "
                    "when available."
                )
            ),
        ] = None,
        authors: Annotated[
            list[str] | None,
            Field(
                description=(
                    "Optional ordered author list from first author to last author, preserving "
                    "publication order."
                )
            ),
        ] = None,
        year: Annotated[
            int | None,
            Field(description="Optional publication year as a four-digit integer."),
        ] = None,
        source: Annotated[
            str | None,
            Field(description="Optional source label such as arxiv, doi, conference, or journal."),
        ] = None,
        url: Annotated[
            str | None,
            Field(
                description=(
                    "Optional canonical URL (publisher, DOI resolver, or stable preprint URL)."
                )
            ),
        ] = None,
        citekey: Annotated[
            str | None,
            Field(
                description=(
                    "Optional Better BibTeX-style citekey in lowercase authorYearTitleword "
                    "format, for example mottes2026gradient."
                )
            ),
        ] = None,
        sections: Annotated[
            dict[str, str] | None,
            Field(description=PAPER_CARD_CREATE_SECTIONS_GUIDANCE),
        ] = None,
        tags: Annotated[
            list[str] | None,
            Field(
                description=(
                    "Optional paper tags for thematic grouping, e.g. memory, planning, or "
                    f"evaluation. {TAG_CREATION_GUIDANCE}"
                )
            ),
        ] = None,
        status: Annotated[
            str | None,
            Field(
                description=(
                    "Optional reading status such as unread, queued, reading, reviewed, "
                    "or archived."
                )
            ),
        ] = None,
        relative_path: Annotated[
            str | None,
            Field(
                description=(
                    "Optional vault-relative Markdown path for the new paper card, such as "
                    "library/papers/ml/transformers/attention-card.md. Must stay under "
                    "library/papers/ and end in .md."
                )
            ),
        ] = None,
    ) -> dict[str, object]:
        """Create a paper card with metadata plus optional canonical sections in one request."""

        def run() -> dict[str, object]:
            next_sections = self._optional_dict(sections, "sections")
            if not any((title, url, next_sections)):
                raise InvalidInputError("At least one of title, url, or sections must be provided.")
            paper_card, path = self.vault.create_paper_card(
                title=title,
                authors=authors,
                year=year,
                source=source,
                url=url,
                citekey=citekey,
                sections=next_sections,
                tags=tags,
                status=status,
                relative_path=relative_path,
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

        return self._execute_tool("shardmind.create_paper_card", run)

    @tool_spec("shardmind_append_to_note", "shardmind.append_to_note")
    def append_to_note(
        self,
        id: Annotated[str, Field(description=OBJECT_ID_GUIDANCE)],  # noqa: A002
        content: Annotated[
            str,
            Field(
                description=(
                    "Content to append to the existing # Content section without replacing "
                    "current text. "
                    f"{WIKILINK_GUIDANCE}"
                )
            ),
        ],
        section: Annotated[
            str | None,
            Field(
                description=(
                    "Optional section selector. Milestone 2 supports only Content/content."
                )
            ),
        ] = None,
    ) -> dict[str, object]:
        """Append content to the canonical Content section of an existing note."""

        def run() -> dict[str, object]:
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

        return self._execute_tool("shardmind.append_to_note", run)

    @tool_spec("shardmind_edit_note", "shardmind.edit_note")
    def edit_note(
        self,
        id: Annotated[str, Field(description=OBJECT_ID_GUIDANCE)],  # noqa: A002
        sections: Annotated[
            dict[str, str] | None,
            Field(description=NOTE_SECTION_PATCH_GUIDANCE),
        ] = None,
        metadata: Annotated[
            dict[str, object] | None,
            Field(description=NOTE_METADATA_PATCH_GUIDANCE),
        ] = None,
        mode: Annotated[
            Literal["fill-empty", "refresh"] | None,
            Field(description=EDIT_MODE_GUIDANCE),
        ] = None,
    ) -> dict[str, object]:
        """Edit supported sections and metadata on an existing note."""

        def run() -> dict[str, object]:
            self._require_non_empty_string(id, "id")
            next_mode = mode or "refresh"
            next_sections = self._optional_dict(sections, "sections")
            next_metadata = self._optional_dict(metadata, "metadata")
            if not next_sections and not next_metadata:
                raise InvalidInputError("At least one of sections or metadata must be provided.")
            note, path = self.vault.update_note(
                id,
                sections=next_sections,
                metadata=next_metadata,
                mode=next_mode,
            )
            return {
                "ok": True,
                "result": {
                    "id": note.id,
                    "type": note.type,
                    "path": path,
                    "updated_at": note.updated_at,
                    "mode": next_mode,
                },
            }

        return self._execute_tool("shardmind.edit_note", run)

    @tool_spec("shardmind_edit_paper_card", "shardmind.edit_paper_card")
    def edit_paper_card(
        self,
        id: Annotated[str, Field(description=OBJECT_ID_GUIDANCE)],  # noqa: A002
        sections: Annotated[
            dict[str, str] | None,
            Field(description=PAPER_CARD_SECTION_PATCH_GUIDANCE),
        ] = None,
        metadata: Annotated[
            dict[str, object] | None,
            Field(description=PAPER_CARD_METADATA_PATCH_GUIDANCE),
        ] = None,
        mode: Annotated[
            Literal["fill-empty", "refresh"] | None,
            Field(description=EDIT_MODE_GUIDANCE),
        ] = None,
    ) -> dict[str, object]:
        """Populate or replace the canonical structured sections on an existing paper card."""

        def run() -> dict[str, object]:
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

        return self._execute_tool("shardmind.edit_paper_card", run)

    @tool_spec("shardmind_get_object", "shardmind.get_object")
    def get_object(
        self,
        id: Annotated[str, Field(description=OBJECT_ID_GUIDANCE)],  # noqa: A002
    ) -> dict[str, object]:
        def run() -> dict[str, object]:
            self._require_non_empty_string(id, "id")
            record, path = self.vault.read_object(id)
            return {"ok": True, "result": record.to_document(path)}

        return self._execute_tool("shardmind.get_object", run)

    @tool_spec("shardmind_move_object", "shardmind.move_object")
    def move_object(
        self,
        id: Annotated[str, Field(description=OBJECT_ID_GUIDANCE)],  # noqa: A002
        relative_path: Annotated[
            str,
            Field(
                description=(
                    "New vault-relative Markdown path for the existing object. "
                    f"{OBJECT_PATH_GUIDANCE}"
                )
            ),
        ],
    ) -> dict[str, object]:
        """Move an existing object to a new allowed path without changing its id."""

        def run() -> dict[str, object]:
            self._require_non_empty_string(id, "id")
            self._require_non_empty_string(relative_path, "relative_path")
            record, path = self.vault.move_object(id, relative_path)
            return {
                "ok": True,
                "result": {
                    "id": record.id,
                    "type": record.type,
                    "path": path,
                    **titled_fields(record.type, record.title),
                    **path_reference_fields(path),
                },
            }

        return self._execute_tool("shardmind.move_object", run)

    @tool_spec("shardmind_delete_object", "shardmind.delete_object")
    def delete_object(
        self,
        id: Annotated[str, Field(description=OBJECT_ID_GUIDANCE)],  # noqa: A002
    ) -> dict[str, object]:
        """Delete an existing object by id and remove it from the derived index."""

        def run() -> dict[str, object]:
            self._require_non_empty_string(id, "id")
            record, path = self.vault.delete_object(id)
            return {
                "ok": True,
                "result": {
                    "id": record.id,
                    "type": record.type,
                    "path": path,
                    "deleted": True,
                    **titled_fields(record.type, record.title),
                    **path_reference_fields(path),
                },
            }

        return self._execute_tool("shardmind.delete_object", run)

    @tool_spec("shardmind_list_objects", "shardmind.list_objects")
    def list_objects(
        self,
        object_type: Annotated[
            Literal["note", "paper-card"] | None,
            Field(description="Optional type filter. Omit to include both object types."),
        ] = None,
        path_scope: Annotated[
            str | None,
            Field(
                description=("Optional path prefix filter such as notes/inbox or library/papers.")
            ),
        ] = None,
        limit: Annotated[
            int,
            Field(ge=1, le=200, description="Maximum number of objects to return."),
        ] = 50,
    ) -> dict[str, object]:
        def run() -> dict[str, object]:
            objects = self._list_live_objects(
                object_type=object_type,
                path_scope=path_scope,
                limit=limit,
            )
            return {"ok": True, "result": {"objects": objects}}

        return self._execute_tool("shardmind.list_objects", run)

    @tool_spec("shardmind_list_tags", "shardmind.list_tags")
    def list_tags(
        self,
        object_type: Annotated[
            Literal["note", "paper-card"] | None,
            Field(description="Optional type filter. Omit to include tags from both object types."),
        ] = None,
        path_scope: Annotated[
            str | None,
            Field(
                description=(
                    "Optional path prefix filter such as notes/inbox or library/papers; "
                    "limits tags to documents under that path."
                )
            ),
        ] = None,
        limit: Annotated[
            int,
            Field(
                ge=1,
                le=200,
                description="Maximum number of distinct tag strings to return (index-backed).",
            ),
        ] = 200,
    ) -> dict[str, object]:
        def run() -> dict[str, object]:
            tags = self._list_live_tags(
                object_type=object_type,
                path_scope=path_scope,
                limit=limit,
            )
            return {"ok": True, "result": {"tags": tags}}

        return self._execute_tool("shardmind.list_tags", run)

    @tool_spec("shardmind_search", "shardmind.search")
    def search(
        self,
        query: Annotated[
            str,
            Field(description="Lexical search query string."),
        ],
        object_types: Annotated[
            list[Literal["note", "paper-card"]] | None,
            Field(description="Optional object-type filter list."),
        ] = None,
        path_scope: Annotated[
            str | None,
            Field(description="Optional path prefix filter."),
        ] = None,
        top_k: Annotated[
            int,
            Field(ge=1, le=50, description="Maximum number of ranked results to return."),
        ] = 10,
        tags: Annotated[
            list[str] | None,
            Field(
                description=("Optional tag filter; only objects matching these tags are returned.")
            ),
        ] = None,
    ) -> dict[str, object]:
        def run() -> dict[str, object]:
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

        return self._execute_tool("shardmind.search", run)

    def invoke(self, tool_name: str, payload: dict[str, Any]) -> dict[str, object]:
        def run() -> dict[str, object]:
            return invoke_registered_tool(self, tool_name, payload)

        return self._execute_tool(tool_name, run)

    def _execute_tool(
        self,
        tool_name: str,
        operation: Callable[[], dict[str, object]],
    ) -> dict[str, object]:
        try:
            return operation()
        except ShardMindError as exc:
            return exc.to_response()
        except Exception:
            LOGGER.exception("Unexpected error while executing %s", tool_name)
            return InternalError().to_response()

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

    def _list_live_tags(
        self,
        *,
        object_type: Literal["note", "paper-card"] | None,
        path_scope: str | None,
        limit: int,
    ) -> list[str]:
        stale_found = False
        live_tags: list[str] = []
        seen_tags: set[str] = set()
        tag_references = self.index.list_tag_references(
            object_type=object_type,
            path_scope=path_scope,
        )
        for candidate in tag_references:
            if len(live_tags) >= limit:
                break
            tag = str(candidate["tag"])
            if tag in seen_tags:
                continue
            resolved = self.vault.reconcile_index_entry(
                str(candidate["id"]),
                str(candidate["path"]),
            )
            if resolved is None:
                stale_found = True
                continue
            seen_tags.add(tag)
            live_tags.append(tag)
        if stale_found:
            return self.index.list_tags(
                object_type=object_type,
                path_scope=path_scope,
                limit=limit,
            )
        return live_tags
