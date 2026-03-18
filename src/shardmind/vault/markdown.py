"""Deterministic frontmatter rendering with tolerant parsing."""

from __future__ import annotations

import json
import re
from dataclasses import asdict

from shardmind.models import (
    Note,
    NoteProvenance,
    NoteSections,
    ObjectRecord,
    PaperCard,
    PaperCardProvenance,
    PaperCardSections,
)
from shardmind.paper_cards import PAPER_CARD_SECTION_HEADINGS, PAPER_CARD_SECTION_TITLES

_INTEGER_RE = re.compile(r"^-?\d+$")


def parse_frontmatter(raw: str) -> dict[str, object]:
    lines = raw.splitlines()
    parsed, _ = _parse_mapping(lines, 0, 0)
    return parsed


def _parse_mapping(
    lines: list[str], start_index: int, indent: int
) -> tuple[dict[str, object], int]:
    data: dict[str, object] = {}
    index = start_index
    while index < len(lines):
        line = lines[index]
        if not line.strip():
            index += 1
            continue
        current_indent = _indent_level(line)
        if current_indent < indent:
            break
        if current_indent > indent:
            raise ValueError("Invalid frontmatter indentation.")
        stripped = line[indent:]
        key, separator, remainder = stripped.partition(":")
        if separator == "":
            raise ValueError(f"Invalid frontmatter line: {line}")
        key = key.strip()
        value = remainder.lstrip()
        if value:
            data[key] = _parse_scalar(value)
            index += 1
            continue
        next_index = _next_non_empty(lines, index + 1)
        if next_index is None or _indent_level(lines[next_index]) <= indent:
            data[key] = ""
            index += 1
            continue
        if lines[next_index].lstrip().startswith("- "):
            data[key], index = _parse_list(lines, next_index, indent + 2)
            continue
        data[key], index = _parse_mapping(lines, next_index, indent + 2)
    return data, index


def _parse_list(lines: list[str], start_index: int, indent: int) -> tuple[list[object], int]:
    items: list[object] = []
    index = start_index
    while index < len(lines):
        line = lines[index]
        if not line.strip():
            index += 1
            continue
        current_indent = _indent_level(line)
        if current_indent < indent:
            break
        if current_indent != indent or not line[indent:].startswith("- "):
            raise ValueError("Invalid list indentation in frontmatter.")
        items.append(_parse_scalar(line[indent + 2 :].strip()))
        index += 1
    return items, index


def _parse_scalar(value: str) -> object:
    stripped = value.strip()
    if stripped == "":
        return ""
    if stripped.startswith('"') and stripped.endswith('"'):
        return json.loads(stripped)
    if stripped.startswith("'") and stripped.endswith("'"):
        return stripped[1:-1].replace("''", "'")
    lowered = stripped.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"null", "~"}:
        return None
    if _INTEGER_RE.match(stripped):
        return int(stripped)
    if stripped.startswith("[") and stripped.endswith("]"):
        inline_list = _parse_inline_list(stripped)
        if inline_list is not None:
            return inline_list
    return stripped


def _parse_inline_list(value: str) -> list[object] | None:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, list):
        return parsed
    inner = value[1:-1].strip()
    if inner == "":
        return []
    return [_parse_scalar(item) for item in _split_inline_list(inner)]


def _split_inline_list(value: str) -> list[str]:
    items: list[str] = []
    current: list[str] = []
    quote: str | None = None
    for char in value:
        if quote is not None:
            current.append(char)
            if char == quote:
                quote = None
            continue
        if char in {"'", '"'}:
            quote = char
            current.append(char)
            continue
        if char == ",":
            items.append("".join(current).strip())
            current = []
            continue
        current.append(char)
    items.append("".join(current).strip())
    return items


def _next_non_empty(lines: list[str], start_index: int) -> int | None:
    for index in range(start_index, len(lines)):
        if lines[index].strip():
            return index
    return None


def _indent_level(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def _split_frontmatter(markdown_text: str) -> tuple[dict[str, object], str]:
    if not markdown_text.startswith("---\n"):
        raise ValueError("Markdown object is missing frontmatter.")
    _, frontmatter_body = markdown_text.split("---\n", 1)
    frontmatter_raw, body = frontmatter_body.split("\n---\n", 1)
    return parse_frontmatter(frontmatter_raw), body


def parse_note(markdown_text: str) -> Note:
    frontmatter, body = _split_frontmatter(markdown_text)
    return _note_from_parts(frontmatter, body)


def parse_sections(markdown_body: str) -> dict[str, str]:
    sections: dict[str, str] = {}
    current_heading: str | None = None
    current_lines: list[str] = []
    for raw_line in markdown_body.lstrip("\n").splitlines():
        if raw_line.startswith("# "):
            if current_heading is not None:
                sections[current_heading] = "\n".join(current_lines).strip()
            current_heading = raw_line[2:].strip()
            current_lines = []
        else:
            current_lines.append(raw_line)
    if current_heading is not None:
        sections[current_heading] = "\n".join(current_lines).strip()
    return sections


def parse_paper_card(markdown_text: str) -> PaperCard:
    frontmatter, body = _split_frontmatter(markdown_text)
    return _paper_card_from_parts(frontmatter, body)


def parse_object(markdown_text: str) -> ObjectRecord:
    frontmatter, body = _split_frontmatter(markdown_text)
    object_type = str(frontmatter.get("type", ""))
    if object_type == "paper-card":
        return _paper_card_from_parts(frontmatter, body)
    return _note_from_parts(frontmatter, body)


def _note_from_parts(frontmatter: dict[str, object], body: str) -> Note:
    heading = "# Content\n"
    content = body.lstrip("\n")
    if content.startswith(heading):
        content = content[len(heading) :]
    content = content.lstrip("\n").rstrip()
    provenance = frontmatter.get("provenance") or {}
    if not isinstance(provenance, dict):
        provenance = {}
    return Note(
        id=str(frontmatter.get("id", "")),
        type=str(frontmatter.get("type", "note")),
        title=str(frontmatter.get("title", "")),
        tags=_coerce_list_of_strings(frontmatter.get("tags")),
        provenance=NoteProvenance(created_from=str(provenance.get("created_from", ""))),
        created_at=str(frontmatter.get("created_at", "")),
        updated_at=str(frontmatter.get("updated_at", "")),
        sections=NoteSections(content=content),
    )


def _paper_card_from_parts(frontmatter: dict[str, object], body: str) -> PaperCard:
    provenance = frontmatter.get("provenance") or {}
    if not isinstance(provenance, dict):
        provenance = {}
    raw_sections = parse_sections(body)
    sections = PaperCardSections(
        **{
            field_name: raw_sections.get(heading, "")
            for heading, field_name in PAPER_CARD_SECTION_HEADINGS.items()
        }
    )
    year = frontmatter.get("year")
    return PaperCard(
        id=str(frontmatter.get("id", "")),
        type=str(frontmatter.get("type", "paper-card")),
        title=str(frontmatter.get("title", "")),
        authors=_coerce_list_of_strings(frontmatter.get("authors")),
        year=year if isinstance(year, int) else None,
        source=_coerce_optional_string(frontmatter.get("source")),
        url=_coerce_optional_string(frontmatter.get("url")),
        citekey=_coerce_optional_string(frontmatter.get("citekey")),
        tags=_coerce_list_of_strings(frontmatter.get("tags")),
        status=str(frontmatter.get("status", "unread")),
        provenance=PaperCardProvenance(
            created_from=str(provenance.get("created_from", "")),
            source_type=str(provenance.get("source_type", "")),
            source_ref=str(provenance.get("source_ref", "")),
            llm_enriched=bool(provenance.get("llm_enriched", False)),
        ),
        created_at=str(frontmatter.get("created_at", "")),
        updated_at=str(frontmatter.get("updated_at", "")),
        sections=sections,
    )


def _coerce_list_of_strings(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _coerce_optional_string(value: object) -> str:
    if value is None:
        return ""
    return str(value)


def render_note(note: Note) -> str:
    frontmatter = asdict(note)
    lines = ["---"]
    lines.extend(
        (
            _frontmatter_line("id", frontmatter["id"]),
            _frontmatter_line("type", frontmatter["type"]),
            _frontmatter_line("title", frontmatter["title"]),
            _frontmatter_line("tags", frontmatter["tags"]),
            "provenance:",
            _nested_frontmatter_line("created_from", frontmatter["provenance"]["created_from"]),
            _frontmatter_line("created_at", frontmatter["created_at"]),
            _frontmatter_line("updated_at", frontmatter["updated_at"]),
        )
    )
    lines.append("---")
    lines.append("")
    lines.append("# Content")
    content = note.sections.content.rstrip()
    if content:
        lines.append("")
        lines.append(content)
    return "\n".join(lines) + "\n"


def render_paper_card(paper_card: PaperCard) -> str:
    frontmatter = asdict(paper_card)
    lines = ["---"]
    lines.extend(
        (
            _frontmatter_line("id", frontmatter["id"]),
            _frontmatter_line("type", frontmatter["type"]),
            _frontmatter_line("title", frontmatter["title"]),
            _frontmatter_line("authors", frontmatter["authors"]),
            _frontmatter_line("year", frontmatter["year"]),
            _frontmatter_line("source", frontmatter["source"]),
            _frontmatter_line("url", frontmatter["url"]),
            _frontmatter_line("citekey", frontmatter["citekey"]),
            _frontmatter_line("tags", frontmatter["tags"]),
            _frontmatter_line("status", frontmatter["status"]),
            "provenance:",
            _nested_frontmatter_line("created_from", frontmatter["provenance"]["created_from"]),
            _nested_frontmatter_line("source_type", frontmatter["provenance"]["source_type"]),
            _nested_frontmatter_line("source_ref", frontmatter["provenance"]["source_ref"]),
            _nested_frontmatter_line("llm_enriched", frontmatter["provenance"]["llm_enriched"]),
            _frontmatter_line("created_at", frontmatter["created_at"]),
            _frontmatter_line("updated_at", frontmatter["updated_at"]),
        )
    )
    lines.append("---")
    lines.append("")
    for field_name, heading in PAPER_CARD_SECTION_TITLES.items():
        lines.append(f"# {heading}")
        content = getattr(paper_card.sections, field_name).rstrip()
        if content:
            lines.append("")
            lines.append(content)
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _frontmatter_line(key: str, value: object) -> str:
    return f"{key}: {_format_frontmatter_value(value)}"


def _nested_frontmatter_line(key: str, value: object) -> str:
    return f"  {key}: {_format_frontmatter_value(value)}"


def _format_frontmatter_value(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=True)
    return json.dumps(str(value), ensure_ascii=True)
