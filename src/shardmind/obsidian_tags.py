"""Obsidian-compatible tag validation for ShardMind write paths."""

from __future__ import annotations

import string

from shardmind.errors import InvalidInputError

_ALLOWED_IN_SEGMENT = frozenset(string.ascii_letters + string.digits + "_-")


def normalize_and_validate_tag(raw: str) -> str:
    """Strip, optional leading #, validate charset and nesting; return stored form (no #)."""
    tag = raw.strip()
    if not tag:
        raise InvalidInputError("Tag must not be empty.")
    if tag.startswith("#"):
        tag = tag[1:]
        if not tag:
            raise InvalidInputError("Tag must not be only '#'.")
        if tag != tag.strip():
            raise InvalidInputError(
                f"Tag {raw!r} must not contain spaces; use kebab-case or underscores."
            )
    if " " in tag:
        raise InvalidInputError(
            f"Tag {raw!r} must not contain spaces; use kebab-case or underscores."
        )
    if any(ord(c) > 127 for c in tag):
        raise InvalidInputError(f"Tag {raw!r} must use ASCII letters, digits, _, -, and / only.")
    if any(c not in _ALLOWED_IN_SEGMENT and c != "/" for c in tag):
        raise InvalidInputError(
            f"Tag {raw!r} may only contain letters, numbers, underscore, hyphen, and /."
        )
    segments = tag.split("/")
    if "" in segments:
        raise InvalidInputError(f"Tag {raw!r} must not have empty nested segments.")
    for segment in segments:
        if not segment:
            raise InvalidInputError(f"Tag {raw!r} must not have empty nested segments.")
        if any(c not in _ALLOWED_IN_SEGMENT for c in segment):
            raise InvalidInputError(
                f"Tag {raw!r} nested segments may only contain letters, numbers, _, and -."
            )
    compact = tag.replace("/", "")
    if compact and all(c in string.digits for c in compact):
        raise InvalidInputError(
            f"Tag {raw!r} must contain at least one non-numerical character (e.g. a letter)."
        )
    return tag


def normalize_tag_list(tags: list[str]) -> list[str]:
    """Validate each tag, dedupe case-insensitively; first spelling wins."""
    seen_lower: set[str] = set()
    out: list[str] = []
    for raw in tags:
        normalized = normalize_and_validate_tag(raw)
        key = normalized.casefold()
        if key in seen_lower:
            continue
        seen_lower.add(key)
        out.append(normalized)
    return out
