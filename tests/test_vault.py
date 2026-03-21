from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from shardmind.bootstrap import build_runtime
from shardmind.config import Settings, default_vault_path
from shardmind.errors import DuplicateObjectError, InvalidInputError
from shardmind.models import PaperCard
from shardmind.vault.ids import slugify
from shardmind.vault.markdown import parse_note, parse_paper_card, render_note, render_paper_card

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class VaultServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.root = Path(self.tempdir.name)
        self.env = patch.dict(
            "os.environ",
            {
                "SHARDMIND_VAULT_PATH": str(self.root / "vault"),
                "SHARDMIND_SQLITE_PATH": str(self.root / "var" / "shardmind.sqlite3"),
                "SHARDMIND_SHARED_PATH": str(PROJECT_ROOT / "shared"),
            },
            clear=False,
        )
        self.env.start()
        self.addCleanup(self.env.stop)
        self.runtime = build_runtime()
        self.addCleanup(self.runtime.close)

    def test_slugify_normalizes_human_titles(self) -> None:
        self.assertEqual(slugify("Memory Architecture Idea!"), "memory-architecture-idea")

    def test_create_note_writes_canonical_markdown_and_log(self) -> None:
        note, relative_path = self.runtime.vault.create_note(
            title="Memory Architecture Idea",
            content="First line\nSecond line",
            tags=["memory", "agents"],
        )

        note_path = self.runtime.settings.vault_path / relative_path
        self.assertTrue(note_path.exists())
        saved_note = parse_note(note_path.read_text(encoding="utf-8"))
        self.assertEqual(saved_note.id, note.id)
        self.assertEqual(saved_note.sections.content, "First line\nSecond line")
        self.assertEqual(saved_note.tags, ["memory", "agents"])
        self.assertIn('title: "Memory Architecture Idea"', note_path.read_text(encoding="utf-8"))
        self.assertEqual(
            note_path.stem,
            f"memory-architecture-idea--{note.id.removeprefix('note-')[:8]}",
        )

        log_path = self.runtime.settings.vault_path / "system" / "logs" / "operations.log"
        event = json.loads(log_path.read_text(encoding="utf-8").strip().splitlines()[-1])
        self.assertEqual(event["tool_name"], "shardmind.create_note")

    def test_append_to_note_updates_content(self) -> None:
        note, _ = self.runtime.vault.create_note(content="Original", title="Scratch note")
        updated_note, _ = self.runtime.vault.append_to_note(note.id, "More context")
        self.assertEqual(updated_note.sections.content, "Original\n\nMore context")

    def test_update_note_refresh_updates_content_and_metadata(self) -> None:
        note, _ = self.runtime.vault.create_note(
            content="Original content",
            title="Original title",
            tags=["seed"],
        )
        updated, _ = self.runtime.vault.update_note(
            note.id,
            sections={"content": "Replaced content"},
            metadata={"title": "Updated title", "tags": ["memory", "agents"]},
            mode="refresh",
        )
        self.assertEqual(updated.sections.content, "Replaced content")
        self.assertEqual(updated.title, "Updated title")
        self.assertEqual(updated.tags, ["memory", "agents"])

    def test_update_note_defaults_to_refresh(self) -> None:
        note, _ = self.runtime.vault.create_note(
            content="Original content",
            title="Original title",
        )
        updated, _ = self.runtime.vault.update_note(
            note.id,
            sections={"content": "Updated content"},
            metadata={"title": "Updated title"},
        )
        self.assertEqual(updated.sections.content, "Updated content")
        self.assertEqual(updated.title, "Updated title")

    def test_update_note_rejects_unknown_section(self) -> None:
        note, _ = self.runtime.vault.create_note(content="Original", title="Scratch note")
        with self.assertRaisesRegex(InvalidInputError, "Unsupported note section 'summary'"):
            self.runtime.vault.update_note(
                note.id,
                sections={"summary": "invalid"},
                mode="fill-empty",
            )

    def test_render_and_parse_round_trip(self) -> None:
        note, _ = self.runtime.vault.create_note(content="Round trip", title="Round trip")
        rendered = render_note(note)
        parsed = parse_note(rendered)
        self.assertEqual(parsed.id, note.id)
        self.assertEqual(parsed.title, note.title)
        self.assertEqual(parsed.sections.content, note.sections.content)

    def test_frontmatter_parser_tolerates_obsidian_yaml_round_trip(self) -> None:
        markdown = """---
id: paper-attention
type: paper-card
title: "Attention Is All You Need: A Transformer Architecture"
authors:
  - A. Author
  - B. Author
year: 2025
source: arxiv
url: https://example.com/paper
citekey: attention-2025
tags:
  - memory
  - agents
status: reviewed
provenance:
  created_from: mcp
  source_type: zotero
  source_ref: doi:10.1000/test
created_at: 2026-03-18T15:42:00Z
updated_at: 2026-03-18T15:45:00Z
---

# Notes

Abstract here

# Summary

Summary here
"""
        parsed = parse_paper_card(markdown)
        self.assertEqual(parsed.title, "Attention Is All You Need: A Transformer Architecture")
        self.assertEqual(parsed.authors, ["A. Author", "B. Author"])
        self.assertEqual(parsed.tags, ["memory", "agents"])
        self.assertEqual(parsed.sections.notes, "Abstract here")
        self.assertEqual(parsed.sections.summary, "Summary here")

    def test_create_paper_card_writes_canonical_markdown_and_log(self) -> None:
        paper_card, relative_path = self.runtime.vault.create_paper_card(
            title="Memory Systems for Research Agents",
            authors=["A. Author"],
            year=2025,
            source="arxiv",
            url="https://example.com/paper",
            sections={
                "summary": "Typed long-term memory for research agents.",
                "main_claims": "Claim 1",
            },
            tags=["memory", "agents"],
            status="reviewed",
        )

        paper_path = self.runtime.settings.vault_path / relative_path
        self.assertTrue(paper_path.exists())
        saved = parse_paper_card(paper_path.read_text(encoding="utf-8"))
        self.assertEqual(saved.id, paper_card.id)
        self.assertEqual(saved.sections.notes, "")
        self.assertEqual(saved.sections.summary, "Typed long-term memory for research agents.")
        self.assertEqual(saved.sections.main_claims, "Claim 1")
        self.assertEqual(saved.source, "arxiv")
        self.assertEqual(saved.status, "reviewed")
        self.assertIn(
            'title: "Memory Systems for Research Agents"',
            paper_path.read_text(encoding="utf-8"),
        )
        self.assertEqual(
            paper_path.stem,
            f"memory-systems-for-research-agents--{paper_card.id.removeprefix('paper-')[:8]}",
        )

        log_path = self.runtime.settings.vault_path / "system" / "logs" / "operations.log"
        event = json.loads(log_path.read_text(encoding="utf-8").strip().splitlines()[-1])
        self.assertEqual(event["tool_name"], "shardmind.create_paper_card")

    def test_update_paper_card_sections_preserves_user_owned_fields(self) -> None:
        paper_card, relative_path = self.runtime.vault.create_paper_card(
            title="Deterministic Cards",
            sections={"notes": "raw abstract"},
        )
        paper_card.sections.user_notes = "keep this"
        self.runtime.vault._write_object(relative_path, render_paper_card(paper_card))  # noqa: SLF001

        updated, _ = self.runtime.vault.update_paper_card_sections(
            paper_card.id,
            sections={"summary": "new summary", "notes": "clean notes"},
            mode="fill-empty",
        )
        self.assertEqual(updated.sections.user_notes, "keep this")
        self.assertEqual(updated.sections.notes, "raw abstract")
        self.assertEqual(updated.sections.summary, "new summary")

    def test_edit_paper_card_updates_allowed_sections_only(self) -> None:
        paper_card, relative_path = self.runtime.vault.create_paper_card(
            title="Paper to Enrich",
            sections={"notes": "Original source"},
        )
        paper_card.sections.user_notes = "Do not overwrite"
        self.runtime.vault._write_object(relative_path, render_paper_card(paper_card))  # noqa: SLF001

        updated, _ = self.runtime.vault.update_paper_card_sections(
            paper_card.id,
            sections={
                "summary": "Summary",
                "notes": "Updated notes",
                "main_claims": "Claim 1",
                "related_links": "[[supporting-note--1234abcd]]",
            },
            metadata={"source": "conference"},
            mode="fill-empty",
        )
        self.assertEqual(updated.sections.summary, "Summary")
        self.assertEqual(updated.sections.main_claims, "Claim 1")
        self.assertEqual(updated.sections.notes, "Original source")
        self.assertEqual(updated.sections.related_links, "[[supporting-note--1234abcd]]")
        self.assertEqual(updated.sections.user_notes, "Do not overwrite")
        self.assertEqual(updated.source, "conference")

    def test_edit_rejects_user_owned_section(self) -> None:
        paper_card, _ = self.runtime.vault.create_paper_card(
            title="Metadata Only",
            sections={"notes": "Original source"},
        )
        with self.assertRaisesRegex(
            InvalidInputError,
            "Unsupported paper card section 'user_notes'",
        ):
            self.runtime.vault.update_paper_card_sections(
                paper_card.id,
                sections={"user_notes": "hands off"},
                mode="fill-empty",
            )

    def test_duplicate_paper_card_detection_uses_title_or_url(self) -> None:
        self.runtime.vault.create_paper_card(
            title="Duplicate Me",
            url="https://example.com/duplicate",
        )
        with self.assertRaisesRegex(DuplicateObjectError, "title, URL, or citekey"):
            self.runtime.vault.create_paper_card(
                title="Duplicate Me",
                url="https://example.com/another",
            )

    def test_duplicate_paper_card_detection_uses_citekey(self) -> None:
        self.runtime.vault.create_paper_card(
            title="One",
            citekey="smith2025memory",
        )
        with self.assertRaisesRegex(DuplicateObjectError, "title, URL, or citekey"):
            self.runtime.vault.create_paper_card(
                title="Two",
                citekey="smith2025memory",
            )

    def test_deleted_paper_card_does_not_block_recreation_from_stale_index(self) -> None:
        paper_card, relative_path = self.runtime.vault.create_paper_card(
            title="Duplicate Me",
            url="https://example.com/duplicate",
            citekey="mottes2026gradient",
            sections={"notes": "seed"},
        )
        self.runtime.index.reindex_object(paper_card, relative_path)
        (self.runtime.settings.vault_path / relative_path).unlink()

        recreated, recreated_path = self.runtime.vault.create_paper_card(
            title="Duplicate Me",
            url="https://example.com/duplicate",
            citekey="mottes2026gradient",
            sections={"notes": "seed"},
        )
        self.assertNotEqual(recreated.id, paper_card.id)
        self.assertEqual(self.runtime.index.get_path(paper_card.id), None)
        self.assertEqual(self.runtime.index.get_path(recreated.id), recreated_path)
        self.assertTrue((self.runtime.settings.vault_path / recreated_path).exists())

    def test_reconcile_indexes_replacement_object_at_stale_path(self) -> None:
        original, original_path = self.runtime.vault.create_note(title="Original", content="a")
        replacement, replacement_path = self.runtime.vault.create_note(
            title="Replacement",
            content="b",
        )
        (self.runtime.settings.vault_path / original_path).write_text(
            render_note(replacement),
            encoding="utf-8",
        )
        (self.runtime.settings.vault_path / replacement_path).unlink()

        resolved = self.runtime.vault.reconcile_index_entry(original.id, original_path)
        self.assertIsNone(resolved)
        self.assertIsNone(self.runtime.index.get_path(original.id))
        self.assertEqual(self.runtime.index.get_path(replacement.id), original_path)

    def test_duplicate_scan_skips_malformed_paper_card_file(self) -> None:
        malformed = self.runtime.settings.vault_path / "library" / "papers" / "broken.md"
        malformed.write_text("not frontmatter", encoding="utf-8")

        created, relative_path = self.runtime.vault.create_paper_card(
            title="Fresh paper",
            url="https://example.com/fresh",
            citekey="fresh2026paper",
            sections={"notes": "seed"},
        )
        self.assertTrue((self.runtime.settings.vault_path / relative_path).exists())
        self.assertEqual(created.title, "Fresh paper")

    def test_list_indexable_objects_skips_malformed_files(self) -> None:
        note, path = self.runtime.vault.create_note(title="Indexable", content="body")
        malformed = self.runtime.settings.vault_path / "notes" / "inbox" / "broken.md"
        malformed.write_text("not frontmatter", encoding="utf-8")

        records, skipped = self.runtime.vault.list_indexable_objects()

        self.assertEqual(records, [(note, path)])
        self.assertEqual(skipped, ["notes/inbox/broken.md"])

    def test_invalid_citekey_format_is_rejected(self) -> None:
        with self.assertRaisesRegex(InvalidInputError, "mottes2026gradient"):
            self.runtime.vault.create_paper_card(
                title="Bad citekey",
                citekey="Mottes-2026-Gradient",
            )

    def test_default_settings_use_user_shardmind_vault(self) -> None:
        self.env.stop()
        home = self.root / "home"
        with patch.dict(
            "os.environ",
            {
                "HOME": str(home),
                "SHARDMIND_SHARED_PATH": str(PROJECT_ROOT / "shared"),
            },
            clear=True,
        ):
            settings = Settings.load()
        self.assertEqual(settings.vault_path, default_vault_path(home))

    def test_settings_fall_back_to_bundled_shared_assets(self) -> None:
        self.env.stop()
        bundled_root = self.root / "bundled"
        shutil.copytree(PROJECT_ROOT / "shared", bundled_root)
        home = self.root / "home"
        with patch.dict(
            "os.environ",
            {
                "HOME": str(home),
            },
            clear=True,
        ):
            with patch("shardmind.config.find_project_root", side_effect=RuntimeError("missing")):
                with patch("shardmind.config.bundled_shared_path", return_value=bundled_root):
                    settings = Settings.load()
        self.assertIsNone(settings.project_root)
        self.assertEqual(settings.shared_path, bundled_root)

    def test_rendered_paper_card_round_trips_bracket_like_strings(self) -> None:
        card = PaperCard(
            id="paper-brackets",
            title="Lists [not tags]",
            source="arxiv:2501.12345",
            url="https://example.com/paper",
            citekey="brackets",
        )
        rendered = render_paper_card(card)
        parsed = parse_paper_card(rendered)
        self.assertEqual(parsed.title, "Lists [not tags]")
        self.assertEqual(parsed.source, "arxiv:2501.12345")

    def test_create_note_rejects_pure_numeric_tag(self) -> None:
        with self.assertRaisesRegex(InvalidInputError, "non-numerical"):
            self.runtime.vault.create_note(content="body", title="t", tags=["1984"])

    def test_create_note_rejects_spaces_and_double_slash_nesting(self) -> None:
        with self.assertRaisesRegex(InvalidInputError, "spaces"):
            self.runtime.vault.create_note(content="body", title="t", tags=["bad tag"])
        with self.assertRaisesRegex(InvalidInputError, "spaces"):
            self.runtime.vault.create_note(content="body", title="t", tags=["# my-tag"])
        with self.assertRaisesRegex(InvalidInputError, "empty nested"):
            self.runtime.vault.create_note(content="body", title="t", tags=["foo//bar"])

    def test_create_note_rejects_illegal_characters(self) -> None:
        with self.assertRaisesRegex(InvalidInputError, "only contain"):
            self.runtime.vault.create_note(content="body", title="t", tags=["foo.bar"])

    def test_create_note_strips_hash_prefix_and_nested_tags_ok(self) -> None:
        note, _ = self.runtime.vault.create_note(
            content="body",
            title="Tagged",
            tags=["#my-tag", "topic/subtopic"],
        )
        self.assertEqual(note.tags, ["my-tag", "topic/subtopic"])

    def test_create_note_dedupes_tags_case_insensitively_first_spelling_wins(self) -> None:
        note, _ = self.runtime.vault.create_note(
            content="body",
            title="Dedupe",
            tags=["Tag", "tag", "TAG"],
        )
        self.assertEqual(note.tags, ["Tag"])

    def test_update_note_tags_validated(self) -> None:
        note, _ = self.runtime.vault.create_note(content="body", title="n", tags=["ok"])
        with self.assertRaisesRegex(InvalidInputError, "non-numerical"):
            self.runtime.vault.update_note(note.id, metadata={"tags": ["999"]}, mode="refresh")
