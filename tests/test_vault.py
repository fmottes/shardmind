from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from shardmind.bootstrap import build_runtime
from shardmind.config import Settings, default_vault_path
from shardmind.errors import DuplicateObjectError
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

        log_path = self.runtime.settings.vault_path / "system" / "logs" / "operations.log"
        event = json.loads(log_path.read_text(encoding="utf-8").strip().splitlines()[-1])
        self.assertEqual(event["tool_name"], "knowledge.create_note")

    def test_append_to_note_updates_content(self) -> None:
        note, _ = self.runtime.vault.create_note(content="Original", title="Scratch note")
        updated_note, _ = self.runtime.vault.append_to_note(note.id, "More context")
        self.assertEqual(updated_note.sections.content, "Original\n\nMore context")

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
  llm_enriched: true
created_at: 2026-03-18T15:42:00Z
updated_at: 2026-03-18T15:45:00Z
---

# Source notes

Abstract here

# LLM summary

Summary here
"""
        parsed = parse_paper_card(markdown)
        self.assertEqual(parsed.title, "Attention Is All You Need: A Transformer Architecture")
        self.assertEqual(parsed.authors, ["A. Author", "B. Author"])
        self.assertEqual(parsed.tags, ["memory", "agents"])
        self.assertTrue(parsed.provenance.llm_enriched)

    def test_create_paper_card_writes_canonical_markdown_and_log(self) -> None:
        paper_card, relative_path = self.runtime.vault.create_paper_card(
            title="Memory Systems for Research Agents",
            authors=["A. Author"],
            year=2025,
            url="https://example.com/paper",
            source_text="Typed long-term memory for research agents",
            tags=["memory", "agents"],
        )

        paper_path = self.runtime.settings.vault_path / relative_path
        self.assertTrue(paper_path.exists())
        saved = parse_paper_card(paper_path.read_text(encoding="utf-8"))
        self.assertEqual(saved.id, paper_card.id)
        self.assertEqual(saved.sections.source_notes, "Typed long-term memory for research agents")
        self.assertEqual(saved.sections.llm_summary, "")
        self.assertIn(
            'title: "Memory Systems for Research Agents"',
            paper_path.read_text(encoding="utf-8"),
        )

        log_path = self.runtime.settings.vault_path / "system" / "logs" / "operations.log"
        event = json.loads(log_path.read_text(encoding="utf-8").strip().splitlines()[-1])
        self.assertEqual(event["tool_name"], "knowledge.create_paper_card")

    def test_update_paper_card_sections_preserves_user_owned_fields(self) -> None:
        paper_card, relative_path = self.runtime.vault.create_paper_card(
            title="Deterministic Cards",
            source_text="raw abstract",
        )
        paper_card.sections.user_notes = "keep this"
        self.runtime.vault._write_object(relative_path, render_paper_card(paper_card))  # noqa: SLF001

        updated, _ = self.runtime.vault.update_paper_card_sections(
            paper_card.id,
            sections={"llm_summary": "new summary"},
            mode="fill-empty",
        )
        self.assertEqual(updated.sections.user_notes, "keep this")
        self.assertEqual(updated.sections.source_notes, "raw abstract")
        self.assertEqual(updated.sections.llm_summary, "new summary")

    def test_enrich_paper_card_updates_allowed_sections_only(self) -> None:
        paper_card, relative_path = self.runtime.vault.create_paper_card(
            title="Paper to Enrich",
            source_text="Original source",
        )
        paper_card.sections.user_notes = "Do not overwrite"
        self.runtime.vault._write_object(relative_path, render_paper_card(paper_card))  # noqa: SLF001

        updated, _ = self.runtime.vault.update_paper_card_sections(
            paper_card.id,
            sections={
                "llm_summary": "Summary",
                "main_claims": "Claim 1",
            },
            metadata={"source": "conference"},
            mode="fill-empty",
        )
        self.assertEqual(updated.sections.llm_summary, "Summary")
        self.assertEqual(updated.sections.main_claims, "Claim 1")
        self.assertEqual(updated.sections.source_notes, "Original source")
        self.assertEqual(updated.sections.user_notes, "Do not overwrite")
        self.assertEqual(updated.source, "conference")
        self.assertTrue(updated.provenance.llm_enriched)

    def test_metadata_only_enrich_does_not_set_llm_provenance(self) -> None:
        paper_card, _ = self.runtime.vault.create_paper_card(
            title="Metadata Only",
            source_text="Original source",
        )
        updated, _ = self.runtime.vault.update_paper_card_sections(
            paper_card.id,
            metadata={"source": "conference"},
            mode="fill-empty",
        )
        self.assertFalse(updated.provenance.llm_enriched)

    def test_duplicate_paper_card_detection_uses_title_or_url(self) -> None:
        self.runtime.vault.create_paper_card(
            title="Duplicate Me",
            url="https://example.com/duplicate",
        )
        with self.assertRaisesRegex(DuplicateObjectError, "matching title or URL"):
            self.runtime.vault.create_paper_card(
                title="Duplicate Me",
                url="https://example.com/another",
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
