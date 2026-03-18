from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from shardmind.bootstrap import build_runtime

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class IndexServiceTest(unittest.TestCase):
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

    def test_reindex_and_search_note(self) -> None:
        note, path = self.runtime.vault.create_note(
            title="Memory Architecture Idea",
            content="Typed long-term memory for research agents",
            tags=["memory"],
        )
        self.runtime.index.reindex_note(note, path)

        results = self.runtime.index.search("memory")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].id, note.id)
        self.assertIn("memory", results[0].snippet.lower())

    def test_list_objects_orders_by_recent_update(self) -> None:
        first, first_path = self.runtime.vault.create_note(title="First", content="alpha")
        self.runtime.index.reindex_note(first, first_path)
        second, second_path = self.runtime.vault.create_note(title="Second", content="beta")
        self.runtime.index.reindex_note(second, second_path)

        objects = self.runtime.index.list_objects(object_type="note", limit=10)
        self.assertEqual(objects[0]["id"], second.id)
        self.assertEqual(objects[1]["id"], first.id)

    def test_reindex_and_search_paper_card_sections(self) -> None:
        paper_card, path = self.runtime.vault.create_paper_card(
            title="Memory Systems for Research Agents",
            source_text="abstract",
            tags=["memory", "agents"],
        )
        paper_card, path = self.runtime.vault.update_paper_card_sections(
            paper_card.id,
            sections={"llm_summary": "Typed long-term memory for research agents"},
            mode="fill-empty",
        )
        self.runtime.index.reindex_object(paper_card, path)

        results = self.runtime.index.search("memory", object_types=["paper-card"], tags=["memory"])
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].id, paper_card.id)
        self.assertIn("LLM summary", results[0].matched_sections)

    def test_search_collapses_mixed_object_results(self) -> None:
        note, note_path = self.runtime.vault.create_note(
            title="Memory note",
            content="Memory systems notes",
            tags=["memory"],
        )
        self.runtime.index.reindex_object(note, note_path)
        paper_card, paper_path = self.runtime.vault.create_paper_card(
            title="Memory paper",
            source_text="memory substrate",
            tags=["memory"],
        )
        paper_card, paper_path = self.runtime.vault.update_paper_card_sections(
            paper_card.id,
            sections={
                "llm_summary": "memory summary",
                "why_relevant": "memory relevance",
            },
            mode="fill-empty",
        )
        self.runtime.index.reindex_object(paper_card, paper_path)

        results = self.runtime.index.search("memory", top_k=5)
        self.assertEqual({result.type for result in results}, {"note", "paper-card"})

    def test_read_object_repairs_stale_index_path(self) -> None:
        note, path = self.runtime.vault.create_note(title="Repair me", content="body")
        self.runtime.index.reindex_object(note, path)
        source = self.runtime.settings.vault_path / path
        destination = self.runtime.settings.vault_path / "notes" / "scratch" / source.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        source.replace(destination)

        fetched, repaired_path = self.runtime.vault.read_object(note.id)
        self.assertEqual(fetched.id, note.id)
        self.assertEqual(repaired_path, "notes/scratch/repair-me.md")
        self.assertEqual(self.runtime.index.get_path(note.id), repaired_path)

    def test_duplicate_detection_uses_index_metadata(self) -> None:
        paper_card, path = self.runtime.vault.create_paper_card(
            title="Duplicate via index",
            url="https://example.com/duplicate",
        )
        self.runtime.index.reindex_object(paper_card, path)
        self.assertEqual(
            self.runtime.index.find_duplicate_paper_card(
                normalized_title="duplicate-via-index",
                url="",
                citekey="",
            ),
            paper_card.id,
        )

    def test_rebuild_restores_manually_added_object(self) -> None:
        note, path = self.runtime.vault.create_note(title="Indexed", content="body")
        self.runtime.index.reindex_object(note, path)
        manual_note, manual_path = self.runtime.vault.create_note(
            title="Manual",
            content="outside index",
        )
        self.runtime.index.remove_object(manual_note.id)

        self.runtime.index.rebuild(self.runtime.vault.list_objects())
        ids = {item["id"] for item in self.runtime.index.list_objects(limit=10)}
        self.assertIn(note.id, ids)
        self.assertIn(manual_note.id, ids)
        self.assertEqual(self.runtime.index.get_path(manual_note.id), manual_path)

    def test_rebuild_keeps_existing_index_when_vault_parse_fails(self) -> None:
        note, path = self.runtime.vault.create_note(title="Safe note", content="body")
        self.runtime.index.reindex_object(note, path)
        bad_path = self.runtime.settings.vault_path / "notes" / "inbox" / "broken.md"
        bad_path.write_text("not frontmatter", encoding="utf-8")

        with self.assertRaises(ValueError):
            self.runtime.vault.list_objects()

        objects = self.runtime.index.list_objects(limit=10)
        self.assertEqual([item["id"] for item in objects], [note.id])

    def test_foreign_key_delete_cleans_related_rows(self) -> None:
        note, path = self.runtime.vault.create_note(title="Delete rows", content="body", tags=["x"])
        self.runtime.index.reindex_object(note, path)
        self.runtime.index.remove_object(note.id)
        connection = self.runtime.index.connection
        self.assertIsNotNone(connection)
        chunk_count = connection.execute(
            "SELECT COUNT(*) FROM chunks WHERE document_id = ?",
            (note.id,),
        ).fetchone()[0]
        tag_count = connection.execute(
            "SELECT COUNT(*) FROM tags WHERE document_id = ?",
            (note.id,),
        ).fetchone()[0]
        self.assertEqual(
            chunk_count,
            0,
        )
        self.assertEqual(
            tag_count,
            0,
        )

    def test_connection_pragmas_are_enabled(self) -> None:
        connection = self.runtime.index.connection
        self.assertIsNotNone(connection)
        self.assertEqual(connection.execute("PRAGMA foreign_keys").fetchone()[0], 1)
        self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0].lower(), "wal")
        self.assertEqual(connection.execute("PRAGMA busy_timeout").fetchone()[0], 5000)
