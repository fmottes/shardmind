from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import anyio
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError

from shardmind.bootstrap import build_runtime
from shardmind.mcp.main import register_tools

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class MCPToolsTest(unittest.TestCase):
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

    def test_create_and_get_note_via_mcp_envelope(self) -> None:
        created = self.runtime.tools.create_note(
            title="Memory Architecture Idea",
            content="Typed long-term memory",
            destination="inbox",
            tags=["memory"],
        )
        self.assertTrue(created["ok"])
        note_id = created["result"]["id"]

        fetched = self.runtime.tools.get_object(note_id)
        self.assertTrue(fetched["ok"])
        self.assertEqual(fetched["result"]["id"], note_id)
        self.assertEqual(fetched["result"]["note_title"], "Memory Architecture Idea")
        self.assertEqual(
            fetched["result"]["wikilink"],
            Path(fetched["result"]["path"]).stem,
        )
        self.assertEqual(fetched["result"]["sections"]["content"], "Typed long-term memory")

    def test_append_and_search_note_via_mcp_envelope(self) -> None:
        created = self.runtime.tools.create_note(
            title="Search Target",
            content="Original body",
        )
        note_id = created["result"]["id"]
        appended = self.runtime.tools.append_to_note(id=note_id, content="Semantic memory")
        self.assertTrue(appended["ok"])

        searched = self.runtime.tools.search(
            query="memory",
            object_types=["note"],
            top_k=5,
        )
        self.assertTrue(searched["ok"])
        self.assertEqual(searched["result"]["results"][0]["id"], note_id)
        self.assertEqual(searched["result"]["results"][0]["note_title"], "Search Target")
        self.assertEqual(
            searched["result"]["results"][0]["wikilink"],
            Path(searched["result"]["results"][0]["path"]).stem,
        )

    def test_create_note_with_relative_path_under_library(self) -> None:
        created = self.runtime.tools.create_note(
            title="Library Note",
            content="Nested body",
            relative_path="library/references/agents/library-note.md",
            tags=["memory"],
        )
        self.assertTrue(created["ok"])
        self.assertEqual(
            created["result"]["path"],
            "library/references/agents/library-note.md",
        )

        fetched = self.runtime.tools.get_object(created["result"]["id"])
        self.assertTrue(fetched["ok"])
        self.assertEqual(fetched["result"]["type"], "note")
        self.assertEqual(fetched["result"]["path"], "library/references/agents/library-note.md")

        listed = self.runtime.tools.list_objects(path_scope="library/references/", limit=10)
        self.assertTrue(listed["ok"])
        self.assertEqual(
            listed["result"]["objects"][0]["path"],
            "library/references/agents/library-note.md",
        )

    def test_edit_note_via_mcp_envelope(self) -> None:
        created = self.runtime.tools.create_note(
            title="Draft Note",
            content="Seed note body",
            tags=["draft"],
        )
        self.assertTrue(created["ok"])
        note_id = created["result"]["id"]

        edited = self.runtime.tools.edit_note(
            id=note_id,
            sections={"content": "Refined note body"},
            metadata={"title": "Refined Note", "tags": ["final"]},
            mode="refresh",
        )
        self.assertTrue(edited["ok"])

        fetched = self.runtime.tools.get_object(note_id)
        self.assertTrue(fetched["ok"])
        self.assertEqual(fetched["result"]["note_title"], "Refined Note")
        self.assertEqual(fetched["result"]["sections"]["content"], "Refined note body")
        self.assertEqual(fetched["result"]["frontmatter"]["tags"], ["final"])

    def test_edit_note_defaults_to_refresh_mode(self) -> None:
        created = self.runtime.tools.create_note(
            title="Original Note",
            content="Original content",
            tags=["draft"],
        )
        self.assertTrue(created["ok"])

        edited = self.runtime.tools.edit_note(
            id=created["result"]["id"],
            sections={"content": "Updated content"},
            metadata={"title": "Updated Note"},
        )
        self.assertTrue(edited["ok"])
        self.assertEqual(edited["result"]["mode"], "refresh")

        fetched = self.runtime.tools.get_object(created["result"]["id"])
        self.assertTrue(fetched["ok"])
        self.assertEqual(fetched["result"]["note_title"], "Updated Note")
        self.assertEqual(fetched["result"]["sections"]["content"], "Updated content")

    def test_move_object_note_via_mcp_envelope(self) -> None:
        created = self.runtime.tools.create_note(
            title="Movable Note",
            content="Seed body",
        )
        self.assertTrue(created["ok"])

        moved = self.runtime.tools.move_object(
            id=created["result"]["id"],
            relative_path="archive/2026/reorg/movable-note.md",
        )
        self.assertTrue(moved["ok"])
        self.assertEqual(moved["result"]["path"], "archive/2026/reorg/movable-note.md")
        self.assertEqual(moved["result"]["note_title"], "Movable Note")

        fetched = self.runtime.tools.get_object(created["result"]["id"])
        self.assertTrue(fetched["ok"])
        self.assertEqual(fetched["result"]["path"], "archive/2026/reorg/movable-note.md")

    def test_delete_object_via_mcp_envelope(self) -> None:
        created = self.runtime.tools.create_paper_card(
            title="Disposable card",
            sections={"notes": "abstract"},
        )
        self.assertTrue(created["ok"])

        deleted = self.runtime.tools.delete_object(id=created["result"]["id"])
        self.assertTrue(deleted["ok"])
        self.assertTrue(deleted["result"]["deleted"])
        self.assertEqual(deleted["result"]["type"], "paper-card")

        fetched = self.runtime.tools.get_object(created["result"]["id"])
        self.assertFalse(fetched["ok"])
        self.assertEqual(fetched["error"]["code"], "OBJECT_NOT_FOUND")

    def test_move_object_rejects_crossing_type_boundary(self) -> None:
        created = self.runtime.tools.create_note(
            title="Protected Note",
            content="body",
        )
        self.assertTrue(created["ok"])

        moved = self.runtime.tools.move_object(
            id=created["result"]["id"],
            relative_path="library/papers/protected-note.md",
        )
        self.assertFalse(moved["ok"])
        self.assertEqual(moved["error"]["code"], "INVALID_INPUT")

    def test_create_and_edit_paper_card_via_mcp_envelope(self) -> None:
        created = self.runtime.tools.create_paper_card(
            title="Memory Systems for Research Agents",
            citekey="smith2025memory",
            sections={"notes": "raw abstract"},
            tags=["memory"],
        )
        self.assertTrue(created["ok"])
        paper_id = created["result"]["id"]

        edited = self.runtime.tools.edit_paper_card(
            id=paper_id,
            sections={
                "summary": "Typed long-term memory",
                "notes": "clean notes",
                "why_relevant": "Relevant to agent memory",
                "related_links": "[[other-paper--1234abcd]]",
            },
            metadata={"source": "arxiv"},
            mode="refresh",
        )
        self.assertTrue(edited["ok"])

        fetched = self.runtime.tools.get_object(paper_id)
        self.assertTrue(fetched["ok"])
        self.assertEqual(fetched["result"]["type"], "paper-card")
        self.assertEqual(fetched["result"]["paper_title"], "Memory Systems for Research Agents")
        self.assertEqual(fetched["result"]["sections"]["summary"], "Typed long-term memory")
        self.assertEqual(fetched["result"]["sections"]["notes"], "clean notes")
        self.assertEqual(
            fetched["result"]["sections"]["related_links"],
            "[[other-paper--1234abcd]]",
        )
        self.assertEqual(fetched["result"]["frontmatter"]["source"], "arxiv")

    def test_create_paper_card_can_seed_structured_sections(self) -> None:
        created = self.runtime.tools.create_paper_card(
            title="Equilibrium Limits of Molecular Computation",
            source="arxiv",
            status="reviewed",
            sections={
                "summary": (
                    "Equilibrium self-assembly cannot scale as a general computing substrate."
                ),
                "main_claims": "Required specificity grows with competing assemblies.",
                "limitations": "Analysis is restricted to the modeled assembly regime.",
            },
            tags=["molecular-computing"],
        )
        self.assertTrue(created["ok"])

        fetched = self.runtime.tools.get_object(created["result"]["id"])
        self.assertTrue(fetched["ok"])
        self.assertEqual(
            fetched["result"]["sections"]["summary"],
            "Equilibrium self-assembly cannot scale as a general computing substrate.",
        )
        self.assertEqual(
            fetched["result"]["sections"]["main_claims"],
            "Required specificity grows with competing assemblies.",
        )
        self.assertEqual(
            fetched["result"]["sections"]["limitations"],
            "Analysis is restricted to the modeled assembly regime.",
        )
        self.assertEqual(fetched["result"]["frontmatter"]["source"], "arxiv")
        self.assertEqual(fetched["result"]["frontmatter"]["status"], "reviewed")

    def test_create_paper_card_with_nested_relative_path(self) -> None:
        created = self.runtime.tools.create_paper_card(
            title="Nested card",
            sections={"notes": "abstract"},
            relative_path="library/papers/ml/transformers/nested-card.md",
        )
        self.assertTrue(created["ok"])
        self.assertEqual(
            created["result"]["path"],
            "library/papers/ml/transformers/nested-card.md",
        )

        fetched = self.runtime.tools.get_object(created["result"]["id"])
        self.assertTrue(fetched["ok"])
        self.assertEqual(fetched["result"]["type"], "paper-card")
        self.assertEqual(
            fetched["result"]["path"],
            "library/papers/ml/transformers/nested-card.md",
        )

    def test_create_note_rejects_protected_relative_path(self) -> None:
        response = self.runtime.tools.create_note(
            title="Protected",
            content="body",
            relative_path="library/papers/protected.md",
        )
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], "INVALID_INPUT")

    def test_duplicate_paper_card_returns_structured_error(self) -> None:
        first = self.runtime.tools.create_paper_card(
            title="Duplicate Card",
            url="https://example.com/dup",
        )
        self.assertTrue(first["ok"])

        duplicate = self.runtime.tools.invoke(
            "shardmind_create_paper_card",
            {"title": "Duplicate Card", "url": "https://example.com/another"},
        )
        self.assertFalse(duplicate["ok"])
        self.assertEqual(duplicate["error"]["code"], "DUPLICATE_OBJECT")

    def test_invalid_payload_returns_structured_error(self) -> None:
        response = self.runtime.tools.invoke("shardmind.create_note", {"content": ""})
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], "INVALID_INPUT")

    def test_unexpected_errors_return_internal_error(self) -> None:
        with patch.object(self.runtime.vault, "create_note", side_effect=RuntimeError("boom")):
            response = self.runtime.tools.create_note(content="body")
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], "INTERNAL_ERROR")

    def test_removed_fields_are_rejected_by_invoke(self) -> None:
        response = self.runtime.tools.invoke(
            "shardmind_create_paper_card",
            {
                "title": "Legacy card",
                "notes": "hello",
            },
        )
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], "INVALID_INPUT")
        self.assertIn("unexpected keyword", response["error"]["message"])

    def test_claude_safe_tool_aliases_resolve(self) -> None:
        response = self.runtime.tools.invoke(
            "shardmind_create_note",
            {"title": "Alias note", "content": "hello from Claude"},
        )
        self.assertTrue(response["ok"])
        note_id = response["result"]["id"]

        fetched = self.runtime.tools.invoke("shardmind_get_object", {"id": note_id})
        self.assertTrue(fetched["ok"])
        self.assertEqual(fetched["result"]["id"], note_id)

    def test_claude_safe_paper_card_aliases_resolve(self) -> None:
        response = self.runtime.tools.invoke(
            "shardmind_create_paper_card",
            {"title": "Alias card", "sections": {"notes": "hello from Claude"}},
        )
        self.assertTrue(response["ok"])
        paper_id = response["result"]["id"]

        fetched = self.runtime.tools.invoke("shardmind_get_object", {"id": paper_id})
        self.assertTrue(fetched["ok"])
        self.assertEqual(fetched["result"]["type"], "paper-card")

    def test_registered_tools_expose_typed_parameters(self) -> None:
        server = register_tools(FastMCP("ShardMind"), self.runtime.tools)
        create_note = server._tool_manager._tools["shardmind_create_note"]  # noqa: SLF001
        parameters = create_note.parameters
        self.assertIn("content", parameters["properties"])
        self.assertNotIn("payload", parameters["properties"])
        self.assertIn("content", parameters["required"])
        self.assertIn("wikilink", parameters["properties"]["content"]["description"].lower())
        self.assertIn("relative_path", parameters["properties"])
        self.assertIn(
            "Prefer existing tags",
            parameters["properties"]["tags"]["description"],
        )
        self.assertIn(
            "shardmind.list_tags",
            parameters["properties"]["tags"]["description"],
        )
        create_paper = server._tool_manager._tools["shardmind_create_paper_card"]  # noqa: SLF001
        self.assertIn("citekey", create_paper.parameters["properties"])
        self.assertIn("sections", create_paper.parameters["properties"])
        self.assertIn("source", create_paper.parameters["properties"])
        self.assertIn("status", create_paper.parameters["properties"])
        self.assertIn("relative_path", create_paper.parameters["properties"])
        self.assertNotIn("notes", create_paper.parameters["properties"])
        self.assertIn(
            "mottes2026gradient",
            create_paper.parameters["properties"]["citekey"]["description"],
        )
        self.assertIn(
            "Use sections.notes for raw source capture only",
            create_paper.parameters["properties"]["sections"]["description"],
        )
        self.assertIn(
            "Do not include duplicate headings",
            create_paper.parameters["properties"]["sections"]["description"],
        )
        self.assertIn(
            "shardmind_edit_paper_card",
            create_paper.parameters["properties"]["sections"]["description"],
        )
        self.assertIn(
            "Do not put a synthesized paper summary",
            create_paper.parameters["properties"]["sections"]["description"],
        )
        self.assertIn(
            "usable in one tool call",
            create_paper.parameters["properties"]["sections"]["description"],
        )
        self.assertIn(
            "Prefer existing tags",
            create_paper.parameters["properties"]["tags"]["description"],
        )
        self.assertIn(
            "shardmind.list_tags",
            create_paper.parameters["properties"]["tags"]["description"],
        )
        edit_note = server._tool_manager._tools["shardmind_edit_note"]  # noqa: SLF001
        self.assertIn("id", edit_note.parameters["required"])
        self.assertIn("sections", edit_note.parameters["properties"])
        self.assertIn(
            "Supported key: content",
            edit_note.parameters["properties"]["sections"]["description"],
        )
        self.assertIn(
            "refresh replaces existing values",
            edit_note.parameters["properties"]["mode"]["description"],
        )
        edit_paper = server._tool_manager._tools["shardmind_edit_paper_card"]  # noqa: SLF001
        self.assertIn(
            "summary=high-level takeaway in 2-4 sentences",
            edit_paper.parameters["properties"]["sections"]["description"],
        )
        self.assertIn(
            "Use these structured sections for synthesized content",
            edit_paper.parameters["properties"]["sections"]["description"],
        )
        move_object = server._tool_manager._tools["shardmind_move_object"]  # noqa: SLF001
        self.assertIn("id", move_object.parameters["required"])
        self.assertIn("relative_path", move_object.parameters["required"])
        delete_object = server._tool_manager._tools["shardmind_delete_object"]  # noqa: SLF001
        self.assertIn("id", delete_object.parameters["required"])

    def test_registered_tools_reject_unknown_fields(self) -> None:
        server = register_tools(FastMCP("ShardMind"), self.runtime.tools)
        create_note = server._tool_manager._tools["shardmind_create_note"]  # noqa: SLF001
        with self.assertRaises(ToolError):
            anyio.run(
                create_note.run,
                {"title": "Strict", "content": "body", "normalize": True},
            )

    def test_list_tags_returns_indexed_tags(self) -> None:
        note = self.runtime.tools.create_note(
            title="Tagged list",
            content="body for search",
            tags=["list-smoke", "shared-tag"],
        )
        paper = self.runtime.tools.create_paper_card(
            title="Tagged paper",
            sections={"notes": "abstract for search"},
            tags=["shared-tag", "paper-tag"],
        )
        self.assertTrue(note["ok"])
        self.assertTrue(paper["ok"])

        listed = self.runtime.tools.list_tags(limit=50)
        self.assertTrue(listed["ok"])
        self.assertEqual(
            set(listed["result"]["tags"]),
            {"list-smoke", "shared-tag", "paper-tag"},
        )

        notes_only = self.runtime.tools.list_tags(object_type="note", limit=50)
        self.assertTrue(notes_only["ok"])
        self.assertEqual(set(notes_only["result"]["tags"]), {"list-smoke", "shared-tag"})

    def test_list_tags_prunes_deleted_ghosts(self) -> None:
        kept = self.runtime.tools.create_note(
            title="Live tag note",
            content="body",
            tags=["live-tag"],
        )
        ghost = self.runtime.tools.create_note(
            title="Ghost tag note",
            content="body",
            tags=["ghost-tag"],
        )
        self.assertTrue(kept["ok"])
        self.assertTrue(ghost["ok"])

        deleted_path = self.runtime.settings.vault_path / ghost["result"]["path"]
        deleted_path.unlink()

        listed = self.runtime.tools.list_tags(limit=50)
        self.assertTrue(listed["ok"])
        self.assertEqual(set(listed["result"]["tags"]), {"live-tag"})
        self.assertIsNone(self.runtime.index.get_path(ghost["result"]["id"]))

    def test_list_objects_includes_wikilink_fields(self) -> None:
        created = self.runtime.tools.create_paper_card(
            title="Listable card",
            sections={"notes": "alpha"},
        )
        self.assertTrue(created["ok"])

        listed = self.runtime.tools.list_objects(object_type="paper-card", limit=10)
        self.assertTrue(listed["ok"])
        item = listed["result"]["objects"][0]
        self.assertEqual(item["id"], created["result"]["id"])
        self.assertEqual(item["paper_title"], "Listable card")
        self.assertEqual(item["wikilink"], Path(item["path"]).stem)

    def test_list_objects_prunes_deleted_ghosts_and_refills(self) -> None:
        first = self.runtime.tools.create_note(title="First", content="alpha")
        second = self.runtime.tools.create_note(title="Second", content="beta")
        third = self.runtime.tools.create_note(title="Third", content="gamma")
        self.assertTrue(first["ok"])
        self.assertTrue(second["ok"])
        self.assertTrue(third["ok"])

        deleted_path = self.runtime.settings.vault_path / third["result"]["path"]
        deleted_path.unlink()

        listed = self.runtime.tools.list_objects(object_type="note", limit=2)
        self.assertTrue(listed["ok"])
        objects = listed["result"]["objects"]
        self.assertEqual(len(objects), 2)
        ids = {item["id"] for item in objects}
        self.assertEqual(ids, {first["result"]["id"], second["result"]["id"]})
        self.assertIsNone(self.runtime.index.get_path(third["result"]["id"]))

    def test_list_objects_drains_more_than_three_stale_rows(self) -> None:
        created = [
            self.runtime.tools.create_note(title=f"Note {index}", content="alpha")
            for index in range(5)
        ]
        for response in created:
            self.assertTrue(response["ok"])

        for response in created[1:]:
            deleted_path = self.runtime.settings.vault_path / response["result"]["path"]
            deleted_path.unlink()

        listed = self.runtime.tools.list_objects(object_type="note", limit=1)
        self.assertTrue(listed["ok"])
        self.assertEqual(len(listed["result"]["objects"]), 1)
        self.assertEqual(listed["result"]["objects"][0]["id"], created[0]["result"]["id"])

    def test_search_repairs_moved_paths(self) -> None:
        created = self.runtime.tools.create_note(
            title="Moved note",
            content="delta repair target",
        )
        self.assertTrue(created["ok"])
        original_path = created["result"]["path"]
        source = self.runtime.settings.vault_path / original_path
        destination = self.runtime.settings.vault_path / "notes" / "scratch" / "2026" / source.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        source.replace(destination)

        searched = self.runtime.tools.search(query="delta", object_types=["note"], top_k=5)
        self.assertTrue(searched["ok"])
        result = searched["result"]["results"][0]
        self.assertEqual(result["id"], created["result"]["id"])
        self.assertEqual(
            result["path"],
            destination.relative_to(self.runtime.settings.vault_path).as_posix(),
        )
        self.assertEqual(
            self.runtime.index.get_path(created["result"]["id"]),
            result["path"],
        )
        self.assertEqual(result["note_title"], "Moved note")
        self.assertEqual(result["wikilink"], Path(result["path"]).stem)

    def test_search_prunes_deleted_ghosts(self) -> None:
        created = self.runtime.tools.create_paper_card(
            title="Ghost card",
            sections={"notes": "epsilon spectral trace"},
        )
        self.assertTrue(created["ok"])
        deleted_path = self.runtime.settings.vault_path / created["result"]["path"]
        deleted_path.unlink()

        searched = self.runtime.tools.search(
            query="spectral",
            object_types=["paper-card"],
            top_k=5,
        )
        self.assertTrue(searched["ok"])
        self.assertEqual(searched["result"]["results"], [])
        self.assertIsNone(self.runtime.index.get_path(created["result"]["id"]))
        connection = self.runtime.index.connection
        self.assertIsNotNone(connection)
        chunk_count = connection.execute(
            "SELECT COUNT(*) FROM chunks_fts WHERE document_id = ?",
            (created["result"]["id"],),
        ).fetchone()[0]
        self.assertEqual(chunk_count, 0)

    def test_get_object_tolerates_unrelated_malformed_file_during_repair(self) -> None:
        created = self.runtime.tools.create_note(title="Moved note", content="delta repair target")
        self.assertTrue(created["ok"])
        source = self.runtime.settings.vault_path / created["result"]["path"]
        destination = self.runtime.settings.vault_path / "notes" / "scratch" / "2026" / source.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        source.replace(destination)
        bad = self.runtime.settings.vault_path / "notes" / "inbox" / "broken.md"
        bad.write_text("not frontmatter", encoding="utf-8")

        fetched = self.runtime.tools.get_object(created["result"]["id"])
        self.assertTrue(fetched["ok"])
        self.assertEqual(fetched["result"]["id"], created["result"]["id"])

    def test_user_notes_remain_rejected_from_edit(self) -> None:
        created = self.runtime.tools.create_paper_card(
            title="Protected notes",
            sections={"notes": "seed"},
        )
        self.assertTrue(created["ok"])

        response = self.runtime.tools.edit_paper_card(
            id=created["result"]["id"],
            sections={"user_notes": "hands off"},
            mode="fill-empty",
        )
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], "INVALID_INPUT")
