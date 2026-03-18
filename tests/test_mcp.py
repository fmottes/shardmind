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

    def test_create_and_enrich_paper_card_via_mcp_envelope(self) -> None:
        created = self.runtime.tools.create_paper_card(
            title="Memory Systems for Research Agents",
            source_text="raw abstract",
            tags=["memory"],
        )
        self.assertTrue(created["ok"])
        paper_id = created["result"]["id"]

        enriched = self.runtime.tools.enrich_paper_card(
            id=paper_id,
            sections={
                "llm_summary": "Typed long-term memory",
                "why_relevant": "Relevant to agent memory",
            },
            metadata={"source": "arxiv"},
            mode="fill-empty",
        )
        self.assertTrue(enriched["ok"])

        fetched = self.runtime.tools.get_object(paper_id)
        self.assertTrue(fetched["ok"])
        self.assertEqual(fetched["result"]["type"], "paper-card")
        self.assertEqual(fetched["result"]["sections"]["llm_summary"], "Typed long-term memory")
        self.assertEqual(fetched["result"]["frontmatter"]["source"], "arxiv")

    def test_duplicate_paper_card_returns_structured_error(self) -> None:
        first = self.runtime.tools.create_paper_card(
            title="Duplicate Card",
            url="https://example.com/dup",
        )
        self.assertTrue(first["ok"])

        duplicate = self.runtime.tools.invoke(
            "knowledge_create_paper_card",
            {"title": "Duplicate Card", "url": "https://example.com/another"},
        )
        self.assertFalse(duplicate["ok"])
        self.assertEqual(duplicate["error"]["code"], "DUPLICATE_OBJECT")

    def test_invalid_payload_returns_structured_error(self) -> None:
        response = self.runtime.tools.invoke("knowledge.create_note", {"content": ""})
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], "INVALID_INPUT")

    def test_removed_fields_are_rejected_by_invoke(self) -> None:
        response = self.runtime.tools.invoke(
            "knowledge_create_paper_card",
            {
                "title": "Legacy card",
                "source_text": "hello",
                "generate_llm_fields": True,
            },
        )
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], "INVALID_INPUT")
        self.assertIn("unexpected keyword", response["error"]["message"])

    def test_claude_safe_tool_aliases_resolve(self) -> None:
        response = self.runtime.tools.invoke(
            "knowledge_create_note",
            {"title": "Alias note", "content": "hello from Claude"},
        )
        self.assertTrue(response["ok"])
        note_id = response["result"]["id"]

        fetched = self.runtime.tools.invoke("knowledge_get_object", {"id": note_id})
        self.assertTrue(fetched["ok"])
        self.assertEqual(fetched["result"]["id"], note_id)

    def test_claude_safe_paper_card_aliases_resolve(self) -> None:
        response = self.runtime.tools.invoke(
            "knowledge_create_paper_card",
            {"title": "Alias card", "source_text": "hello from Claude"},
        )
        self.assertTrue(response["ok"])
        paper_id = response["result"]["id"]

        fetched = self.runtime.tools.invoke("knowledge_get_object", {"id": paper_id})
        self.assertTrue(fetched["ok"])
        self.assertEqual(fetched["result"]["type"], "paper-card")

    def test_registered_tools_expose_typed_parameters(self) -> None:
        server = register_tools(FastMCP("ShardMind"), self.runtime.tools)
        create_note = server._tool_manager._tools["knowledge_create_note"]  # noqa: SLF001
        parameters = create_note.parameters
        self.assertIn("content", parameters["properties"])
        self.assertNotIn("payload", parameters["properties"])
        self.assertIn("content", parameters["required"])

    def test_registered_tools_reject_unknown_fields(self) -> None:
        server = register_tools(FastMCP("ShardMind"), self.runtime.tools)
        create_note = server._tool_manager._tools["knowledge_create_note"]  # noqa: SLF001
        with self.assertRaises(ToolError):
            anyio.run(
                create_note.run,
                {"title": "Strict", "content": "body", "normalize": True},
            )
