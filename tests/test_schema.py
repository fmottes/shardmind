from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from shardmind.bootstrap import build_runtime
from shardmind.errors import SchemaValidationError

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class SchemaValidationTest(unittest.TestCase):
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

    def test_note_validation_rejects_invalid_datetime(self) -> None:
        note, _ = self.runtime.vault.create_note(title="Invalid time", content="body")
        note.created_at = "not-a-date"
        with self.assertRaisesRegex(SchemaValidationError, "ISO 8601"):
            self.runtime.schema_store.validate_note(note)

    def test_paper_card_validation_rejects_invalid_uri(self) -> None:
        paper_card, _ = self.runtime.vault.create_paper_card(
            title="Bad URL",
            source_text="body",
        )
        paper_card.url = "not a uri"
        with self.assertRaisesRegex(SchemaValidationError, "valid URI"):
            self.runtime.schema_store.validate_paper_card(paper_card)
