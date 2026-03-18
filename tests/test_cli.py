from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from shardmind.cli import main

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class CLITest(unittest.TestCase):
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

    def test_reindex_all_rebuilds_index_from_vault(self) -> None:
        with redirect_stdout(io.StringIO()):
            self.assertEqual(
                main(["invoke", "knowledge_create_note", '{"title":"one","content":"body"}']),
                0,
            )
            self.assertEqual(
                main(["invoke", "knowledge_create_note", '{"title":"two","content":"body"}']),
                0,
            )
        out = io.StringIO()
        with redirect_stdout(out):
            result = main(["reindex-all"])
        self.assertEqual(result, 0)
        self.assertEqual(out.getvalue().strip(), "2")
