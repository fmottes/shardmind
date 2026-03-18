"""Runtime assembly helpers."""

from __future__ import annotations

from dataclasses import dataclass

from shardmind.config import Settings
from shardmind.index.service import IndexService
from shardmind.mcp.tools import KnowledgeTools
from shardmind.schemas import SchemaStore
from shardmind.vault.service import VaultService


@dataclass(slots=True)
class Runtime:
    settings: Settings
    schema_store: SchemaStore
    vault: VaultService
    index: IndexService
    tools: KnowledgeTools

    def close(self) -> None:
        self.index.close()


def build_runtime() -> Runtime:
    settings = Settings.load()
    schema_store = SchemaStore(settings.shared_path)
    index = IndexService(settings.sqlite_path)
    vault = VaultService(settings.vault_path, schema_store, index=index)
    tools = KnowledgeTools(vault=vault, index=index)
    return Runtime(
        settings=settings,
        schema_store=schema_store,
        vault=vault,
        index=index,
        tools=tools,
    )
