# ShardMind Agent Guide

## Purpose

This repo is building ShardMind as an MCP-first local research memory system. The primary product
surface is the MCP server; the local CLI exists to bootstrap the vault and exercise tools during
development.

Milestone 2 includes deterministic paper-card support through MCP. Real semantic retrieval and
server-side LLM generation are intentionally deferred.

## Source Of Truth

- Runtime code must not depend on `dev-docs/`.
- Tracked runtime schemas and templates live in `shared/`.
- The vault on disk is canonical. The SQLite index is derived and rebuildable.
- The default runtime vault is a user-level `~/Documents/ShardMind` vault, not a repo-local one.
- That vault must be treated as a normal Obsidian vault, not as an exclusively owned application
  directory.

## Vault Path Policy

- `system/**` is reserved for ShardMind internals and is non-indexable.
- `assets/**` is attachment storage and is non-indexable.
- `library/papers/**` is a protected paper-card-only subtree, including nested subfolders.
- Notes may live under `notes/**`, `archive/**`, and `library/**` except `library/papers/**`.
- ShardMind object discovery and reindexing are recursive across supported Markdown subfolders.
- `shardmind.create_note` accepts optional `relative_path` for explicit placement in allowed note
  roots.
- `shardmind.create_paper_card` accepts optional `relative_path` constrained to
  `library/papers/**`.
- `shardmind.move_object` moves an existing object by id to a new allowed `relative_path` while
  preserving object type and id.
- `shardmind.delete_object` deletes an existing object by id and removes it from the derived index.
- `destination` remains supported for note creation as a backward-compatible shortcut, but it is
  mutually exclusive with `relative_path`.

## Working Rules

- Use `uv` for dependency management and Python commands.
- Use `ruff` for linting and formatting.
- Keep implementations stdlib-first unless a dependency is needed for the product surface.
- The MCP surface is the main interface. Prefer changes that improve the server contract, tool
  registration, and runtime behavior before adding local-only helpers.
- Preserve typed write paths through the vault/index/tool layers. Do not add arbitrary file edits.
- Keep repo structure aligned with the current layering:
  - `src/shardmind/vault/`
  - `src/shardmind/index/`
  - `src/shardmind/mcp/`
  - `shared/`

## Commands

```bash
UV_CACHE_DIR=.uv-cache uv run ruff check .
UV_CACHE_DIR=.uv-cache uv run ruff format .
UV_CACHE_DIR=.uv-cache uv run python -m unittest discover -s tests -v
UV_CACHE_DIR=.uv-cache uv run shardmind-mcp
UV_CACHE_DIR=.uv-cache uv run shardmind serve-mcp
UV_CACHE_DIR=.uv-cache uv run shardmind init-vault
UV_CACHE_DIR=.uv-cache uv run shardmind invoke shardmind.create_note '{"title":"Example","content":"Hello"}'
```

## Current Constraints

- `shardmind.search` is lexical-only in Milestone 2; semantic ranking is deferred to Milestone 3.
- `shardmind.create_paper_card` writes sparse deterministic paper cards.
- `shardmind.create_note` and `shardmind.create_paper_card` support explicit nested placement via
  `relative_path`, but edits remain id-based after creation.
- `shardmind.move_object` and `shardmind.delete_object` are the intended MCP tools for
  reorganization agents; do not bypass them with arbitrary file edits.
- `shardmind.edit_paper_card` applies structured section patches from the MCP client; it does
  not generate LLM content server-side.
- `shardmind.append_to_note` still appends only to the note `Content` section.
