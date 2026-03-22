# ShardMind

ShardMind is an MCP-first local research memory system.

Current state:
- notes and paper cards are stored as canonical Markdown in an Obsidian-style vault
- note and paper-card files can live in nested subfolders within their allowed roots
- the MCP server supports deterministic create/read/list/search flows for both object types
- paper-card editing is a structured patch operation driven by the MCP client
- search is still lexical-only in the current milestone; real semantic ranking is deferred

## Install

Requirements:
- Python 3.10+
- `uv`

Install dependencies:

```bash
uv sync
```

Optional checks:

```bash
uv run ruff check .
uv run python -m unittest discover -s tests -v
```

## Run

By default, ShardMind uses `~/Documents/ShardMind` as its vault if `SHARDMIND_VAULT_PATH` is not
set. On first startup, it creates the required folder structure inside that vault.

Useful commands:

```bash
uv run shardmind init-vault
uv run shardmind reindex-all
uv run shardmind-mcp
```

You can also override paths explicitly:

```bash
export SHARDMIND_VAULT_PATH="$HOME/Documents/ShardMind"
export SHARDMIND_SQLITE_PATH="$HOME/Library/Application Support/shardmind/shardmind.sqlite3"
uv run shardmind-mcp
```

## Claude Desktop MCP Setup

Claude Desktop can launch ShardMind for you as a local MCP server over stdio. You do not need to
start it manually in a separate terminal during normal use.

Note: MCPB-style support for the newer in-app path should be added later. For the moment, use the
current config-edit route in Claude Desktop:

1. Open `Claude Desktop`.
2. Go to `Settings > Developer`.
3. Click `Edit Config`.
4. Add the `ShardMind` MCP server entry below to the config JSON.

```json
{
  "mcpServers": {
    "ShardMind": {
      "type": "stdio",
      "command": "/opt/homebrew/bin/uv",
      "args": [
        "--directory",
        "/absolute/path/to/shardmind",
        "run",
        "--frozen",
        "shardmind-mcp"
      ],
      "env": {
        "SHARDMIND_VAULT_PATH": "/Users/yourname/Documents/ShardMind",
        "SHARDMIND_SQLITE_PATH": "/Users/yourname/Library/Application Support/shardmind/shardmind.sqlite3"
      }
    }
  }
}
```

If your config already contains other top-level keys such as `preferences`, keep them and merge in
the `mcpServers.ShardMind` block.

After saving the config:

1. Quit Claude Desktop completely.
2. Reopen Claude Desktop.
3. Start a new chat.
4. Try prompts like:
   - `Use ShardMind to create a note titled "test note" with content "hello from Claude".`
   - `Use ShardMind to create a note with relative_path "archive/2026/test-note.md" and content "hello from Claude".`
   - `Use ShardMind to create a paper card titled "test paper" with sections.notes set to "example abstract".`
   - `Use ShardMind to create a paper card with relative_path "library/papers/ml/test-paper.md" and sections.notes set to "example abstract".`
   - `Use ShardMind to search for "hello".`

Current exported MCP tools:
- `shardmind_create_note`
- `shardmind_append_to_note`
- `shardmind_edit_note`
- `shardmind_create_paper_card`
- `shardmind_edit_paper_card`
- `shardmind_get_object`
- `shardmind_list_objects`
- `shardmind_list_tags`
- `shardmind_search`

## Suggested Prompts

Once Claude Desktop is connected to the `ShardMind` MCP server, prompts like these should work
well:


- `Summarize this conversation and save it as a note in ShardMind titled "memory architecture recap".`
- `Find [relevant paper] online and save a paper card for it in ShardMind.`
- `Search ShardMind for my notes and paper cards about memory systems.`

## Notes

- `dev-docs/` is scratch/reference material and not part of the runtime product surface.
- The vault is canonical; the SQLite index is derived and can be rebuilt.
- `system/**` is non-indexable and reserved for ShardMind internals.
- `assets/**` is attachment storage, not note or paper-card storage.
- `library/papers/**` is reserved for paper cards and their subfolders.
- Notes may be created under `notes/**`, `archive/**`, or `library/**` except `library/papers/**`.
- `shardmind_create_note` and `shardmind_create_paper_card` accept optional `relative_path`
  parameters for explicit nested placement; create/edit flows remain ID-based after creation.
- `uv run shardmind reindex-all` is the supported repair path after manual vault edits or index drift.
- `shardmind_get_object`, `shardmind_list_objects`, and `shardmind_search` return `note_title` or
  `paper_title` plus a `wikilink` file stem so MCP clients can create correct Obsidian links
  without confusing frontmatter title with link target.
- Server-side LLM generation is intentionally not implemented in the current milestone.
- Server-side note normalization is intentionally not implemented in the current milestone.
