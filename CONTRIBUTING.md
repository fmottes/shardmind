# Contributing

ShardMind is still a small private project. Keep changes narrow, typed, and aligned with the MCP-first runtime surface.

## Setup

Requirements:
- Python 3.10+
- `uv`

From a fresh clone:

```bash
uv sync --extra dev
```

## Local Checks

Run these before opening a pull request:

```bash
uv run ruff check .
uv run ruff format --check .
uv run python -m unittest discover -s tests -v
uv build
```

## Pull Requests

- Branch from `main`.
- Open a pull request back into `main`.
- Wait for the GitHub Actions `CI` workflow to pass before merging.

## Testing Rules

- Tests must keep using temporary directories and test-specific environment overrides.
- Do not point tests at a real `~/Documents/ShardMind` vault or a real user SQLite path.
- New tests should preserve the current pattern of isolated vault/index setup.
