"""CLI helpers for bootstrap and local tool invocation."""

from __future__ import annotations

import argparse
import json
import sys

from shardmind.bootstrap import build_runtime
from shardmind.mcp.main import run_server
from shardmind.vault.bootstrap import bootstrap_vault


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="shardmind")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-vault", help="Create the canonical ShardMind vault layout.")
    subparsers.add_parser("reindex-all", help="Rebuild the derived SQLite index from the vault.")

    invoke_parser = subparsers.add_parser("invoke", help="Invoke a tool with a JSON payload.")
    invoke_parser.add_argument("tool_name")
    invoke_parser.add_argument("payload", help="JSON payload for the tool.")
    subparsers.add_parser("serve-mcp", help="Run the MCP stdio server.")

    args = parser.parse_args(argv)
    runtime = build_runtime()
    try:
        if args.command == "init-vault":
            bootstrap_vault(runtime.settings.vault_path)
            print(runtime.settings.vault_path)
            return 0

        if args.command == "reindex-all":
            records, skipped_paths = runtime.vault.list_indexable_objects()
            runtime.index.rebuild(records)
            print(len(records))
            if skipped_paths:
                print(
                    f"Skipped {len(skipped_paths)} malformed file(s): {', '.join(skipped_paths)}",
                    file=sys.stderr,
                )
            return 0

        if args.command == "invoke":
            payload = json.loads(args.payload)
            response = runtime.tools.invoke(args.tool_name, payload)
            print(json.dumps(response, indent=2, sort_keys=True))
            return 0 if response.get("ok") else 1

        if args.command == "serve-mcp":
            return run_server(runtime.tools)

        parser.print_help(sys.stderr)
        return 1
    finally:
        runtime.close()
