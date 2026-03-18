"""MCP stdio bridge."""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP
from pydantic import ConfigDict

from shardmind.bootstrap import build_runtime
from shardmind.mcp.registry import iter_tool_specs
from shardmind.mcp.tools import KnowledgeTools


def register_tools(server: FastMCP, tools: KnowledgeTools) -> FastMCP:
    """Register the current MCP tool surface onto a FastMCP server."""
    for spec in iter_tool_specs(KnowledgeTools):
        method = getattr(tools, spec.method_name)
        server.tool(name=spec.exported_name)(method)
        registered = server._tool_manager._tools[spec.exported_name]  # noqa: SLF001
        registered.fn_metadata.arg_model.model_config = ConfigDict(
            extra="forbid",
            arbitrary_types_allowed=True,
        )
        registered.fn_metadata.arg_model.model_rebuild(force=True)
        registered.parameters = registered.fn_metadata.arg_model.model_json_schema(by_alias=True)
    return server


def run_server(tools: KnowledgeTools) -> int:
    server = register_tools(FastMCP("ShardMind"), tools)
    try:
        server.run()
        return 0
    finally:
        tools.index.close()


def main() -> int:
    runtime = build_runtime()
    return run_server(runtime.tools)
