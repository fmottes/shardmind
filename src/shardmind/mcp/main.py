"""MCP stdio bridge."""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP
from pydantic import ConfigDict

from shardmind.bootstrap import build_runtime
from shardmind.mcp.registry import iter_tool_specs
from shardmind.mcp.tools import KnowledgeTools


def _apply_strict_arg_model_config(server: FastMCP, tool_name: str) -> None:
    tool_manager = getattr(server, "_tool_manager", None)
    if tool_manager is None:
        raise RuntimeError("FastMCP internals changed: missing _tool_manager.")
    get_tool = getattr(tool_manager, "get_tool", None)
    if get_tool is None:
        raise RuntimeError("FastMCP internals changed: missing ToolManager.get_tool().")
    registered = get_tool(tool_name)
    if registered is None:
        raise RuntimeError(f"FastMCP registration failed for tool '{tool_name}'.")
    fn_metadata = getattr(registered, "fn_metadata", None)
    arg_model = getattr(fn_metadata, "arg_model", None)
    if arg_model is None:
        raise RuntimeError("FastMCP internals changed: missing fn_metadata.arg_model.")
    if not hasattr(arg_model, "model_rebuild") or not hasattr(arg_model, "model_json_schema"):
        raise RuntimeError("FastMCP internals changed: arg_model is not a Pydantic model.")
    arg_model.model_config = ConfigDict(
        extra="forbid",
        arbitrary_types_allowed=True,
    )
    arg_model.model_rebuild(force=True)
    registered.parameters = arg_model.model_json_schema(by_alias=True)


def register_tools(server: FastMCP, tools: KnowledgeTools) -> FastMCP:
    """Register the current MCP tool surface onto a FastMCP server."""
    for spec in iter_tool_specs(KnowledgeTools):
        method = getattr(tools, spec.method_name)
        server.tool(name=spec.exported_name)(method)
        _apply_strict_arg_model_config(server, spec.exported_name)
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
