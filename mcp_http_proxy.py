#!/usr/bin/env python3
"""Expose a Streamable HTTP MCP server as a local stdio MCP server."""

from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager
from urllib.parse import urlsplit

import anyio
from mcp import ClientSession
from mcp.client import streamable_http
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server


def _upstream_url(argv: list[str]) -> str:
    if len(argv) != 2:
        raise ValueError("usage: mcp_http_proxy.py <http(s)://host/path>")
    url = argv[1].strip()
    parsed = urlsplit(url)
    if (
        parsed.scheme not in ("http", "https")
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("upstream must be an http(s) URL without credentials")
    _ = parsed.port
    return url


async def _serve(url: str) -> None:
    @asynccontextmanager
    async def lifespan(_server):
        async with streamable_http.streamable_http_client(url) as streams:
            async with ClientSession(streams[0], streams[1]) as session:
                await session.initialize()
                yield session

    async def list_tools(context, params):
        return await context.lifespan_context.list_tools(params=params)

    async def call_tool(context, params):
        # Return the SDK object unchanged so all content variants,
        # structured content, and upstream isError values survive.
        return await context.lifespan_context.call_tool(
            params.name, params.arguments
        )

    server = Server(
        "ghost-cursor-http-proxy",
        version="1",
        lifespan=lifespan,
        on_list_tools=list_tools,
        on_call_tool=call_tool,
    )
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
            raise_exceptions=False,
        )


def main() -> int:
    # stdout is exclusively the MCP protocol transport.
    logging.basicConfig(
        level=logging.WARNING,
        stream=sys.stderr,
        format="mcp-http-proxy: %(levelname)s: %(message)s",
    )
    try:
        url = _upstream_url(sys.argv)
        anyio.run(_serve, url)
        return 0
    except BaseException as exc:
        print(
            f"mcp-http-proxy: failed: {type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
