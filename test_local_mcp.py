import socket
import sys
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path

import anyio
import pytest

from plugins.ghost_cursor import local_mcp


def test_configured_servers_builds_bundled_stdio_definition():
    servers = local_mcp.configured_servers({
        "paper": {"url": "http://127.0.0.1:29979/mcp"},
    })

    assert servers == [{
        "name": "paper",
        "type": "stdio",
        "command": sys.executable,
        "args": [
            str(Path(local_mcp.__file__).with_name("mcp_http_proxy.py").resolve()),
            "http://127.0.0.1:29979/mcp",
        ],
    }]


@pytest.mark.parametrize("raw", [
    [],
    {"bad name": {"url": "http://127.0.0.1/mcp"}},
    {"paper": "http://127.0.0.1/mcp"},
    {"paper": {"url": "file:///tmp/mcp"}},
    {"paper": {"url": "http://user@127.0.0.1/mcp"}},
    {"paper": {"url": "http://127.0.0.1/mcp?token=nope"}},
    {"paper": {"url": "http://127.0.0.1:99999/mcp"}},
    {"paper": {"url": "http://127.0.0.1/mcp", "headers": {}}},
])
def test_malformed_config_is_ignored(raw):
    assert local_mcp.configured_servers(raw) == []


def test_stdio_proxy_mirrors_tools_content_and_errors():
    pytest.importorskip("uvicorn")
    pytest.importorskip("starlette")
    from mcp import ClientSession
    from mcp.client.stdio import StdioServerParameters, stdio_client
    from mcp.server.lowlevel import Server
    from mcp.server.streamable_http_manager import (
        StreamableHTTPASGIApp,
        StreamableHTTPSessionManager,
    )
    from mcp.types import (
        CallToolResult,
        ImageContent,
        ListToolsResult,
        TextContent,
        Tool,
    )
    from starlette.applications import Starlette
    from starlette.routing import Mount
    import uvicorn

    async def list_tools(_context, _params):
        return ListToolsResult(tools=[
            Tool(name="mixed", description="mixed content", inputSchema={
                "type": "object",
                "properties": {"value": {"type": "string"}},
                "required": ["value"],
            }),
            Tool(name="failure", description="upstream error", inputSchema={
                "type": "object",
                "properties": {},
            }),
        ])

    async def call_tool(_context, params):
        if params.name == "failure":
            return CallToolResult(
                content=[TextContent(type="text", text="paper failed")],
                isError=True,
            )
        return CallToolResult(content=[
            TextContent(type="text", text=params.arguments["value"]),
            ImageContent(type="image", data="aGVsbG8=", mimeType="image/png"),
        ])

    upstream = Server(
        "tiny-upstream",
        on_list_tools=list_tools,
        on_call_tool=call_tool,
    )
    manager = StreamableHTTPSessionManager(upstream, stateless=True)

    @asynccontextmanager
    async def lifespan(_app):
        async with manager.run():
            yield

    app = Starlette(
        routes=[Mount("/mcp", app=StreamableHTTPASGIApp(manager))],
        lifespan=lifespan,
    )
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    config = uvicorn.Config(
        app, host="127.0.0.1", port=port, log_level="error", lifespan="on"
    )
    http_server = uvicorn.Server(config)
    thread = threading.Thread(target=http_server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 5
    while not http_server.started and time.monotonic() < deadline:
        time.sleep(0.01)
    assert http_server.started

    async def exercise():
        proxy = Path(local_mcp.__file__).with_name("mcp_http_proxy.py")
        params = StdioServerParameters(
            command=sys.executable,
            args=[str(proxy), f"http://127.0.0.1:{port}/mcp"],
        )
        async with stdio_client(params) as streams:
            async with ClientSession(streams[0], streams[1]) as client:
                await client.initialize()
                listed = await client.list_tools()
                assert [tool.name for tool in listed.tools] == ["mixed", "failure"]
                assert listed.tools[0].input_schema["required"] == ["value"]

                mixed = await client.call_tool("mixed", {"value": "from paper"})
                assert isinstance(mixed.content[0], TextContent)
                assert mixed.content[0].text == "from paper"
                assert isinstance(mixed.content[1], ImageContent)
                assert mixed.content[1].data == "aGVsbG8="

                failure = await client.call_tool("failure", {})
                assert failure.is_error is True
                assert failure.content[0].text == "paper failed"

    try:
        anyio.run(exercise)
    finally:
        http_server.should_exit = True
        thread.join(timeout=5)
