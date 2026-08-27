"""Host-local MCP configuration for self-hosted Cursor workers."""

from __future__ import annotations

import logging
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)

_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_PROXY_PATH = Path(__file__).with_name("mcp_http_proxy.py").resolve()


def _raw_config() -> Any:
    from hermes_cli.config import cfg_get, read_raw_config

    return cfg_get(
        read_raw_config(), "plugins", "ghost_cursor", "local_mcp_servers"
    )


def _valid_url(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    url = value.strip()
    try:
        parsed = urlsplit(url)
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            return None
        # Accessing port validates malformed/non-numeric/out-of-range ports.
        _ = parsed.port
    except ValueError:
        return None
    return url


def configured_servers(raw: Any = None) -> List[Dict[str, Any]]:
    """Validated Cursor API stdio definitions, or an empty safe fallback."""
    if raw is None:
        try:
            raw = _raw_config()
        except Exception:
            logger.debug("ghost_cursor local MCP config read failed", exc_info=True)
            return []
    if raw is None:
        return []
    if not isinstance(raw, dict):
        logger.warning(
            "ghost_cursor: ignoring local_mcp_servers: expected a mapping"
        )
        return []

    servers: List[Dict[str, Any]] = []
    for name, config in raw.items():
        if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
            logger.warning("ghost_cursor: ignoring local MCP with invalid name %r", name)
            continue
        url = _valid_url(config.get("url") if isinstance(config, dict) else None)
        if url is None or set(config) != {"url"}:
            logger.warning(
                "ghost_cursor: ignoring local MCP %r: expected only a valid "
                "http(s) url",
                name,
            )
            continue
        servers.append(
            {
                "name": name,
                "type": "stdio",
                "command": sys.executable,
                "args": [str(_PROXY_PATH), url],
            }
        )
    return servers
