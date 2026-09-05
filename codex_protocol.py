"""Codex app-server wire layer: newline JSON-RPC over stdio + normalizer.

Stdlib only, no package imports: ``codex_controller.py`` runs this outside
the gateway. Protocol per https://developers.openai.com/codex/app-server/
(``initialize``/``initialized``, ``thread/start``/``thread/resume``,
``turn/start``/``turn/steer``/``turn/interrupt``, ``model/list``, and the
``item/*`` / ``turn/*`` notifications). The ``jsonrpc`` header is omitted on
the wire, as the server does.

Normalization keeps the plugin's existing envelope vocabulary (``content``,
``tool_use``, ``tool_result``, ``file_diff``, ``lifecycle``) and carries the
native ids (``codex_item_id``, ``turn_id``, ``thread_id``) on every envelope.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import subprocess
import threading
import time
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

CLIENT_INFO = {"name": "hermes_cursor_codex", "title": "Hermes Cursor (Codex backend)", "version": "0.7.0"}

SOURCE = "ghost"
TOOL_SHELL = "shell"
TOOL_FILE_EDIT = "file-edit"
TOOL_MCP = "mcp"
TOOL_SEARCH = "search"
TOOL_PLAN = "plan"
STATUS_DONE = "done"
STATUS_ERROR = "error"

# Server-initiated requests we answer by declining (v1 has no approval
# broker). The turn then ends with the item declined instead of hanging.
APPROVAL_METHODS = (
    "item/commandExecution/requestApproval",
    "item/fileChange/requestApproval",
    "item/permissions/requestApproval",
)
INPUT_METHODS = ("item/tool/requestUserInput", "mcpServer/elicitation/request")

MAX_OUTPUT_CHARS = 200_000
MAX_DIFF_CHARS = 100_000


class ProtocolError(RuntimeError):
    """Transport or JSON-RPC failure (message is user-safe)."""


class RpcError(ProtocolError):
    def __init__(self, code: Any, message: str, data: Any = None) -> None:
        super().__init__(f"codex app-server error {code}: {message}")
        self.code = code
        self.message = message
        self.data = data


def group_members(pgid: int) -> List[int]:
    """Live (non-zombie) pids whose process group is ``pgid``, from /proc."""
    out: List[int] = []
    if pgid <= 0:
        return out
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            with open(f"/proc/{name}/stat", "r") as fh:
                rest = fh.read().rsplit(")", 1)[1].split()
        except Exception:
            continue
        # rest[0] = state, rest[1] = ppid, rest[2] = pgrp
        if len(rest) > 2 and rest[2] == str(pgid) and rest[0] != "Z":
            out.append(int(name))
    return out


def group_empty(pgid: int) -> bool:
    return not group_members(pgid)


def terminate_group(pgid: int, grace_s: float) -> bool:
    """SIGTERM, then SIGKILL, the whole group; True only when it is observed
    empty. A group whose leader is gone but still has members is ours (Linux
    never reuses a pid that is still a live pgid), so signalling is safe."""
    if pgid <= 0:
        return True
    deadline = time.monotonic() + max(grace_s, 0.1)
    for sig in (15, 9):
        if group_empty(pgid):
            return True
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            return group_empty(pgid)
        except PermissionError:
            return False
        while time.monotonic() < deadline:
            if group_empty(pgid):
                return True
            time.sleep(0.05)
        deadline = time.monotonic() + max(grace_s, 0.1)
    return group_empty(pgid)


def _clip(text: Any, limit: int) -> str:
    s = str(text or "")
    return s if len(s) <= limit else s[:limit] + f"\n… [truncated {len(s) - limit} chars]"


class AppServer:
    """One ``codex app-server`` child on stdio.

    A reader thread routes replies to waiters and hands notifications and
    server requests to ``on_notification`` / ``on_request`` (called on the
    reader thread; keep them short — the controller queues them).
    """

    def __init__(
        self,
        codex_bin: str,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        extra_args: Optional[List[str]] = None,
        on_notification: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_request: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_exit: Optional[Callable[[Optional[int]], None]] = None,
    ) -> None:
        self._on_notification = on_notification
        self._on_request = on_request
        self._on_exit = on_exit
        self._pending: Dict[int, "queue.Queue[Dict[str, Any]]"] = {}
        self._lock = threading.Lock()
        self._next_id = 1
        self._closed = False
        self.stderr_tail: List[str] = []
        spawn_env = dict(env) if env is not None else dict(os.environ)
        spawn_env.setdefault("RUST_LOG", "warn")
        cmd = [codex_bin, "app-server", *(extra_args or [])]
        self._proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
            cwd=cwd,
            env=spawn_env,
            start_new_session=True,
        )
        self.pid = self._proc.pid
        self.pgid = self._proc.pid  # start_new_session=True: leader of its own group
        threading.Thread(target=self._read_loop, name="codex-appserver-stdout", daemon=True).start()
        threading.Thread(target=self._stderr_loop, name="codex-appserver-stderr", daemon=True).start()

    # -- lifecycle -----------------------------------------------------------

    def alive(self) -> bool:
        return not self._closed and self._proc.poll() is None

    def returncode(self) -> Optional[int]:
        return self._proc.poll()

    def close(self, grace_s: float = 5.0) -> bool:
        """Stop the app-server AND everything it spawned.

        The child was started as a session/process-group leader, so the
        whole group is signalled — a parent that exits cleanly on stdin EOF
        never signals its own descendants (tool shells), so the parent's
        exit alone is not termination evidence. Returns True only when the
        process group is OBSERVED empty afterwards (:func:`group_empty`);
        False means owned execution may still be running.
        """
        self._closed = True
        try:
            if self._proc.stdin:
                self._proc.stdin.close()
        except Exception:
            pass
        try:
            self._proc.wait(timeout=grace_s)
        except Exception:
            pass
        proven = terminate_group(self.pgid, grace_s)
        try:
            self._proc.wait(timeout=0.1)  # reap the leader if it is ours
        except Exception:
            pass
        with self._lock:
            waiters = list(self._pending.values())
            self._pending.clear()
        for q in waiters:
            q.put({"_transport": True, "error": {"code": -1, "message": "app-server closed"}})
        return proven

    def group_empty(self) -> bool:
        """True when no process in the app-server's group remains."""
        return group_empty(self.pgid)

    # -- wire ----------------------------------------------------------------

    def _send(self, msg: Dict[str, Any]) -> None:
        line = json.dumps(msg, ensure_ascii=False) + "\n"
        try:
            self._proc.stdin.write(line.encode("utf-8"))
            self._proc.stdin.flush()
        except Exception as exc:
            raise ProtocolError(f"codex app-server stdin closed: {exc}") from exc

    def request(self, method: str, params: Optional[Dict[str, Any]] = None, timeout: float = 60.0) -> Dict[str, Any]:
        if self._closed:
            raise ProtocolError("codex app-server is closed")
        q: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        with self._lock:
            rid = self._next_id
            self._next_id += 1
            self._pending[rid] = q
        self._send({"id": rid, "method": method, "params": params or {}})
        try:
            msg = q.get(timeout=timeout)
        except queue.Empty:
            with self._lock:
                self._pending.pop(rid, None)
            raise ProtocolError(f"codex app-server {method!r} timed out after {timeout:g}s")
        if msg.get("_transport"):
            # Synthesized locally when the pipe closed: NOT a provider answer.
            # Callers must treat it as an ambiguous transport outcome.
            raise ProtocolError(f"codex app-server {method!r}: {(msg.get('error') or {}).get('message')}")
        if "error" in msg and msg["error"] is not None:
            err = msg["error"] or {}
            raise RpcError(err.get("code"), str(err.get("message") or "unknown error"), err.get("data"))
        result = msg.get("result")
        return result if isinstance(result, dict) else {}

    def notify(self, method: str, params: Optional[Dict[str, Any]] = None) -> None:
        self._send({"method": method, "params": params or {}})

    def respond(self, request_id: Any, result: Dict[str, Any]) -> None:
        self._send({"id": request_id, "result": result})

    def initialize(self, timeout: float = 30.0) -> Dict[str, Any]:
        result = self.request("initialize", {"clientInfo": dict(CLIENT_INFO), "capabilities": {}}, timeout=timeout)
        self.notify("initialized")
        return result

    def _read_loop(self) -> None:
        try:
            for raw in self._proc.stdout:
                line = raw.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except Exception:
                    logger.debug("codex app-server non-json stdout line: %.200s", line)
                    continue
                if not isinstance(msg, dict):
                    continue
                self._route(msg)
        except Exception:
            logger.debug("codex app-server stdout reader ended", exc_info=True)
        finally:
            code = self._proc.poll()
            if code is None:
                try:
                    code = self._proc.wait(timeout=2)
                except Exception:
                    code = None
            with self._lock:
                waiters = list(self._pending.values())
                self._pending.clear()
            for q in waiters:
                q.put({"_transport": True, "error": {"code": -1, "message": f"app-server exited (code {code})"}})
            if self._on_exit is not None:
                try:
                    self._on_exit(code)
                except Exception:
                    logger.debug("on_exit failed", exc_info=True)

    def _route(self, msg: Dict[str, Any]) -> None:
        has_id = "id" in msg
        if has_id and "method" in msg:
            if self._on_request is not None:
                self._on_request(msg)
            return
        if has_id:
            with self._lock:
                q = self._pending.pop(msg.get("id"), None)
            if q is not None:
                q.put(msg)
            return
        if "method" in msg and self._on_notification is not None:
            self._on_notification(msg)

    def _stderr_loop(self) -> None:
        try:
            for raw in self._proc.stderr:
                text = raw.decode("utf-8", errors="replace").rstrip()
                if text:
                    self.stderr_tail.append(text[:500])
                    del self.stderr_tail[:-50]
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Catalog helpers
# ---------------------------------------------------------------------------

def list_models(server: AppServer, timeout: float = 30.0) -> List[Dict[str, Any]]:
    """Every catalog entry (hidden included), following ``nextCursor``."""
    out: List[Dict[str, Any]] = []
    cursor: Any = None
    for _ in range(20):
        params: Dict[str, Any] = {"limit": 100, "includeHidden": True}
        if cursor:
            params["cursor"] = cursor
        result = server.request("model/list", params, timeout=timeout)
        data = result.get("data")
        if isinstance(data, list):
            out.extend(m for m in data if isinstance(m, dict))
        cursor = result.get("nextCursor")
        if not cursor:
            break
    return out


def resolve_model(catalog: List[Dict[str, Any]], model: str, effort: Optional[str]) -> Dict[str, Any]:
    """Exact catalog match for ``model`` (id or ``model`` field) and effort.

    Never substitutes: an unknown model or unsupported effort raises a
    ``ProtocolError`` naming the available ids/efforts.
    """
    want = str(model or "").strip()
    match = None
    for m in catalog:
        if want and want in (str(m.get("id") or ""), str(m.get("model") or "")):
            match = m
            break
    if match is None:
        ids = sorted({str(m.get("id") or m.get("model") or "") for m in catalog} - {""})
        raise ProtocolError(
            f"model {want!r} is not in this Codex install's catalog; available: "
            + (", ".join(ids) if ids else "(empty catalog)")
        )
    efforts = [
        str(e.get("reasoningEffort") if isinstance(e, dict) else e)
        for e in (match.get("supportedReasoningEfforts") or [])
    ]
    chosen = str(effort or "").strip() or str(match.get("defaultReasoningEffort") or "")
    if chosen and efforts and chosen not in efforts:
        raise ProtocolError(
            f"effort {chosen!r} is not supported by {want!r}; supported: {', '.join(efforts)}"
        )
    return {"model": str(match.get("model") or match.get("id") or want), "id": str(match.get("id") or want),
            "effort": chosen or None, "supported_efforts": efforts}


# ---------------------------------------------------------------------------
# Normalizer: notifications -> envelopes
# ---------------------------------------------------------------------------

def _envelope(kind: str, **payload: Any) -> Dict[str, Any]:
    return {"source": SOURCE, "kind": kind, **payload}


def lifecycle(event: str, **payload: Any) -> Dict[str, Any]:
    return _envelope("lifecycle", event=event, **payload)


def _diff_counts(diff: str) -> tuple[int, int]:
    added = removed = 0
    for line in str(diff or "").splitlines():
        if line.startswith("+++") or line.startswith("---"):
            continue
        if line.startswith("+"):
            added += 1
        elif line.startswith("-"):
            removed += 1
    return added, removed


_KIND_STATUS = {"add": "A", "added": "A", "create": "A", "delete": "D", "deleted": "D", "remove": "D"}


class Normalizer:
    """Per-thread notification folding with duplicate suppression.

    Agent text: deltas are emitted as they stream; the ``item/completed``
    text is emitted only when NO delta was seen for that item. File diffs
    are emitted once per completed ``fileChange`` item (the aggregate
    ``turn/diff/updated`` is kept as state, never counted again).
    """

    def __init__(self) -> None:
        self._delta_items: set = set()
        self._reasoning: Dict[str, List[str]] = {}
        self._items: Dict[str, Dict[str, Any]] = {}
        self._diffed_items: set = set()
        self.turn_diff: str = ""
        self.plan: List[Dict[str, str]] = []

    def normalize(self, msg: Dict[str, Any]) -> List[Dict[str, Any]]:
        method = str(msg.get("method") or "")
        p = msg.get("params") if isinstance(msg.get("params"), dict) else {}
        ids = {"thread_id": p.get("threadId"), "turn_id": p.get("turnId")}
        out: List[Dict[str, Any]] = []
        if method == "turn/started":
            turn = p.get("turn") or {}
            out.append(lifecycle("run.started", turn_id=turn.get("id"), thread_id=p.get("threadId")))
        elif method == "turn/completed":
            turn = p.get("turn") or {}
            status = str(turn.get("status") or "")
            err = turn.get("error") or {}
            if status == "completed":
                out.append(lifecycle("run.completed", turn_id=turn.get("id"), thread_id=p.get("threadId")))
            else:
                out.append(lifecycle(
                    "run.failed",
                    turn_id=turn.get("id"), thread_id=p.get("threadId"),
                    error=str(err.get("message") or ("turn interrupted" if status == "interrupted" else f"turn {status or 'failed'}")),
                    cancelled=status == "interrupted",
                    codex_error_info=err.get("codexErrorInfo"),
                ))
        elif method == "item/agentMessage/delta":
            item_id = str(p.get("itemId") or "")
            delta = str(p.get("delta") or "")
            if delta:
                self._delta_items.add(item_id)
                out.append(_envelope("content", delta=delta, done=False, codex_item_id=item_id, **ids))
        elif method == "item/reasoning/summaryTextDelta":
            self._reasoning.setdefault(str(p.get("itemId") or ""), []).append(str(p.get("delta") or ""))
        elif method == "item/commandExecution/outputDelta":
            item = self._items.setdefault(str(p.get("itemId") or ""), {})
            item["_output"] = (item.get("_output") or "") + str(p.get("delta") or "")
        elif method == "item/started":
            out.extend(self._item_started(p.get("item") or {}, ids))
        elif method == "item/completed":
            out.extend(self._item_completed(p.get("item") or {}, ids))
        elif method == "turn/diff/updated":
            self.turn_diff = str(p.get("diff") or "")
        elif method == "turn/plan/updated":
            plan = [
                {"content": str(s.get("step") or ""), "status": str(s.get("status") or "")}
                for s in (p.get("plan") or []) if isinstance(s, dict)
            ]
            self.plan = plan
            out.append(_envelope("tool_use", id=f"plan-{p.get('turnId') or ''}", tool=TOOL_PLAN, title="plan",
                                 plan_items=plan, updated=True, **ids))
        elif method == "thread/status/changed":
            st = p.get("status") or {}
            out.append(lifecycle("thread.status", status=st.get("type"), flags=list(st.get("activeFlags") or []),
                                 thread_id=p.get("threadId")))
        elif method == "error":
            err = p.get("error") or {}
            out.append(lifecycle("error", error=str(err.get("message") or "codex error"), **ids))
        elif method in ("model/rerouted",):
            out.append(lifecycle("model.rerouted", from_model=p.get("fromModel"), to_model=p.get("toModel"),
                                 reason=p.get("reason"), **ids))
        return out

    def _item_started(self, item: Dict[str, Any], ids: Dict[str, Any]) -> List[Dict[str, Any]]:
        kind = str(item.get("type") or "")
        item_id = str(item.get("id") or "")
        self._items.setdefault(item_id, {})["type"] = kind
        if kind == "commandExecution":
            cmd = item.get("command")
            cmd_text = " ".join(cmd) if isinstance(cmd, list) else str(cmd or "")
            return [_envelope("tool_use", id=item_id, tool=TOOL_SHELL, command=cmd_text, title=cmd_text[:200],
                              cwd=item.get("cwd"), codex_item_id=item_id, **ids)]
        if kind == "fileChange":
            paths = [str(c.get("path") or "") for c in (item.get("changes") or []) if isinstance(c, dict)]
            return [_envelope("tool_use", id=item_id, tool=TOOL_FILE_EDIT, title=", ".join(paths)[:200] or "file change",
                              paths=paths, codex_item_id=item_id, **ids)]
        if kind == "mcpToolCall":
            return [_envelope("tool_use", id=item_id, tool=TOOL_MCP, title=f"{item.get('server')}/{item.get('tool')}",
                              arguments=item.get("arguments"), codex_item_id=item_id, **ids)]
        if kind == "webSearch":
            return [_envelope("tool_use", id=item_id, tool=TOOL_SEARCH, title=str(item.get("query") or "web search"),
                              codex_item_id=item_id, **ids)]
        return []

    def _item_completed(self, item: Dict[str, Any], ids: Dict[str, Any]) -> List[Dict[str, Any]]:
        kind = str(item.get("type") or "")
        item_id = str(item.get("id") or "")
        state = self._items.pop(item_id, {})
        out: List[Dict[str, Any]] = []
        if kind == "agentMessage":
            if item_id not in self._delta_items and str(item.get("text") or ""):
                out.append(_envelope("content", delta=str(item.get("text")), done=True, codex_item_id=item_id, **ids))
            else:
                self._delta_items.discard(item_id)
                out.append(_envelope("content", delta="", done=True, codex_item_id=item_id, phase=item.get("phase"), **ids))
        elif kind == "reasoning":
            parts = self._reasoning.pop(item_id, [])
            text = "".join(parts) or "\n".join(str(s) for s in (item.get("summary") or []) if s)
            if text:
                out.append(lifecycle("reasoning", text=_clip(text, MAX_OUTPUT_CHARS), codex_item_id=item_id, **ids))
        elif kind == "commandExecution":
            status = str(item.get("status") or "")
            output = str(item.get("aggregatedOutput") or state.get("_output") or "")
            cmd = item.get("command")
            cmd_text = " ".join(cmd) if isinstance(cmd, list) else str(cmd or "")
            out.append(_envelope(
                "tool_result", id=item_id, tool=TOOL_SHELL, command=cmd_text,
                status=STATUS_DONE if status == "completed" else STATUS_ERROR,
                native_status=status, exit_code=item.get("exitCode"), duration_ms=item.get("durationMs"),
                output=_clip(output, MAX_OUTPUT_CHARS), codex_item_id=item_id, **ids,
            ))
        elif kind == "fileChange":
            status = str(item.get("status") or "")
            out.append(_envelope("tool_result", id=item_id, tool=TOOL_FILE_EDIT,
                                 status=STATUS_DONE if status == "completed" else STATUS_ERROR,
                                 native_status=status, codex_item_id=item_id, **ids))
            if status == "completed" and item_id not in self._diffed_items:
                self._diffed_items.add(item_id)
                for change in item.get("changes") or []:
                    if not isinstance(change, dict):
                        continue
                    diff = str(change.get("diff") or "")
                    added, removed = _diff_counts(diff)
                    out.append(_envelope(
                        "file_diff", path=str(change.get("path") or ""), before="", after="",
                        diff=_clip(diff, MAX_DIFF_CHARS), added=added, removed=removed,
                        status=_KIND_STATUS.get(str(change.get("kind") or "").lower(), "M"),
                        codex_item_id=item_id, **ids,
                    ))
        elif kind == "mcpToolCall":
            status = str(item.get("status") or "")
            out.append(_envelope("tool_result", id=item_id, tool=TOOL_MCP,
                                 status=STATUS_DONE if status == "completed" else STATUS_ERROR,
                                 native_status=status, output=_clip(json.dumps(item.get("result"), default=str) if item.get("result") is not None else str(item.get("error") or ""), MAX_OUTPUT_CHARS),
                                 codex_item_id=item_id, **ids))
        elif kind == "webSearch":
            out.append(_envelope("tool_result", id=item_id, tool=TOOL_SEARCH, status=STATUS_DONE,
                                 codex_item_id=item_id, **ids))
        return out


def decline_result(method: str) -> Dict[str, Any]:
    """The response that declines a server-initiated approval/input request."""
    if method in APPROVAL_METHODS:
        if method == "item/permissions/requestApproval":
            return {"permissions": {}}
        return {"decision": "decline"}
    if method == "mcpServer/elicitation/request":
        return {"action": "decline", "content": None, "_meta": None}
    return {"answers": {}}
