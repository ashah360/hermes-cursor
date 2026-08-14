"""Normalization of cursor events into canonical envelopes.

Two normalizers share the same envelope builders:

* :func:`normalize_harness` — the legacy ``--print --output-format
  stream-json`` stdout events (kept for the fallback/reference runner).
* :class:`SdkNormalizer` — SDKMessage-shaped dicts from the cloud runner
  (``cloud_runner.py`` converts the REST SSE events into these; the
  shapes originate with the cursor-sdk stream, hence the name). Stateful,
  because ``tool_call`` completion messages inherit the kind/title
  resolved when the call started, and because durations are measured
  client-side.

Adapted from the Threshold bridge (``app/bridge/events.py`` — the
``normalize_harness`` block), so the envelopes ``cursor_edit`` emits as tool
progress are byte-compatible with what the Threshold frontend already folds
into its transcript: ``content`` / ``tool_use`` / ``tool_result`` /
``lifecycle`` plus the ``file_diff`` kind carrying full before/after content
per completed edit.

Canonical envelope::

    {"source": "ghost", "kind": <"content"|"tool_use"|"tool_result"
                                 |"lifecycle"|"file_diff">, ...}
"""

from __future__ import annotations

import difflib
import re
import time
from typing import Any, Dict, List, Tuple

SOURCE = "ghost"

TOOL_SHELL = "shell"
TOOL_FILE_EDIT = "file-edit"
# Capture-derived kinds (the REST tool vocabulary is UNDOCUMENTED upstream:
# RunStreamToolCallData.name is a free-form string — "such as `read_file`,
# `run_terminal_cmd`, or `mcp`" is the spec's entire guidance). Names are
# matched by hint, never enum, and anything unrecognized falls back to
# TOOL_SHELL with its real name + salvaged description as the title, so a
# brand-new cursor tool degrades to something informative.
TOOL_SUBAGENT = "subagent"   # `task` — spawns a sub-agent (captured live 2026-07-09)
TOOL_AWAIT = "await"         # `await` — blocks on a background task/sub-agent
TOOL_PLAN = "plan"           # `todo_write` — cursor's own plan/progress list
TOOL_SEARCH = "search"       # `grep_search` / `codebase_search` / glob
TOOL_MCP = "mcp"             # documented name for MCP tool calls

STATUS_RUNNING = "running"
STATUS_DONE = "done"
STATUS_ERROR = "error"

# Full-file before/after payloads ride inside tool-progress events; cap each
# field so a single giant file can't balloon one SSE frame past what the
# gateway (and the persisted session row for the final result) can absorb.
MAX_CONTENT_CHARS = 200_000
MAX_DIFF_CHARS = 100_000
# Shell/tool output cap for the canonical envelope. Deliberately generous:
# the envelope is the full-fidelity record that lands in the per-session
# JSONL spill log (eventlog.py); the compact views (rolling buffer, status
# tails, paged events) apply their own much smaller inline clips.
MAX_OUTPUT_CHARS = 200_000


def _clip(text: Any, limit: int) -> str:
    s = str(text or "")
    if len(s) <= limit:
        return s
    return s[:limit] + f"\n… [truncated {len(s) - limit} chars]"


def _envelope(kind: str, **payload: Any) -> Dict[str, Any]:
    return {"source": SOURCE, "kind": kind, **payload}


def content_delta(delta: str, done: bool = False) -> Dict[str, Any]:
    """A streamed text fragment destined for a TextBlock."""
    return _envelope("content", delta=delta, done=done)


def lifecycle(event: str, **payload: Any) -> Dict[str, Any]:
    """A run-level signal (start/complete/fail/reasoning)."""
    return _envelope("lifecycle", event=event, **payload)


def file_diff(
    path: str,
    before: str,
    after: str,
    diff: str,
    added: int = 0,
    removed: int = 0,
    status: str | None = None,
) -> Dict[str, Any]:
    """A completed file edit with full before/after content.

    ``status`` follows git porcelain: "A" (added — no prior content),
    "M" (modified), "D" (deleted — no content after). Inferred when omitted.
    """
    if status is None:
        if not before:
            status = "A"
        elif not after:
            status = "D"
        else:
            status = "M"
    return _envelope(
        "file_diff",
        path=path,
        before=_clip(before, MAX_CONTENT_CHARS),
        after=_clip(after, MAX_CONTENT_CHARS),
        diff=_clip(diff, MAX_DIFF_CHARS),
        added=int(added or 0),
        removed=int(removed or 0),
        status=status,
    )


# cursor-agent tool_call payloads key the call by its kind: exactly one of
# these (or an unknown *ToolCall) is present under obj["tool_call"].
_EDIT_TOOLS = {"editToolCall", "writeToolCall", "deleteToolCall"}


def _tool_call_payload(data: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
    """Extract (raw_tool_key, payload) from a tool_call event, e.g.
    ("editToolCall", {"args": ..., "result": ...})."""
    tc = data.get("tool_call")
    if isinstance(tc, dict):
        for key, val in tc.items():
            if key.endswith("ToolCall") and isinstance(val, dict):
                return key, val
    return "", {}


def _tool_kind(raw_key: str) -> str:
    return TOOL_FILE_EDIT if raw_key in _EDIT_TOOLS else TOOL_SHELL


def _tool_title(raw_key: str, kind: str, args: Dict[str, Any]) -> str:
    path = str(args.get("path") or "")
    if kind == TOOL_FILE_EDIT:
        return f"Editing {path}" if path else "File edit"
    if raw_key == "readToolCall":
        return f"Reading {path}" if path else "Read file"
    cmd = str(args.get("command") or args.get("cmd") or "")
    if cmd:
        return cmd.strip().splitlines()[0][:120]
    name = raw_key[: -len("ToolCall")] if raw_key.endswith("ToolCall") else raw_key
    return name or "Tool"


def _duration_ms(tc_payload: Dict[str, Any], data: Dict[str, Any]) -> int | None:
    """durationMs = completedAtMs - startedAtMs when both are present.

    cursor-agent puts these on the tool_call wrapper (as STRINGS), sibling to
    the *ToolCall payload.
    """
    wrapper = data.get("tool_call") if isinstance(data.get("tool_call"), dict) else {}
    started = wrapper.get("startedAtMs") or tc_payload.get("startedAtMs")
    completed = wrapper.get("completedAtMs") or tc_payload.get("completedAtMs")
    try:
        return int(completed) - int(started)
    except (TypeError, ValueError):
        return None


def _assistant_text(data: Dict[str, Any]) -> str:
    """Concatenate text parts from a user/assistant message event."""
    message = data.get("message")
    if not isinstance(message, dict):
        return ""
    parts: List[str] = []
    for part in message.get("content") or []:
        if isinstance(part, dict) and isinstance(part.get("text"), str):
            parts.append(part["text"])
        elif isinstance(part, str):
            parts.append(part)
    return "".join(parts)


def _tool_started(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw_key, payload = _tool_call_payload(data)
    kind = _tool_kind(raw_key)
    args = payload.get("args") if isinstance(payload.get("args"), dict) else {}
    call_id = str(data.get("call_id") or "tool")
    env = _envelope(
        "tool_use",
        id=call_id,
        tool=kind,
        status=STATUS_RUNNING,
        title=_tool_title(raw_key, kind, args),
    )
    if kind == TOOL_FILE_EDIT:
        env["path"] = str(args.get("path") or "")
        env["additions"] = 0
        env["deletions"] = 0
        stream = args.get("streamContent")
        if isinstance(stream, str) and stream:
            env["preview"] = stream[:4000]
    else:
        env["command"] = str(
            args.get("command") or args.get("cmd") or args.get("path") or ""
        )
    return [env]


def _tool_completed(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw_key, payload = _tool_call_payload(data)
    kind = _tool_kind(raw_key)
    call_id = str(data.get("call_id") or "tool")
    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    success = result.get("success") if isinstance(result.get("success"), dict) else None
    is_error = success is None and ("error" in result or "failure" in result)

    res_env: Dict[str, Any] = _envelope(
        "tool_result",
        id=call_id,
        status=STATUS_ERROR if is_error else STATUS_DONE,
    )
    dur = _duration_ms(payload, data)
    if dur is not None:
        res_env["durationMs"] = dur

    envelopes = [res_env]

    if raw_key == "editToolCall" and success is not None:
        added = int(success.get("linesAdded") or 0)
        removed = int(success.get("linesRemoved") or 0)
        res_env["additions"] = added
        res_env["deletions"] = removed
        envelopes.append(
            file_diff(
                path=str(success.get("path") or ""),
                before=str(success.get("beforeFullFileContent") or ""),
                after=str(success.get("afterFullFileContent") or ""),
                diff=str(success.get("diffString") or ""),
                added=added,
                removed=removed,
            )
        )
    elif kind == TOOL_SHELL and success is not None:
        out = success.get("content") or success.get("output") or success.get("stdout")
        if isinstance(out, str) and out:
            res_env["output"] = _clip(out, MAX_OUTPUT_CHARS)
    elif is_error:
        err = result.get("error") or result.get("failure")
        if err:
            res_env["output"] = _clip(str(err), MAX_OUTPUT_CHARS)

    return envelopes


def normalize_harness(event_key: str, data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Translate one cursor-agent stream-json event into canonical envelopes.

    Args:
        event_key: ``"{type}.{subtype}"`` or bare ``"{type}"`` as produced by
            :func:`runner.event_key` (e.g. ``"tool_call.completed"``).
        data: The parsed JSON object for that stream line.

    Returns:
        Zero or more canonical envelopes. A completed edit yields TWO: the
        ``tool_result`` card plus a ``file_diff`` for the code pane.
    """
    key = (event_key or "").strip()
    data = data if isinstance(data, dict) else {}

    if key == "system.init":
        return [
            lifecycle(
                "run.started",
                model=data.get("model"),
                cwd=data.get("cwd"),
                harness_session_id=data.get("session_id"),
            )
        ]

    if key == "user" or key.startswith("user."):
        return []  # task echo — the caller already knows its own input

    if key.startswith("thinking"):
        text = data.get("text")
        if isinstance(text, str) and text:
            return [lifecycle("reasoning", text=text)]
        return []

    if key == "tool_call.started":
        return _tool_started(data)

    if key == "tool_call.completed":
        return _tool_completed(data)

    if key == "assistant" or key.startswith("assistant."):
        text = _assistant_text(data)
        return [content_delta(text)] if text else []

    if key.startswith("result"):
        if data.get("is_error"):
            return [
                lifecycle(
                    "run.failed",
                    status="failed",
                    error=data.get("result"),
                    duration_ms=data.get("duration_ms"),
                    usage=data.get("usage"),
                )
            ]
        return [
            lifecycle(
                "run.completed",
                status="completed",
                duration_ms=data.get("duration_ms"),
                usage=data.get("usage"),
            )
        ]

    if key == "harness.error":
        return [
            lifecycle(
                "run.failed",
                status="failed",
                error=data.get("error") or "harness error",
                timeout=bool(data.get("timeout")),
            )
        ]

    # Unknown event: opaque passthrough so nothing is silently dropped.
    return [lifecycle("passthrough", name=key, data=data)]


# ---------------------------------------------------------------------------
# cloud normalization — cloud_runner.run_cloud event tuples
# ---------------------------------------------------------------------------
# Message shapes follow the SDKMessage convention (type discriminator:
# system / user / assistant / thinking / tool_call / status / task / request
# / usage) — cloud_runner._message_from_sse converts the REST SSE events
# into the same shapes. The envelope fields (type, call_id, name, status)
# are stable; tool_call ``args`` and ``result`` payloads are EXPLICITLY
# UNSTABLE upstream — everything below parses them defensively and never
# raises on an unexpected shape.

# Message types that never produce envelopes: the task echo, handshake
# metadata, and transient status pings.
_SDK_NOISE_TYPES = {"system", "user", "request", "status"}

# tool_call names that mean a file edit (vs shell/read). Names are unstable
# upstream, so this is a substring match, not an enum.
_SDK_EDIT_NAME_HINTS = ("edit", "write", "delete", "apply_patch", "applypatch")
# Sub-agent spawn tools. Exact "task" observed live (2026-07-09,
# run-c99dd650: args {description, prompt, subagentType, model}); the
# other hints anticipate obvious renames.
_SDK_SUBAGENT_NAMES = ("task", "subagent", "spawn_agent", "agent_task")
# Search-shaped tools: grep_search / codebase_search / glob_file_search /
# web_search (args carry pattern/query + path).
_SDK_SEARCH_HINTS = ("grep", "search", "glob")

_SDK_TERMINAL_TOOL_STATUSES = {"completed", "error", "failed", "cancelled"}

# Cap for the sub-agent prompt snippet kept on the envelope. Full prompts
# are multi-KB and already live in cursor's own transcript; the snippet is
# digest/status context only.
_SUBAGENT_PROMPT_SNIPPET_CHARS = 280
# Cap for plan items retained per todo_write envelope (defensive: the list
# is model-authored and unbounded upstream).
_PLAN_MAX_ITEMS = 30


def _first_str(data: Dict[str, Any], *keys: str) -> str:
    """The first non-empty string value among ``keys``, else ""."""
    for key in keys:
        val = data.get(key)
        if isinstance(val, str) and val:
            return val
    return ""


def _sdk_tool_kind(name: str) -> str:
    lowered = str(name or "").lower()
    # Order matters: specific vocabulary before the broad edit hints
    # ("todo_write" would otherwise match the "write" edit hint).
    if lowered in _SDK_SUBAGENT_NAMES:
        return TOOL_SUBAGENT
    if lowered == "await":
        return TOOL_AWAIT
    if lowered in ("todo_write", "todo_read", "update_plan"):
        return TOOL_PLAN
    if lowered == "mcp" or lowered.startswith("mcp_"):
        return TOOL_MCP
    if any(hint in lowered for hint in _SDK_SEARCH_HINTS):
        return TOOL_SEARCH
    if any(hint in lowered for hint in _SDK_EDIT_NAME_HINTS):
        return TOOL_FILE_EDIT
    return TOOL_SHELL


def _plan_items(args: Dict[str, Any]) -> List[Dict[str, str]]:
    """Parsed todo items from a todo_write call, shape-tolerant.

    Observed live (2026-07-09): ``args.todos`` is a list of
    ``{id, content, status}`` with statuses like TODO_STATUS_IN_PROGRESS /
    TODO_STATUS_COMPLETED. Statuses are normalized to lowercase with the
    TODO_STATUS_ prefix stripped; unknown shapes yield [].
    """
    todos = args.get("todos")
    if not isinstance(todos, list):
        return []
    items: List[Dict[str, str]] = []
    for raw in todos[:_PLAN_MAX_ITEMS]:
        if not isinstance(raw, dict):
            continue
        content = str(raw.get("content") or raw.get("title") or "").strip()
        if not content:
            continue
        status = str(raw.get("status") or "").strip()
        if status.upper().startswith("TODO_STATUS_"):
            status = status[len("TODO_STATUS_"):]
        items.append({"content": content[:200], "status": status.lower()})
    return items


def _sdk_tool_title(name: str, kind: str, args: Dict[str, Any]) -> str:
    # The model-written description is the most human string on the wire
    # ("Run focused tests in background") — prefer it for
    # every kind that carries one. args are unstable/optional upstream, so
    # every probe degrades gracefully.
    description = _first_str(args, "description").strip()
    path = _first_str(args, "path", "file_path", "filePath")
    if kind == TOOL_SUBAGENT:
        return description[:160] if description else "sub-agent"
    if kind == TOOL_AWAIT:
        task_id = _first_str(args, "taskId", "task_id")
        return f"awaiting `{task_id}`" if task_id else "awaiting background task"
    if kind == TOOL_PLAN:
        items = _plan_items(args)
        if items:
            done = sum(1 for i in items if "complet" in i["status"])
            current = next(
                (i["content"] for i in items if "progress" in i["status"]), ""
            )
            head = f"plan updated ({done}/{len(items)} done)"
            return f"{head} — current: {current}"[:160] if current else head
        return "plan updated"
    if kind == TOOL_SEARCH:
        pattern = _first_str(args, "pattern", "query", "glob_pattern")
        scope = _first_str(args, "path", "target_directories")
        if pattern:
            title = f"searching '{pattern}'" + (f" in {scope}" if scope else "")
            return title[:160]
        return description[:160] if description else "searching"
    if kind == TOOL_FILE_EDIT:
        return f"Editing {path}" if path else "File edit"
    if description:
        return description[:160]
    cmd = _first_str(args, "command", "cmd")
    if cmd:
        return cmd.strip().splitlines()[0][:120]
    if "read" in str(name or "").lower() and path:
        return f"Reading {path}"
    return str(name or "").strip() or "Tool"


def _sdk_assistant_text(msg: Dict[str, Any]) -> str:
    """Concatenated text blocks from an assistant message, shape-tolerant."""
    message = msg.get("message")
    if isinstance(message, str):
        return message
    if not isinstance(message, dict):
        return ""
    content = message.get("content")
    if isinstance(content, str):
        return content
    parts: List[str] = []
    for block in content or []:
        if isinstance(block, dict) and isinstance(block.get("text"), str):
            parts.append(block["text"])
        elif isinstance(block, str):
            parts.append(block)
    return "".join(parts)


# Key aliases probed on edit-tool payloads. The REAL shape (captured live
# 2026-07-03 from cursor-sdk, fixtures/sdk_edit_tool_call.jsonl) wraps the
# payload in a {"status": "success", "value": {...}} envelope, keeps
# linesAdded / linesRemoved / diffString inside "value", and carries the
# path ONLY in the call's args — never in the result. The schema is
# documented unstable, so the older direct shapes stay matched too.
_DIFF_PATH_KEYS = ("path", "file_path", "filePath")
_DIFF_BEFORE_KEYS = ("beforeFullFileContent", "oldText", "before")
_DIFF_AFTER_KEYS = ("afterFullFileContent", "newText", "after")
_DIFF_TEXT_KEYS = ("diffString", "diff", "unifiedDiff", "patch")
_DIFF_ADDED_KEYS = ("linesAdded", "added", "additions")
_DIFF_REMOVED_KEYS = ("linesRemoved", "removed", "deletions")


def _int_or_none(val: Any) -> int | None:
    if val is None or isinstance(val, bool):
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _first_int(data: Dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        val = _int_or_none(data.get(key))
        if val is not None:
            return val
    return None


def _diff_candidates(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Dict payloads that may describe file edits, envelope wrappers
    ({"status": ..., "value": {...}} and friends) unwrapped recursively."""
    out: List[Dict[str, Any]] = []

    def add(node: Any, depth: int = 0) -> None:
        if not isinstance(node, dict):
            return
        out.append(node)
        if depth >= 3:
            return
        for key in ("value", "success", "result", "edit", "data"):
            add(node.get(key), depth + 1)
        for key in ("content", "diffs", "edits", "files"):
            nested = node.get(key)
            if isinstance(nested, list):
                for item in nested:
                    add(item, depth + 1)

    add(result)
    return out


def _diff_status_from_text(diff_text: str) -> str:
    """Porcelain status inferred from unified-diff headers.

    Used for pre-rendered diffString payloads (no before/after content to
    infer from): "--- /dev/null" means the file was added, "+++ /dev/null"
    deleted, anything else a modification.
    """
    for line in diff_text.splitlines()[:6]:
        if line.startswith("--- ") and "/dev/null" in line:
            return "A"
        if line.startswith("+++ ") and "/dev/null" in line:
            return "D"
    return "M"


def _sdk_diff_entries(
    result: Dict[str, Any], fallback_path: str = ""
) -> List[Dict[str, Any]]:
    """Best-effort file_diff envelopes mined from a tool_call result.

    The payload schema is unstable, so this probes every shape seen live
    (the {"status", "value"} envelope with diffString + line counts, full
    before/after content, oldText/newText blocks, pre-rendered diff
    strings) and returns [] rather than guessing. ``fallback_path`` is the
    path from the call's args / running message — the REAL edit tool's
    result carries no path at all. Runs whose edits slip through entirely
    still land in files_changed via the git fallback
    (cloud_runner.git_fallback_diffs).
    """
    entries: List[Dict[str, Any]] = []
    seen: set = set()
    for cand in _diff_candidates(result):
        path = _first_str(cand, *_DIFF_PATH_KEYS) or fallback_path
        if not path:
            continue
        before = _first_str(cand, *_DIFF_BEFORE_KEYS)
        after = _first_str(cand, *_DIFF_AFTER_KEYS)
        pre_rendered = _first_str(cand, *_DIFF_TEXT_KEYS)
        if not (before or after or pre_rendered):
            continue
        status: str | None = None
        if before or after:
            diff_text, added, removed = unified_diff_text(before, after, path)
        else:
            diff_text = pre_rendered
            added_hint = _first_int(cand, *_DIFF_ADDED_KEYS)
            removed_hint = _first_int(cand, *_DIFF_REMOVED_KEYS)
            added = added_hint if added_hint is not None else sum(
                1 for l in pre_rendered.splitlines()
                if l.startswith("+") and not l.startswith("+++")
            )
            removed = removed_hint if removed_hint is not None else sum(
                1 for l in pre_rendered.splitlines()
                if l.startswith("-") and not l.startswith("---")
            )
            status = _diff_status_from_text(pre_rendered)
        if not diff_text:
            continue
        dedupe_key = (path, diff_text[:500])
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        entries.append(
            file_diff(
                path=path,
                before=before,
                after=after,
                diff=diff_text,
                added=added,
                removed=removed,
                status=status,
            )
        )
    return entries


def _sdk_output_text(result: Any) -> str:
    """Shell/read output mined from a tool_call result, shape-tolerant.

    Real payloads (captured live 2026-07-03) nest the useful fields inside
    a {"status": "success", "value": {...}} envelope; older/other shapes
    carry them at the top level. Both are probed.
    """
    if isinstance(result, str):
        return result
    if not isinstance(result, dict):
        return "" if result is None else str(result)
    for wrapper_key in ("value", "success", "error", "failure"):
        nested = result.get(wrapper_key)
        if isinstance(nested, dict):
            inner = _sdk_output_text(nested)
            if inner:
                return inner
    parts = [
        str(result.get(k))
        for k in ("stdout", "stderr", "content", "output", "text", "error")
        if isinstance(result.get(k), str) and result.get(k)
    ]
    return "\n".join(parts)


def _sdk_exit_code(result: Any) -> int | None:
    """The command exit code from a tool_call result, envelope-tolerant."""
    if not isinstance(result, dict):
        return None
    code = _int_or_none(result.get("exitCode", result.get("exit_code")))
    if code is not None:
        return code
    nested = result.get("value")
    if isinstance(nested, dict):
        return _int_or_none(nested.get("exitCode", nested.get("exit_code")))
    return None


class SdkNormalizer:
    """Stateful cloud_runner event → canonical-envelope mapper (one per run).

    The name survives from the cursor-sdk era because the MESSAGE SHAPES
    it parses do: ``cloud_runner`` converts the REST SSE events into the
    same SDKMessage-style dicts (type discriminator, snake_case call_id)
    the sdk streamed, so the mapping — and the replay fixtures captured
    from both transports — stay valid.

    Stateful because tool_call completion messages must inherit the
    kind/title resolved when the call started, and because durations are
    measured client-side.
    """

    def __init__(self) -> None:
        # call_id → {kind, title, command, started (monotonic)}
        self._calls: Dict[str, Dict[str, Any]] = {}
        # call_ids whose "running" tool_use envelope was already emitted
        # (the SDK may re-emit running-status messages as args accumulate).
        self._started: set = set()
        # Tail of the FINAL contiguous content segment: assistant deltas
        # accumulate here and any tool/thinking activity resets it, so at a
        # "finished" result it holds only what the agent streamed last.
        # Scanned for transport-error signatures (see _TRANSPORT_ERROR_RE) —
        # observed live under ACP and kept under the SDK: a dying model
        # stream can be narrated as ordinary content before a clean finish.
        self._final_content_tail = ""

    # -- event entry point ---------------------------------------------------

    def normalize(self, event_key: str, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Translate one cloud_runner event into canonical envelopes.

        Args:
            event_key: ``"cloud.session" | "cloud.message" |
                "sse.reattached" | "cloud.model_warning" | "cloud.result" |
                "cloud.error"`` as yielded by :func:`cloud_runner.run_cloud`.
            data: For ``cloud.message``, the message as a plain dict.
        """
        data = data if isinstance(data, dict) else {}

        if event_key == "cloud.session":
            return [
                lifecycle(
                    "run.started",
                    model=data.get("model"),
                    cwd=data.get("cwd"),
                    harness_session_id=data.get("agentId"),
                    runtime=data.get("runtime"),
                    worker=data.get("worker"),
                    agents_ui_url=data.get("agents_ui_url"),
                    run_id=data.get("run_id"),
                )
            ]

        if event_key == "cloud.model_warning":
            # A legacy/unparseable model string was substituted (see
            # cloud_runner.translate_model). Log-signal lifecycle event:
            # the fold ignores unknown lifecycle events, so this lands in
            # the progress buffer and JSONL log without touching the run.
            return [
                lifecycle(
                    "model.warning",
                    warning=data.get("warning"),
                    requested=data.get("requested"),
                    using=data.get("using"),
                )
            ]

        if event_key == "sse.reattached":
            # Transparent stream recovery (Last-Event-ID reconnect):
            # JSONL-log signal only. The fold ignores unknown lifecycle
            # events, so nothing user-visible changes — no synthetic
            # messages, no re-prompt.
            return [
                lifecycle(
                    "sse.reattached",
                    last_event_id=data.get("last_event_id"),
                    attempt=data.get("attempt"),
                )
            ]

        if event_key == "cloud.result":
            status = str(data.get("status") or "")
            if status == "finished":
                transport_error = _transport_error_line(self._final_content_tail)
                if transport_error:
                    # The "clean" finish is a lie: the stream died and the
                    # error was narrated as the last message. Classify the
                    # run failed, error first-class.
                    return [
                        lifecycle(
                            "run.failed",
                            status="failed",
                            error=transport_error,
                            transport_error=True,
                            run_status=status,
                        )
                    ]
                return [lifecycle("run.completed", status="completed",
                                  run_status=status)]
            if status == "cancelled":
                return [
                    lifecycle(
                        "run.failed",
                        status="failed",
                        error="run cancelled (run.cancel)",
                        cancelled=True,
                    )
                ]
            if status == "expired":
                return [
                    lifecycle(
                        "run.failed",
                        status="failed",
                        error="cursor run expired",
                        timeout=True,
                        run_status=status,
                    )
                ]
            return [
                lifecycle(
                    "run.failed",
                    status="failed",
                    error=str(data.get("error") or "")
                    or f"cursor run ended with status: {status or 'unknown'}",
                    run_status=status,
                )
            ]

        if event_key == "cloud.error":
            # Terminal-error payloads carry typed detail (run_status /
            # unroutable_worker — see cloud_runner); mid-run failures
            # don't. Pass through whichever keys are present so the fold
            # and the completion summary can render them.
            extras = {
                key: data.get(key)
                for key in (
                    "retryable", "retry_after", "run_status", "unroutable_worker"
                )
                if key in data
            }
            return [
                lifecycle(
                    "run.failed",
                    status="failed",
                    error=data.get("error") or "cursor cloud error",
                    timeout=bool(data.get("timeout")),
                    **extras,
                )
            ]

        if event_key == "cloud.message":
            return self._message(data)

        # Unknown runner event: opaque passthrough so nothing is dropped.
        return [lifecycle("passthrough", name=event_key, data=data)]

    # -- SDKMessage types ------------------------------------------------------

    def _message(self, msg: Dict[str, Any]) -> List[Dict[str, Any]]:
        mtype = str(msg.get("type") or "")

        if mtype in _SDK_NOISE_TYPES:
            return []

        if mtype == "thinking":
            self._final_content_tail = ""
            text = msg.get("text")
            if isinstance(text, str) and text:
                return [lifecycle("reasoning", text=text)]
            return []

        if mtype == "assistant":
            text = _sdk_assistant_text(msg)
            if text:
                self._final_content_tail = (
                    self._final_content_tail + text
                )[-_TRANSPORT_SCAN_CHARS:]
            return [content_delta(text)] if text else []

        if mtype == "tool_call":
            self._final_content_tail = ""
            return self._tool_call(msg)

        if mtype == "usage":
            # Log-only: token accounting lands in the JSONL event log.
            return [lifecycle("usage", usage=msg.get("usage"))]

        if mtype == "task":
            return [lifecycle("task", status=msg.get("status"),
                              text=msg.get("text"))]

        # Unknown message type: opaque passthrough.
        return [lifecycle("passthrough", name=f"cloud.{mtype or 'message'}", data=msg)]

    def _tool_call(self, msg: Dict[str, Any]) -> List[Dict[str, Any]]:
        call_id = str(msg.get("call_id") or "tool")
        status = str(msg.get("status") or "")
        if status in _SDK_TERMINAL_TOOL_STATUSES:
            return self._tool_completed(call_id, status, msg)
        return self._tool_started(call_id, msg)

    def _tool_started(self, call_id: str, msg: Dict[str, Any]) -> List[Dict[str, Any]]:
        name = str(msg.get("name") or "")
        raw_args = msg.get("args")
        args: Dict[str, Any] = raw_args if isinstance(raw_args, dict) else {}

        if call_id in self._started:
            # Re-emitted running message. Args accumulate across these
            # (captured live 2026-07-09: the FIRST running message often
            # has no args at all; the description/pattern/todos arrive on
            # a re-emit). If the stored state was built argless and this
            # message finally carries args, upgrade in place and emit ONE
            # updated tool_use (same id — consumers keyed by call id treat
            # it as a refinement). Further re-emits stay dropped.
            state = self._calls.get(call_id)
            if state is not None and not state.get("had_args") and args:
                return self._tool_envelope(call_id, name, args, upgrade=True)
            return []
        self._started.add(call_id)
        return self._tool_envelope(call_id, name, args)

    def _tool_envelope(
        self, call_id: str, name: str, args: Dict[str, Any], upgrade: bool = False
    ) -> List[Dict[str, Any]]:
        prior = self._calls.get(call_id) or {}
        kind = _sdk_tool_kind(name)
        title = _sdk_tool_title(name, kind, args)
        state = {
            "kind": kind,
            "title": title,
            "command": _first_str(args, "command", "cmd"),
            "path": _first_str(args, *_DIFF_PATH_KEYS),
            "had_args": bool(args),
            # An upgrade must not reset the duration clock.
            "started": prior.get("started") or time.monotonic(),
        }
        self._calls[call_id] = state

        env = _envelope(
            "tool_use",
            id=call_id,
            tool=kind,
            status=STATUS_RUNNING,
            title=title,
        )
        if name:
            env["name"] = name
        if upgrade:
            env["updated"] = True
        if kind == TOOL_FILE_EDIT:
            env["path"] = _first_str(args, "path", "file_path", "filePath")
            env["additions"] = 0
            env["deletions"] = 0
            return [env]

        description = _first_str(args, "description").strip()
        if description:
            env["description"] = description[:300]
        if kind == TOOL_SUBAGENT:
            model = _first_str(args, "model")
            if model:
                env["subagent_model"] = model
            subagent_type = _first_str(args, "subagentType", "subagent_type")
            if subagent_type:
                env["subagent_type"] = subagent_type
            prompt = _first_str(args, "prompt")
            if prompt:
                env["prompt_snippet"] = _clip(
                    prompt, _SUBAGENT_PROMPT_SNIPPET_CHARS
                )
        elif kind == TOOL_PLAN:
            items = _plan_items(args)
            if items:
                env["plan_items"] = items
        elif kind == TOOL_AWAIT:
            task_id = _first_str(args, "taskId", "task_id")
            if task_id:
                env["await_task_id"] = task_id
        else:
            env["command"] = state["command"]
        return [env]

    def _tool_completed(
        self, call_id: str, status: str, msg: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        state = self._calls.get(call_id) or {}
        kind = state.get("kind") or _sdk_tool_kind(str(msg.get("name") or ""))

        envelopes: List[Dict[str, Any]] = []
        raw_args = msg.get("args")
        terminal_args: Dict[str, Any] = raw_args if isinstance(raw_args, dict) else {}
        if call_id not in self._started:
            # Terminal message with no prior running message (observed on
            # very fast calls): synthesize the tool_use so the pair renders.
            envelopes.extend(self._tool_started(call_id, msg))
            state = self._calls.get(call_id) or state
        elif not state.get("had_args") and terminal_args:
            # Ran argless the whole way and the args only arrived on the
            # terminal message (captured live 2026-07-09: todo_write's
            # `todos` list appears at completion). Salvage them into one
            # upgraded tool_use so the title/plan context isn't lost.
            envelopes.extend(
                self._tool_envelope(
                    call_id, str(msg.get("name") or ""), terminal_args,
                    upgrade=True,
                )
            )
            state = self._calls.get(call_id) or state

        is_error = status in ("error", "failed", "cancelled")
        res_env: Dict[str, Any] = _envelope(
            "tool_result",
            id=call_id,
            status=STATUS_ERROR if is_error else STATUS_DONE,
        )
        started = state.get("started")
        if isinstance(started, float):
            res_env["durationMs"] = int((time.monotonic() - started) * 1000)

        envelopes.append(res_env)

        result = msg.get("result")
        result_dict = result if isinstance(result, dict) else {}

        # The path from the call's args: the REAL edit tool's result carries
        # no path (captured live 2026-07-03), only the args do — probe the
        # terminal message's args first, then the remembered running state.
        args = msg.get("args") if isinstance(msg.get("args"), dict) else {}
        fallback_path = (
            _first_str(args, *_DIFF_PATH_KEYS) or str(state.get("path") or "")
        )

        diffs = (
            _sdk_diff_entries(result_dict, fallback_path=fallback_path)
            if kind == TOOL_FILE_EDIT
            else []
        )
        if diffs:
            res_env["additions"] = sum(d["added"] for d in diffs)
            res_env["deletions"] = sum(d["removed"] for d in diffs)
            envelopes.extend(diffs)
        else:
            out = _sdk_output_text(result)
            if out:
                res_env["output"] = _clip(out, MAX_OUTPUT_CHARS)
            exit_code = _sdk_exit_code(result)
            if exit_code not in (None, 0):
                res_env["status"] = STATUS_ERROR

        return envelopes


# ---------------------------------------------------------------------------
# Transport-error detection + diff utilities (shared)
# ---------------------------------------------------------------------------

# Transport-level failure signatures. Observed live (2026-07-03, under the
# old ACP transport): when the model stream behind cursor dies mid-run, the
# error text can be streamed as ordinary assistant content (e.g.
# "RetriableError: [canceled] http/2 stream closed with error code CANCEL
# (0x8)") followed by a CLEAN finish. Detect those signatures in the FINAL
# contiguous content segment (anything the agent streamed after its last
# tool/thinking activity) so the run is classified failed instead of
# completed. A signature that appears earlier and is followed by more
# activity means cursor recovered — that run stays a normal completion.
_TRANSPORT_ERROR_RE = re.compile(
    r"(?:\bRetriableError\b|\bConnectError\b|\bECONNRESET\b"
    r"|http/2 stream closed|stream closed with error code"
    r"|\bconnection (?:closed|reset|refused|lost)\b|socket hang up)",
    re.IGNORECASE,
)
# How much trailing content to keep for the transport-error scan; error
# chunks are short, this only needs to survive delta splitting.
_TRANSPORT_SCAN_CHARS = 4_000


def _transport_error_line(text: str) -> str:
    """The line of ``text`` carrying a transport-error signature, or ""."""
    match = _TRANSPORT_ERROR_RE.search(text)
    if not match:
        return ""
    start = text.rfind("\n", 0, match.start()) + 1
    end = text.find("\n", match.end())
    return text[start : end if end >= 0 else len(text)].strip()


def unified_diff_text(before: str, after: str, path: str) -> Tuple[str, int, int]:
    """Unified diff text plus (added, removed) line counts."""
    rel = str(path).lstrip("/")
    lines = list(
        difflib.unified_diff(
            before.splitlines(keepends=True),
            after.splitlines(keepends=True),
            fromfile=f"a/{rel}",
            tofile=f"b/{rel}",
        )
    )
    added = sum(1 for l in lines if l.startswith("+") and not l.startswith("+++"))
    removed = sum(1 for l in lines if l.startswith("-") and not l.startswith("---"))
    return "".join(lines), added, removed
