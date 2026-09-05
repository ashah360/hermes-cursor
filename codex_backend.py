"""The ``codex_*`` tools: thin backend-bound entry points.

Same handle table, event log, subscriptions and completion rail as the
``cursor_*`` tools; execution is the independent Codex controller
(``codex_client`` / ``codex_controller``). A session created here is bound
to the Codex backend for life (``backend: "codex"`` on its handle entry).
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from typing import Any, Dict, List, Optional

from . import codex_client as _codex
from . import eventlog as _eventlog
from . import handles as _handles
from . import progress as _progress
from . import render as _render
from . import runner as _runner
from . import workers as _workers
from . import writer_guard as _guard

logger = logging.getLogger(__name__)

TOOLSET = "ghost_cursor"
CREATE = "codex_create_session"
SEND = "codex_send_message"
STATUS = "codex_status"
STOP = "codex_stop"
EVENTS = "codex_events"
LIST = "codex_list"
SUBSCRIBE = "codex_subscribe"

MAX_TITLE_CHARS = 80
# codex_stop waits this long for the controller to OBSERVE turn/completed.
STOP_WAIT_S = 15.0
_STOP_POLL_S = 0.25

_SESSION_DOC = "The session handle: the title given to codex_create_session."

CREATE_SCHEMA = {
    "name": CREATE,
    "description": (
        "Create a named Codex session (OpenAI Codex app-server) for coding work in a LOCAL worktree. "
        "The `title` IS the handle for every other codex_* tool. Lazy: nothing runs until the first "
        "codex_send_message. Requires an explicit model (no default substitution); the model and effort are "
        "verified against the installed Codex catalog on the first send. Only one writer (Cursor or Codex) may be "
        "active per worktree; sibling worktrees run in parallel."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "title": {"type": "string", "description": "Short plain-English phrase naming the task; becomes the handle. Max 80 chars."},
            "repo": {"type": "string", "description": "Absolute path of the local checkout/worktree to work in. No GitHub origin required."},
            "model": {"type": "string", "description": "Exact Codex model id (verified against model/list on first send). Required unless plugins.ghost_cursor.codex_model is set."},
            "effort": {"type": "string", "description": "Optional reasoning effort (must be one the model supports)."},
        },
        "required": ["title", "repo"],
    },
}

SEND_SCHEMA = {
    "name": SEND,
    "description": (
        "Send work to a Codex session. First message starts a turn on a new Codex thread; later messages "
        "continue the SAME thread. If a turn is still active, the message is appended to it natively "
        "(turn/steer) and the ack names the steered turn — it does not interrupt. Returns immediately; the final "
        "result is delivered automatically on every outcome. Track with codex_status, page with codex_events, "
        "stop with codex_stop."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "session": {"type": "string", "description": _SESSION_DOC},
            "message": {"type": "string", "description": "The task or follow-up instruction."},
            "update_interval_s": {"type": "number", "description": "Optional progress-digest cadence for this Hermes session (default 180; 0 disables)."},
        },
        "required": ["session", "message"],
    },
}

STATUS_SCHEMA = {
    "name": STATUS,
    "description": "Read-only snapshot of a Codex session: turn status (including pending approval/input flags), files changed, recent activity, event log location.",
    "parameters": {"type": "object", "properties": {"session": {"type": "string", "description": _SESSION_DOC}}, "required": ["session"]},
}

STOP_SCHEMA = {
    "name": STOP,
    "description": "Interrupt the active Codex turn (turn/interrupt). Acks 'cancelled' only after the controller OBSERVES the turn end; otherwise reports it is still running.",
    "parameters": {"type": "object", "properties": {"session": {"type": "string", "description": _SESSION_DOC}}, "required": ["session"]},
}

EVENTS_SCHEMA = {
    "name": EVENTS,
    "description": "Page a Codex session's persisted event history (same paging as cursor_events: offset<0 from the end, offset>=0 forward by seq, kind filter).",
    "parameters": {
        "type": "object",
        "properties": {
            "session": {"type": "string", "description": _SESSION_DOC},
            "offset": {"type": "integer"}, "limit": {"type": "integer"},
            "kind": {"type": "string", "description": "reasoning, file_diff, tool_result, tool_use, content, or lifecycle"},
        },
        "required": ["session"],
    },
}

LIST_SCHEMA = {
    "name": LIST,
    "description": "List Codex session handles as TSV (session, repo, status, elapsed, files, last_activity).",
    "parameters": {"type": "object", "properties": {"scope": {"type": "string", "enum": ["session", "all"]}}, "required": []},
}

SUBSCRIBE_SCHEMA = {
    "name": SUBSCRIBE,
    "description": "Subscribe/retune THIS Hermes session's progress digests for a Codex session (interval_s=0 unsubscribes this session only).",
    "parameters": {
        "type": "object",
        "properties": {"session": {"type": "string", "description": _SESSION_DOC}, "interval_s": {"type": "number"}},
        "required": ["session", "interval_s"],
    },
}


def _session_key() -> str:
    try:
        from tools.approval import get_current_session_key

        return get_current_session_key(default="") or ""
    except Exception:
        return ""


def _resolve(identifier: str) -> Optional[str]:
    name = _handles.resolve(str(identifier or "").strip())
    if name is None or _handles.backend_of(_handles.get(name)) != _codex.BACKEND:
        return None
    return name


def _rows(scope: str) -> List[Dict[str, str]]:
    rows = []
    for entry in _handles.entries(scope=scope, session_key=_session_key()):
        if _handles.backend_of(entry) != _codex.BACKEND:
            continue
        status = str(entry.get("status") or "created")
        note = str(entry.get("status_note") or "")
        dur = entry.get("duration_s")
        files = entry.get("files_changed_count")
        rows.append({
            "session": str(entry.get("session") or ""), "repo": str(entry.get("repo") or "—"), "runtime": "codex:local",
            "status": f"{status} ({note})" if note else status,
            "elapsed": _render.secs(dur) if dur is not None else "—",
            "files": str(files) if files is not None else "—", "last_activity": "—",
        })
    return rows


def _unknown(identifier: str) -> str:
    return _render.unknown_session(identifier, _rows("session"))


def codex_create_session(title: str, repo: str, model: Optional[str] = None, effort: Optional[str] = None, **_: Any) -> str:
    name = " ".join(str(title or "").split())
    if not name:
        return "title is required — a short phrase naming the task."
    if len(name) > MAX_TITLE_CHARS:
        return _render.title_too_long(name, MAX_TITLE_CHARS)
    target = str(repo or "").strip()
    if not target:
        return "repo is required — the absolute path of the local worktree."
    try:
        workdir = _runner.resolve_repo(target)
    except _runner.HarnessError as exc:
        return f"cannot create session: {exc}"
    canonical = _workers.canonical_repo_path(str(workdir))
    chosen_model = (str(model).strip() or None) if model else _codex.configured_model()
    if not chosen_model:
        return ("cannot create session: model is required for Codex sessions (no default substitution) — pass "
                "the exact Codex model id or set plugins.ghost_cursor.codex_model.")
    chosen_effort = (str(effort).strip() or None) if effort else _codex.configured_effort()
    if _handles.resolve(name) is not None:
        existing = _handles.get(name) or {}
        updated = existing.get("updated_at")
        return _render.title_taken(name, str(existing.get("status") or "unknown"),
                                   (time.time() - float(updated)) if isinstance(updated, (int, float)) else None)
    _handles.record(name, repo=canonical, status="created", model=chosen_model, effort=chosen_effort,
                    runtime="local", backend=_codex.BACKEND, session_key=_session_key())
    ack = _render.create_session_ack(name, canonical, chosen_model, "codex:local").replace("cursor_send_message", SEND)
    return f"{ack}\neffort: {chosen_effort or 'model default'} · model/effort are verified against the Codex catalog on the first send."


def codex_send_message(session: str, message: str, update_interval_s: Optional[float] = None, **_: Any) -> str:
    ident = str(session or "").strip()
    if not ident:
        return f"session is required — pass the name from {CREATE}."
    if not str(message or "").strip():
        return "message is required."
    name = _resolve(ident)
    if name is None:
        return _unknown(ident)
    entry = _handles.get(name) or {}
    repo = str(entry.get("repo") or "")
    if not repo or not os.path.isdir(repo):
        return f"the repo recorded for session '{name}' no longer exists: {repo}"
    try:
        interval, note = (_progress.validate_interval(update_interval_s, "update_interval_s")
                          if update_interval_s is not None else (None, None))
    except ValueError as exc:
        return str(exc)
    try:
        supervision = _codex.ensure_controller()
    except _codex.ControllerUnavailable as exc:
        return f"cannot dispatch to Codex: {exc}"
    caller = _session_key()
    _handles.set_subscriber(name, caller, _progress.resolve_interval(entry, interval, caller))
    _codex.follower.start()

    ref = _codex.session_ref(name)
    # Active turn -> native steer with the exact expected turn id.
    try:
        st = _codex.request("status", session=ref)
    except _codex.ControllerUnavailable as exc:
        return f"cannot dispatch to Codex: {exc}"
    state = st.get("state") or {}
    active = str(state.get("active_turn_id") or "") if st.get("ok") else ""
    if state.get("pending_intent") and not active:
        pend = state["pending_intent"]
        return (f"cannot dispatch: session '{name}' has an unresolved dispatch intent "
                f"({pend.get('intent_id')}: {pend.get('error') or 'outcome unknown'}). The provider may have "
                f"accepted that turn. Check {STATUS}; if no turn appears, {STOP} clears the intent, then send again.")
    if active:
        resp = _codex.request("steer", session=ref, text=str(message), expected_turn_id=active)
        if resp.get("ok"):
            _eventlog.append(name, {"source": "ghost", "kind": "lifecycle", "event": "steer.sent", "turn_id": active, "backend": "codex"})
            return (f"steered the ACTIVE turn {active} in {name} (turn/steer, expectedTurnId={active}) — no new turn "
                    "was started; the message was appended to the in-flight turn. result still auto-delivers.")
        if resp.get("error") == "stale_turn":
            return (f"the turn {active} in {name} just changed ({resp.get('turn_id') or 'no active turn'}); NOT "
                    "re-sending automatically — check codex_status and send again.")
        if resp.get("error") == "no_active_turn":
            pass  # the turn finished between status and steer: fall through to a new turn
        else:
            return f"steer failed: {resp.get('error')}"

    # Atomic worktree reservation (shared with Cursor local dispatch): the
    # check and the claim write are one flock'd section. The controller
    # re-asserts the same claim (same session ref) before turn/start and
    # holds it through an ambiguous outcome until the turn settles.
    claim, holder = _guard.reserve(repo, "codex", ref, _codex.claims_dir())
    if claim is None:
        return _render.repo_busy(_guard.describe(holder), repo).replace("two cursor runs", "two agents")
    prompt_seq = (_eventlog.stats(name) or {}).get("total_events", 0)
    intent_id = f"intent-{uuid.uuid4().hex[:12]}"
    _handles.record(name, pending_intent_id=intent_id, task=str(message)[:200])
    resp = _codex.request("start", session=ref, cwd=repo, model=str(entry.get("model") or ""),
                          effort=entry.get("effort"), prompt=str(message), intent_id=intent_id)
    if not resp.get("ok"):
        err = str(resp.get("error") or "unknown error")
        if err == "ambiguous_dispatch":
            # Intent and claim stay in place on the controller. Never re-send.
            _handles.record(name, status="running", codex_thread_id=resp.get("thread_id"))
            return (f"dispatch to {name} is AMBIGUOUS: the turn/start reply was lost ({resp.get('detail')}). "
                    "The provider may be running the turn; the worktree stays reserved and the intent "
                    f"{intent_id} stays recorded. If the turn starts, it is adopted and its result auto-delivers. "
                    f"Check {STATUS}; if nothing appears, {STOP} clears the intent.")
        _guard.release(repo, ref, _codex.claims_dir())
        _handles.record(name, pending_intent_id=None)
        if err == "worktree_busy":
            return _render.repo_busy(_guard.describe(resp.get("claim")), repo)
        if err == "turn_active":
            return f"a turn ({resp.get('turn_id')}) became active in {name} meanwhile — send again to steer it."
        if err in ("pending_intent", "duplicate_intent"):
            return (f"cannot dispatch: {resp.get('detail')}. pending intent: "
                    f"{(resp.get('pending_intent') or {}).get('intent_id')}")
        return f"codex dispatch failed for {name}: {err}"
    _handles.record(
        name, status="running", codex_thread_id=resp.get("thread_id"), codex_turn_id=resp.get("turn_id"),
        model=resp.get("model"), effort=resp.get("effort"), last_prompt_seq=prompt_seq, pending_intent_id=None,
        worker_supervision=supervision if supervision in ("systemd", "detached") else None,
    )
    lines = [
        f"sent to {name} · codex turn {resp.get('turn_id')} on thread {resp.get('thread_id')} "
        f"({'resumed thread' if resp.get('resumed') else 'new thread'})",
        f"model: {resp.get('model')} · effort: {resp.get('effort') or 'model default'} · verified against the Codex catalog",
        f"result auto-delivers; {STATUS} polls; another {SEND} while active steers this turn.",
    ]
    if supervision == "detached":
        lines.append("warning: the codex controller is running DETACHED (user systemd unavailable) — it survives "
                     "gateway restarts but nothing restarts it if it crashes.")
    if note:
        lines.append(f"note: {note}")
    return "\n".join(lines)


def codex_status(session: str, **_: Any) -> str:
    ident = str(session or "").strip()
    if not ident:
        return f"session is required — pass the name from {CREATE}."
    name = _resolve(ident)
    if name is None:
        return _unknown(ident)
    entry = _handles.get(name) or {}
    try:
        _codex.follower.ingest(name, entry)
        entry = _handles.get(name) or entry
    except Exception:
        pass
    proj = _codex.follower.turn_projection(name)
    live = proj.get("state")
    stats = _eventlog.stats(name)
    tail = _eventlog.read_events(name, offset=-1, limit=20)
    bullets = _render.recent_bullets((tail or {}).get("events") or [])
    status = str(entry.get("status") or "created")
    note_parts: List[str] = []
    files: List[Dict[str, Any]] = []
    pending: List[Dict[str, Any]] = []
    elapsed = entry.get("duration_s")
    last_activity = None
    if live is not None:
        active = live.get("active_turn_id")
        turn = proj.get("turn") or {}
        if active:
            status = "running"
            if turn:
                elapsed = time.time() - float(turn.get("started_at") or time.time())
        elif live.get("pending_intent"):
            status = "dispatch pending (outcome unknown)"
        elif live.get("status"):
            status = str(live.get("status"))
        files = proj["files"]
        pending = proj["pending_tools"]
        if live.get("last_activity_at"):
            last_activity = time.time() - float(live["last_activity_at"])
        ts = live.get("thread_status") or {}
        if ts.get("flags"):
            note_parts.append(f"codex thread flags: {', '.join(ts['flags'])}")
        if live.get("pending_intent"):
            pend = live["pending_intent"]
            note_parts.append(f"dispatch intent {pend.get('intent_id')} persisted with unknown turn/start outcome "
                              f"({pend.get('error') or 'no reply observed'}) — {STOP} clears it; do not re-send blindly")
        note_parts.append(f"codex thread {live.get('thread_id')} · turn {active or turn.get('turn_id') or '—'} · "
                          f"app-server {'alive' if proj.get('app_server_alive') else 'idle/stopped'}")
    else:
        note_parts.append("codex controller not reachable — showing the persisted record.")
    if str(entry.get("worker_supervision") or "") == "detached":
        note_parts.append("warning: codex controller runs DETACHED (user systemd unavailable — degraded supervision).")
    return _render.status_text(
        name=name, status=status, elapsed_s=elapsed, last_activity_s=last_activity,
        total_events=(stats or {}).get("total_events", 0), log_path=(stats or {}).get("path"),
        task=str(entry.get("task") or ""), files=files, bullets=bullets, error=str(entry.get("status_note") or ""),
        pending_tools=pending, plan=proj.get("plan") or [], last_prompt_seq=_handles.last_prompt_seq(entry),
        runtime="codex:local", note="\n".join(note_parts),
    )


def codex_stop(session: str, **_: Any) -> str:
    ident = str(session or "").strip()
    if not ident:
        return f"session is required — pass the name from {CREATE}."
    name = _resolve(ident)
    if name is None:
        return _unknown(ident)
    ref = _codex.session_ref(name)
    try:
        resp = _codex.request("interrupt", session=ref)
    except _codex.ControllerUnavailable as exc:
        return f"cannot stop {name}: {exc}"
    if not resp.get("ok"):
        return f"codex interrupt failed for {name}: {resp.get('error')}"
    turn_id = str(resp.get("turn_id") or "")
    if resp.get("intent_cleared"):
        _handles.record(name, status="failed", pending_intent_id=None,
                        status_note="unresolved dispatch intent cleared by codex_stop; no turn was observed")
        return (f"no active turn in '{name}'; cleared the unresolved dispatch intent {resp['intent_cleared']} and "
                "released the worktree. Inspect the worktree before sending again — the provider never reported a turn.")
    if not turn_id:
        entry = _handles.get(name) or {}
        return _render.stop_text(name=name, status=str(entry.get("status") or "idle"), elapsed_s=entry.get("duration_s"),
                                 files=[], already_finished=True)
    deadline = time.monotonic() + STOP_WAIT_S
    while time.monotonic() < deadline:
        st = _codex.request("status", session=ref, timeout=10.0)
        state = st.get("state") or {}
        if st.get("ok") and not state.get("active_turn_id"):
            _codex.follower.sync_once()
            comp = next((c for c in reversed(state.get("completions") or []) if c.get("turn_id") == turn_id), {})
            return _render.stop_text(name=name, status=str(comp.get("status") or state.get("status") or "cancelled"),
                                     elapsed_s=None, files=list(comp.get("files") or []), already_finished=False)
        time.sleep(_STOP_POLL_S)
    return (f"interrupt sent for turn {turn_id} in '{name}', but the controller has not observed turn/completed within "
            f"{int(STOP_WAIT_S)}s — it is still executing. status stays running; retry {STOP} or watch {STATUS}.")


def codex_events(session: str, offset: Optional[int] = None, limit: Optional[int] = None, kind: Optional[str] = None, **_: Any) -> str:
    ident = str(session or "").strip()
    if not ident:
        return f"session is required — pass the name from {CREATE}."
    name = _resolve(ident)
    if name is None:
        return _unknown(ident)
    entry = _handles.get(name) or {}
    try:
        _codex.follower.ingest(name, entry)
    except Exception:
        pass
    page = _eventlog.read_events(name, offset=offset if offset is not None else -1,
                                 limit=limit if limit is not None else _eventlog.DEFAULT_EVENTS_LIMIT, kind=kind)
    if page is None:
        return _render.no_event_log(name)
    return _render.events_text(name, page, _handles.last_prompt_seq(_handles.get(name)))


def codex_list(scope: str = "session", **_: Any) -> str:
    scope = scope if scope in _handles.VALID_SCOPES else "session"
    rows = _rows(scope)
    return _render.list_text(rows) if rows else _render.empty_list(scope)


def codex_subscribe(session: str, interval_s: Any = None, **_: Any) -> str:
    ident = str(session or "").strip()
    if not ident:
        return f"session is required — pass the name from {CREATE}."
    name = _resolve(ident)
    if name is None:
        return _unknown(ident)
    if interval_s is None:
        return "interval_s is required — seconds between digests (0 unsubscribes)."
    try:
        interval, note = _progress.validate_interval(interval_s, "interval_s")
    except ValueError as exc:
        return str(exc)
    _handles.set_subscriber(name, _session_key(), interval)
    return _render.subscribe_ack(name, interval, note)


TOOLS = (
    (CREATE, CREATE_SCHEMA, lambda a, **k: codex_create_session(title=a.get("title", ""), repo=a.get("repo", ""), model=a.get("model"), effort=a.get("effort")), "🧭"),
    (SEND, SEND_SCHEMA, lambda a, **k: codex_send_message(session=a.get("session", ""), message=a.get("message", ""), update_interval_s=a.get("update_interval_s")), "📨"),
    (STATUS, STATUS_SCHEMA, lambda a, **k: codex_status(session=a.get("session", "")), "🛰️"),
    (STOP, STOP_SCHEMA, lambda a, **k: codex_stop(session=a.get("session", "")), "🛑"),
    (EVENTS, EVENTS_SCHEMA, lambda a, **k: codex_events(session=a.get("session", ""), offset=a.get("offset"), limit=a.get("limit"), kind=a.get("kind")), "📜"),
    (LIST, LIST_SCHEMA, lambda a, **k: codex_list(scope=a.get("scope", "session")), "📋"),
    (SUBSCRIBE, SUBSCRIBE_SCHEMA, lambda a, **k: codex_subscribe(session=a.get("session", ""), interval_s=a.get("interval_s")), "🔔"),
)


def register(ctx) -> None:
    """Register the codex_* tools. Starts the follower only when a controller
    socket already exists (a previous gateway left Codex work running)."""
    try:
        if (_codex.state_dir() / "control.sock").exists():
            _codex.follower.start()
    except Exception:
        logger.debug("codex follower start at register failed", exc_info=True)
    for name, schema, handler, emoji in TOOLS:
        ctx.register_tool(name=name, toolset=TOOLSET, schema=schema, handler=handler,
                          check_fn=_codex.available, emoji=emoji)
