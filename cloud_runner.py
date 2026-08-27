"""Cloud runner — Cursor cloud agents over REST + SSE (no sdk, no bridge).

Replaces ``sdk_runner.py`` (the python ``cursor-sdk`` transport and its
``cursor-sdk-bridge`` sidecar). Every session is a Cursor CLOUD agent
created through the REST v1 API (``rest_client.py``); events stream over
the run's SSE endpoint. Two runtimes:

* ``runtime="local"`` (default) — the agent is routed to a plugin-managed
  "My Machines" worker on this box (``workers.py``): tool calls execute in
  the local checkout, and the conversation is visible at
  cursor.com/agents.
* ``runtime="cloud"`` — a cursor-hosted VM; the repo is cloned from GitHub
  server-side.

Yielded event tuples (consumed by ``events.SdkNormalizer``):

* ``("cloud.session", {...})`` — agent established (agentId ``bc-...``,
  run_id, cwd, model, resumed, runtime, worker, agents_ui_url, repo_url,
  starting_ref). ``resumed`` is True for a follow-up run on an existing
  agent, False for a fresh create.
* ``("cloud.message", <dict>)`` — one message dict converted from the
  simplified SSE events (``assistant`` / ``thinking`` / ``tool_call``;
  the parallel ``interaction_update`` duplicates are deliberately skipped,
  per the OpenAPI guidance — consuming both double-counts every delta).
  Unknown SSE event types flow through as ``{"type": "sse.<event>", ...}``
  so nothing is silently dropped.
* ``("sse.reattached", {"last_event_id": ..., "attempt": n})`` — the SSE
  stream dropped while the run stayed alive and was reconnected with the
  ``Last-Event-ID`` header. Lifecycle/log signal only.
* ``("cloud.model_warning", {...})`` — a legacy/unparseable model string
  was substituted (see :func:`translate_model`).
* ``("cloud.result", {"status": ...})`` — terminal status: finished |
  cancelled | expired (lowercased from the REST vocabulary).
* ``("cloud.error", {...})`` — hard failure (watchdog abort, terminal
  ERROR, reconnect budget exhausted). Terminal-ERROR payloads carry
  ``run_status: "error"``.

Preflight failures raise :class:`CloudRunnerError` instead (no run).

The terminal status is settled from the ``result`` SSE event plus a final
``GET /v1/agents/{id}/runs/{runId}`` as the authority — the simplified
``status`` SSE events are NOT trustworthy (a cancelled run's replay says
``FINISHED``; captured live, fixtures/rest_v1/run_c_postcancel.sse).

Watchdogs keep the previous semantics exactly (INACTIVITY-based, not
wall-clock): every SSE event (heartbeats included) resets the clock; an
in-flight tool call suspends it; the optional ``max_wall_s`` ceiling is
the runaway safety net.

Threading model: unchanged — the blocking SSE loop runs in a background
thread handing events to the calling thread through a queue; ``run_cloud``
is a plain synchronous generator like the old ``run_sdk``.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import re
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from . import local_mcp as _local_mcp
from . import workers as _workers
from .events import unified_diff_text
from .rest_client import (
    CursorRestClient,
    RestApiError,
    RestClientError,
    RestNetworkError,
    SseEvent,
)
from .runner import DEFAULT_MODEL, HarnessError, resolve_repo

logger = logging.getLogger(__name__)

# Watchdog defaults — semantics unchanged from the sdk transport.
DEFAULT_INACTIVITY_TIMEOUT_S = 600.0
DEFAULT_MAX_WALL_S = 0.0

# After cancel, how long the consumer waits for the worker to settle.
CANCEL_GRACE_S = 15.0
_POLL_S = 0.2

# Bounded transparent SSE recovery: consecutive drops bridged with
# Last-Event-ID reconnects before the run is declared failed. Any received
# event resets the counter.
MAX_STREAM_REATTACHES = 5
_REATTACH_BACKOFF_S = 2.0
_REATTACH_BACKOFF_CAP_S = 10.0

# REST run statuses that mean the run is over (upper-case wire form).
_TERMINAL_RUN_STATUSES = ("FINISHED", "ERROR", "CANCELLED", "EXPIRED")
# tool_call stream statuses that mean the call is no longer in flight.
_TERMINAL_TOOL_STATUSES = ("completed", "error", "failed", "cancelled")

# The phase-0 unroutable-worker signature: a machine-routed run on a FRESH
# (never-verified) worker that errors within this window having produced
# ZERO conversation events almost certainly means another worker on the
# same checkout is swallowing assignments.
UNROUTABLE_WINDOW_S = 90.0

# Cap for diff text produced by the git fallback (mirrors events.MAX_DIFF_CHARS).
_FALLBACK_DIFF_CHARS = 100_000

API_KEY_ENV = "CURSOR_API_KEY"

AGENTS_UI_BASE = "https://cursor.com/agents"

VALID_RUNTIMES = ("local", "cloud")


class CloudRunnerError(HarnessError):
    """Hard failure before the run started — no run happened."""


def rest_available() -> bool:
    """True when the http layer is importable (httpx is a hermes dep)."""
    try:
        import httpx  # noqa: F401

        return True
    except Exception:
        return False


def _default_cancel_check() -> bool:
    """Poll the Hermes per-thread interrupt flag (set by AIAgent.interrupt())."""
    try:
        from tools.interrupt import is_interrupted

        return is_interrupted()
    except Exception:
        return False


def make_client() -> CursorRestClient:
    """A REST client authenticated from the environment.

    Module-level seam: tests monkeypatch this with a fake-client factory.
    """
    api_key = os.environ.get(API_KEY_ENV, "")
    if not api_key.strip():
        raise CloudRunnerError(
            f"{API_KEY_ENV} is not set — create an API key at "
            "https://cursor.com/dashboard (API Keys) and export it, e.g. "
            f"`export {API_KEY_ENV}=your-key`"
        )
    return CursorRestClient(api_key=api_key)


# ---------------------------------------------------------------------------
# Legacy model-string translation (ACP/bridge-era slugs → REST ModelRef)
# ---------------------------------------------------------------------------
# The REST v1 model catalog exposes BASE ids ("claude-fable-5") plus
# per-model parameters/variants. Two legacy string forms still reach us:
#
# * dash suffix   — "claude-fable-5-thinking-high" (old CLI shorthand).
# * bracket suffix — "claude-fable-5[thinking=true,effort=high]" (older
#   handle records; resumed sessions replay these verbatim).
#
# translate_model maps both onto (base_id, params) for the create body's
# ModelRef. Unparseable forms fall back to DEFAULT_MODEL with a warning
# event rather than failing the run.

_EFFORT_LEVELS = {
    "low": "low",
    "medium": "medium",
    "high": "high",
    "extra-high": "xhigh",
    "xhigh": "xhigh",
    "max": "max",
}

_BRACKET_MODEL_RE = re.compile(r"^(?P<base>[^\[\]]*)\[(?P<params>[^\[\]]*)\]$")
_THINKING_SUFFIX_RE = re.compile(r"^(?P<base>.+?)-thinking(?:-(?P<level>.+))?$")

# str base id, {"id": ..., "params": [...]} dict, or None (= cursor default).
ModelValue = Any


def translate_model(model: Optional[str]) -> Tuple[Optional[ModelValue], Optional[str]]:
    """Normalize a requested model string for the REST API.

    Returns ``(model_value, warning)``:

    * plain base ids pass through unchanged (as strings);
    * ``<base>-thinking[-<level>]`` becomes ``{"id": base, "params":
      [thinking=true, effort=<level>]}``;
    * ``<base>[k=v,...]`` (legacy handle records) becomes ``{"id": base,
      "params": [...]}``;
    * unparseable/unknown suffix forms fall back to ``DEFAULT_MODEL`` with
      a human-readable ``warning`` (the caller emits it as a warning event).
    """
    raw = str(model or "").strip()
    if not raw:
        return None, None

    def _fallback(reason: str) -> Tuple[str, str]:
        return DEFAULT_MODEL, (
            f"requested model {raw!r} {reason} — "
            f"falling back to '{DEFAULT_MODEL}'"
        )

    if "[" in raw or "]" in raw:
        match = _BRACKET_MODEL_RE.match(raw)
        base = (match.group("base").strip() if match else "")
        if not match or not base:
            return _fallback("has an unparseable bracket suffix")
        params: List[Dict[str, str]] = []
        for part in match.group("params").split(","):
            part = part.strip()
            if not part:
                continue
            key, sep, val = part.partition("=")
            if not sep or not key.strip() or not val.strip():
                return _fallback(f"has an unparseable bracket parameter {part!r}")
            params.append({"id": key.strip(), "value": val.strip()})
        if not params:
            return base, None
        return {"id": base, "params": params}, None

    match = _THINKING_SUFFIX_RE.match(raw)
    if match:
        level = match.group("level")
        params = [{"id": "thinking", "value": "true"}]
        if level is not None:
            effort = _EFFORT_LEVELS.get(level.strip().lower())
            if effort is None:
                return _fallback("has an unrecognized '-thinking-…' suffix")
            params.append({"id": "effort", "value": effort})
        return {"id": match.group("base"), "params": params}, None

    return raw, None


def model_id_of(model: Optional[ModelValue]) -> str:
    """The base model id of a translated model value ("" when None)."""
    if isinstance(model, dict):
        return str(model.get("id") or "")
    return str(model or "")


def model_params_of(model: Optional[ModelValue]) -> Optional[List[Dict[str, str]]]:
    """The params list of a translated model value (None when absent)."""
    if isinstance(model, dict):
        params = model.get("params")
        return params if isinstance(params, list) and params else None
    return None


# Base catalog ids are slug-like ("claude-fable-5", "gpt-5.3-codex") —
# anything with whitespace/quotes/brackets in the BASE id is junk, not a
# model. Deliberately loose: only rejects strings no catalog id could be.
_MODEL_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/:-]*$")


def invalid_model_reason(model: Optional[str]) -> Optional[str]:
    """Why ``model`` is an obviously-invalid model string, or None.

    A pure SHAPE check — no network contact, so it is safe at
    ``cursor_create_session`` time (create stays lazy). Whether a
    well-formed id exists in the catalog is validated against
    ``GET /v1/models`` on the first send.
    """
    raw = str(model or "").strip()
    if not raw:
        return None
    value, warning = translate_model(raw)
    if warning:
        # The reason phrase without the send-time fallback clause: a
        # create-time caller REJECTS the string, it never substitutes.
        return warning.split(" — falling back", 1)[0]
    if not _MODEL_ID_RE.match(model_id_of(value)):
        return f"requested model {raw!r} is not a plausible model id"
    return None


# ---------------------------------------------------------------------------
# Model-catalog validation (GET /v1/models, cached per process)
# ---------------------------------------------------------------------------

_CATALOG_TTL_S = 600.0
_catalog_lock = threading.Lock()
_catalog_ids: Optional[List[str]] = None
_catalog_all: Optional[set] = None  # ids + aliases
_catalog_at = 0.0


def _model_catalog(client: CursorRestClient) -> Tuple[Optional[List[str]], Optional[set]]:
    """(catalog ids, ids+aliases) from GET /v1/models, cached with a TTL.

    ``(None, None)`` when the catalog cannot be fetched — validation is
    then SKIPPED (the server rejects invalid models itself; a flaky
    catalog endpoint must not block sends).
    """
    global _catalog_ids, _catalog_all, _catalog_at
    with _catalog_lock:
        if _catalog_ids is not None and time.monotonic() - _catalog_at < _CATALOG_TTL_S:
            return _catalog_ids, _catalog_all
    try:
        items = client.list_models()
    except RestClientError as exc:
        logger.warning("model catalog fetch failed — skipping validation: %s", exc)
        return None, None
    ids = [str(m.get("id") or "") for m in items if m.get("id")]
    everything = set(ids)
    for m in items:
        aliases = m.get("aliases")
        if isinstance(aliases, list):
            everything.update(str(a) for a in aliases)
    with _catalog_lock:
        _catalog_ids, _catalog_all, _catalog_at = ids, everything, time.monotonic()
    return ids, everything


def model_catalog_error(client: CursorRestClient, base_id: str) -> Optional[str]:
    """The clear unknown-model error for ``base_id``, or None when valid
    (or when the catalog is unavailable — server-side validation applies)."""
    if not base_id:
        return None
    ids, everything = _model_catalog(client)
    if everything is None or base_id in everything:
        return None
    return (
        f"model '{base_id}' is not in the cursor model catalog — valid ids: "
        + ", ".join(ids or [])
    )


# ---------------------------------------------------------------------------
# Git introspection (subprocess `git -C`, no cwd mutation)
# ---------------------------------------------------------------------------

def _git(args: List[str], cwd: Path) -> str:
    """Run git, returning stdout ("" on any failure). Never raises."""
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=30,
        )
        return proc.stdout or ""
    except Exception:
        return ""


_GITHUB_SSH_RE = re.compile(
    r"^(?:ssh://)?git@github\.com[:/](?P<path>.+?)(?:\.git)?/?$"
)
_GITHUB_HTTPS_RE = re.compile(
    r"^https?://(?:www\.)?github\.com/(?P<path>.+?)(?:\.git)?/?$"
)


def normalize_github_url(url: str) -> Optional[str]:
    """The canonical ``https://github.com/<owner>/<repo>`` form of a
    github remote url (ssh or https), or None for a non-github remote."""
    raw = str(url or "").strip()
    for pattern in (_GITHUB_SSH_RE, _GITHUB_HTTPS_RE):
        match = pattern.match(raw)
        if match:
            return f"https://github.com/{match.group('path')}"
    return None


def derive_repo_ref(repo_path: str) -> Tuple[str, str]:
    """(github https url, current branch) for a local checkout.

    Raises :class:`CloudRunnerError` with an actionable message when the
    origin remote is missing / not github, or HEAD is detached — cloud
    agents can only start from a github ref.
    """
    workdir = Path(repo_path)
    origin = _git(["-C", str(workdir), "remote", "get-url", "origin"], workdir).strip()
    if not origin:
        raise CloudRunnerError(
            f"{repo_path} has no 'origin' git remote — cloud agents start "
            "from a GitHub repo; add a github origin remote first"
        )
    url = normalize_github_url(origin)
    if url is None:
        raise CloudRunnerError(
            f"the origin remote of {repo_path} ({origin}) is not a "
            "github.com repo — cloud agents currently require GitHub"
        )
    branch = _git(
        ["-C", str(workdir), "rev-parse", "--abbrev-ref", "HEAD"], workdir
    ).strip()
    if not branch or branch == "HEAD":
        raise CloudRunnerError(
            f"{repo_path} is on a detached HEAD — check out a branch so "
            "the cloud agent has a starting ref"
        )
    return url, branch


def git_status_snapshot(workdir: Path | str) -> str:
    """Raw ``git status --porcelain`` text, captured before the run."""
    workdir = Path(workdir)
    return _git(["-C", str(workdir), "status", "--porcelain"], workdir)


def git_fallback_diffs(
    workdir: Path | str, before_status: str
) -> List[Dict[str, Any]]:
    """Diff entries for files whose git status changed during the run.

    Used when cursor made edits through paths that emitted no parseable
    diff content in the stream. Compares ``git status --porcelain``
    against the pre-run snapshot so pre-existing dirty files aren't
    misattributed; a pre-existing-dirty file that cursor edits *further*
    is the known blind spot of this fallback.

    Returns:
        Dicts with the :func:`events.file_diff` keyword shape:
        ``{path, before, after, diff, added, removed, status}``.
    """
    workdir = Path(workdir)
    after_status = _git(["-C", str(workdir), "status", "--porcelain"], workdir)
    if not after_status:
        return []
    before_lines = set((before_status or "").splitlines())
    entries: List[Dict[str, Any]] = []
    for line in after_status.splitlines():
        if not line or line in before_lines:
            continue
        xy, rel = line[:2], line[3:].strip()
        if "->" in rel:  # rename: take the new side
            rel = rel.split("->", 1)[1].strip()
        if rel.startswith('"') and rel.endswith('"'):
            try:
                rel = json.loads(rel)  # git quotes exotic paths C-style
            except ValueError:
                rel = rel.strip('"')
        abs_path = workdir / rel
        untracked = xy == "??"
        deleted = "D" in xy

        before_text = (
            "" if untracked
            else _git(["-C", str(workdir), "show", f"HEAD:{rel}"], workdir)
        )
        after_text = ""
        if not deleted:
            try:
                after_text = abs_path.read_text("utf-8")
            except Exception:
                continue  # binary/unreadable — skip rather than mislead
        diff_text, added, removed = unified_diff_text(
            before_text, after_text, str(abs_path)
        )
        if not diff_text:
            continue
        entries.append(
            {
                "path": str(abs_path),
                "before": before_text,
                "after": after_text,
                "diff": diff_text[:_FALLBACK_DIFF_CHARS],
                "added": added,
                "removed": removed,
                "status": "A" if untracked or "A" in xy else ("D" if deleted else "M"),
            }
        )
    return entries


# ---------------------------------------------------------------------------
# SSE event → message-dict conversion
# ---------------------------------------------------------------------------

def _message_from_sse(event: SseEvent) -> Optional[Dict[str, Any]]:
    """The ``cloud.message`` dict for one simplified SSE event, or None
    for pure control/duplicate traffic.

    The dict shapes match what :class:`events.SdkNormalizer` already
    parses (``type`` discriminator, snake_case ``call_id``). tool_call
    ``args``/``result`` payloads pass through verbatim — the normalizer
    parses them defensively.
    """
    data = event.data if isinstance(event.data, dict) else {}
    if event.event == "assistant":
        text = data.get("text")
        if isinstance(text, str) and text:
            return {"type": "assistant", "message": text}
        return None
    if event.event == "thinking":
        return {"type": "thinking", "text": data.get("text")}
    if event.event == "tool_call":
        message: Dict[str, Any] = {
            "type": "tool_call",
            "call_id": str(data.get("callId") or ""),
            "name": str(data.get("name") or ""),
            "status": str(data.get("status") or ""),
        }
        if "args" in data:
            message["args"] = data.get("args")
        if "result" in data:
            message["result"] = data.get("result")
        if "truncated" in data:
            message["truncated"] = data.get("truncated")
        return message
    if event.event in ("status", "heartbeat", "done", "result", "error"):
        return None  # control traffic — handled by the stream loop
    if event.event == "interaction_update":
        # Deliberately skipped: every interaction_update duplicates a
        # simplified event under the same event id (OpenAPI: "emitted
        # alongside the simplified events"; verified in the captures).
        # Consuming both would double-count every delta.
        return None
    # Unknown event type: generic passthrough — never silently dropped.
    return {
        "type": f"sse.{event.event or 'message'}",
        **(data if data else {"raw": event.raw_data}),
    }


def _lower_status(status: Any) -> str:
    return str(status or "").strip().lower()


# ---------------------------------------------------------------------------
# Worker (runs on a dedicated background thread)
# ---------------------------------------------------------------------------

class _CloudWorker:
    def __init__(
        self,
        task: str,
        repo: str,
        out_q: "queue.Queue[Tuple[str, Dict[str, Any]]]",
        cancel_requested: threading.Event,
        *,
        runtime: str,
        agent_id: Optional[str] = None,
        model: Optional[ModelValue] = None,
        repo_url: Optional[str] = None,
        starting_ref: Optional[str] = None,
        session_title: Optional[str] = None,
    ) -> None:
        self._task = task
        self._repo = repo
        self._out_q = out_q
        self._cancel_requested = cancel_requested
        self._runtime = runtime
        # Prior agent to continue with a follow-up run (None = fresh create).
        self._resume_agent_id = agent_id
        self._model = model or None
        self._model_id = model_id_of(model)
        self._repo_url = repo_url
        self._starting_ref = starting_ref
        self._session_title = session_title

        # "cancel" | "timeout" | "first_event" — written by the consumer
        # thread before it sets cancel_requested (single write, read after
        # the event fires).
        self.abort_reason: Optional[str] = None
        self.abort_detail: Optional[str] = None
        # Monotonic timestamp of the last SSE event received. Written by
        # the worker thread, read by the consumer's inactivity watchdog.
        self.last_activity_monotonic: float = time.monotonic()
        # Whether ANY run SSE event has been received yet — feeds the
        # consumer's optional first-event watchdog (issue #17: a retried
        # run that streams nothing must settle fast). GIL-safe bool.
        self.stream_event_seen: bool = False
        # call_ids with a tool_call seen (status "running") but no terminal
        # update yet — suspends the inactivity watchdog (cursor streams
        # nothing while a long command runs, but it is busy, not hung).
        self._pending_tool_calls: set = set()

        self._client: Optional[CursorRestClient] = None
        self._agent_id: str = ""
        self._run_id: str = ""
        self._cancel_sent = False
        self._settled = False
        # Conversation evidence for the unroutable-worker signature: True
        # once ANY assistant/thinking/tool_call message flowed.
        self._saw_conversation = False
        self._worker_record: Optional[_workers.WorkerRecord] = None
        self._started_monotonic = time.monotonic()
        # Per-RUN worker lease (runtime=local): installed atomically by
        # ensure_worker, bound to agent/run identity the instant the
        # create/follow-up returned, and RELEASED only after a remote
        # terminal state was actually observed (or when no agent was
        # ever established). An unconfirmed executor exit KEEPS the
        # lease — conservatively retained (safe leak) rather than risk
        # reaping a worker whose run may still be live server-side.
        self._lease_id = f"run-{uuid.uuid4().hex[:12]}"
        self._lease_held = False
        self._remote_terminal_observed = False
        # Create-uncertainty tracking for the lease release decision:
        # once a create/follow-up POST goes on the wire, a remote agent
        # may exist even if we never read the response. Releasing "no
        # agent was established" is only safe when the create was never
        # attempted, or the API POSITIVELY rejected it (a response was
        # received). A network failure mid-create retains the lease.
        self._create_attempted = False
        self._create_rejected = False

    # -- plumbing ----------------------------------------------------------

    def _put(self, key: str, obj: Dict[str, Any]) -> None:
        self._out_q.put((key, obj))

    def has_pending_tool_call(self) -> bool:
        """True while any tool call has started but not yet finished."""
        return bool(self._pending_tool_calls)

    def _track_tool_call(self, message: Dict[str, Any]) -> None:
        if str(message.get("type") or "") != "tool_call":
            return
        call_id = str(message.get("call_id") or "")
        if not call_id:
            return
        if str(message.get("status") or "") in _TERMINAL_TOOL_STATUSES:
            self._pending_tool_calls.discard(call_id)
        else:
            self._pending_tool_calls.add(call_id)

    def _timeout_error(self) -> Dict[str, Any]:
        detail = self.abort_detail or "watchdog abort"
        return {"error": f"cursor run timed out: {detail}", "timeout": True}

    def _first_event_error(self) -> Dict[str, Any]:
        """The first-event watchdog's failure payload (issue #17).

        Deliberately NOT flagged as a timeout and carrying no
        ``run_status``: the caller settles it as a plain failure (not a
        "timeout" result) and the zero-progress auto-retry gate never
        re-enters on it.
        """
        detail = self.abort_detail or "produced no stream events"
        return {"error": f"cursor run failed: {detail}"}

    # -- cancellation --------------------------------------------------------

    def _cancel_watcher(self) -> None:
        while not self._cancel_requested.is_set():
            if self._settled:
                return
            time.sleep(_POLL_S)
        if self._settled or self._cancel_sent:
            return
        self._cancel_sent = True
        client, agent_id, run_id = self._client, self._agent_id, self._run_id
        if client is None or not agent_id or not run_id:
            return
        try:
            client.cancel_run(agent_id, run_id)
        except RestApiError as exc:
            if exc.code != "run_not_cancellable":
                logger.warning("cancel_run failed: %s", exc)
        except RestClientError as exc:
            logger.warning("cancel_run failed: %s", exc)

    # -- agent establishment ---------------------------------------------------

    def _bind_lease(self, agent_id: str, run_id: str) -> None:
        """Bind the dispatch lease to the real remote identity the
        instant the create/follow-up returned: from here the worker is
        protected until an observed remote terminal, across gateway
        restarts and stream failures. Binding is AUTHORITATIVE —
        continuing without a bound lease is forbidden: on failure the
        run is CANCELLED rather than executed unprotected (fail
        closed).

        A PARTIAL identity (agent without a usable run id) is not bound
        and not treated as a bind failure: there is no run identity to
        cancel with, the dispatch fails via the caller's fatal path, and
        the UNBOUND lease keeps protecting the worker (leases never
        time-expire)."""
        if not self._lease_held or self._worker_record is None:
            return
        if not agent_id or not run_id:
            if agent_id or run_id:
                logger.warning(
                    "worker %s: create returned PARTIAL identity "
                    "(agent=%r run=%r) — lease %s stays unbound and "
                    "keeps protecting the worker",
                    self._worker_record.name, agent_id, run_id,
                    self._lease_id,
                )
            return
        if _workers.bind_lease(
            self._worker_record.name, self._lease_id, agent_id, run_id
        ):
            return
        logger.critical(
            "worker %s: run %s could not be protected by a bound lease — "
            "cancelling the run instead of executing unprotected",
            self._worker_record.name, run_id,
        )
        self.abort_reason = self.abort_reason or "cancel"
        self.abort_detail = (
            self.abort_detail
            or "run lease could not be bound — refused to execute unprotected"
        )
        self._cancel_requested.set()

    def _establish(self, client: CursorRestClient) -> Tuple[str, str, bool, str]:
        """Create the run: follow-up on the existing agent, else a fresh
        agent. Returns (agent_id, run_id, resumed, agents_ui_url)."""
        if self._resume_agent_id:
            try:
                self._create_attempted = True
                created = client.send_followup(self._resume_agent_id, self._task)
                run_id = str((created.get("run") or {}).get("id") or "")
                # Remote identity is captured the INSTANT it is known:
                # from here "no agent was established" is a lie and the
                # lease-release path must never claim it.
                self._agent_id = self._resume_agent_id
                self._run_id = run_id or self._run_id
                self._bind_lease(self._resume_agent_id, run_id)
                return (
                    self._resume_agent_id,
                    run_id,
                    True,
                    f"{AGENTS_UI_BASE}/{self._resume_agent_id}",
                )
            except RestApiError as exc:
                if exc.status_code == 409:
                    # Another run is active on the agent. The plugin's
                    # same-repo guard normally prevents this; surface it
                    # rather than silently forking a new agent.
                    raise
                logger.warning(
                    "follow-up on agent %s failed (%s) — falling back to a "
                    "fresh agent", self._resume_agent_id, exc,
                )

        env = (
            {"type": "machine", "name": self._worker_record.name}
            if self._runtime == "local" and self._worker_record is not None
            else None
        )
        repos = (
            [
                {
                    "url": self._repo_url,
                    **(
                        {"startingRef": self._starting_ref}
                        if self._starting_ref
                        else {}
                    ),
                }
            ]
            if self._repo_url
            else None
        )
        self._create_attempted = True
        created = client.create_agent(
            self._task,
            model_id=self._model_id or None,
            model_params=model_params_of(self._model),
            env=env,
            repos=repos,
            mcp_servers=(
                _local_mcp.configured_servers()
                if self._runtime == "local"
                else None
            ),
            # Local runtime works IN the local checkout: commits belong on
            # the branch that is actually checked out, not an auto branch.
            work_on_current_branch=True if self._runtime == "local" else None,
            name=self._session_title,
        )
        agent = created.get("agent") or {}
        run = created.get("run") or {}
        agent_id = str(agent.get("id") or "")
        run_id = str(run.get("id") or "")
        url = str(agent.get("url") or "") or f"{AGENTS_UI_BASE}/{agent_id}"
        # Remote identity is captured the INSTANT it is known — even a
        # PARTIAL response (agent id without a usable run id) proves a
        # remote agent exists, so the lease-release path must never
        # claim "no agent was established" past this point.
        self._agent_id = agent_id or self._agent_id
        self._run_id = run_id or self._run_id
        self._bind_lease(agent_id, run_id)
        return agent_id, run_id, False, url

    # -- stream consumption ------------------------------------------------------

    def _consume_stream(self, client: CursorRestClient) -> Optional[Dict[str, Any]]:
        """Drain the run's SSE stream, transparently reconnecting on drops.

        Returns the ``result`` event's data when one arrived (terminal
        detail: status/text/durationMs/git), else None. Raises on an
        exhausted reconnect budget.
        """
        last_event_id: Optional[str] = None
        reattaches = 0
        result_data: Optional[Dict[str, Any]] = None

        while True:
            saw_done = False
            saw_stream_error: Optional[Dict[str, Any]] = None
            try:
                for event in client.stream_run_events(
                    self._agent_id, self._run_id, last_event_id=last_event_id
                ):
                    reattaches = 0
                    self.last_activity_monotonic = time.monotonic()
                    self.stream_event_seen = True
                    if event.id:
                        last_event_id = event.id
                    if event.event == "result" and isinstance(event.data, dict):
                        result_data = event.data
                    elif event.event == "error" and isinstance(event.data, dict):
                        # e.g. {"code": "stream_unavailable"} — a stream-
                        # level condition, not a run failure (captured
                        # live, run_c_precancel.sse). Reconnect decides.
                        saw_stream_error = event.data
                        logger.warning(
                            "SSE stream error event for run %s: %s",
                            self._run_id, event.data,
                        )
                    elif event.event == "done":
                        saw_done = True
                    message = _message_from_sse(event)
                    if message is not None:
                        if message.get("type") in ("assistant", "thinking", "tool_call"):
                            self._mark_conversation()
                        self._track_tool_call(message)
                        self._put("cloud.message", message)
            except RestClientError as exc:
                if self._cancel_requested.is_set():
                    return result_data  # cancel racing the teardown
                if self._run_is_terminal(client):
                    return result_data  # run over; the stream died reporting it
                if (
                    isinstance(exc, RestApiError)
                    and not exc.retryable
                    and exc.status_code != 409
                ):
                    raise  # e.g. 404/410 on a live run — not reconnectable
                # A 409 from the stream endpoint (e.g. stream_unavailable)
                # means "not attachable right now", NOT "run failed" — the
                # same condition arrives as an in-stream `error` event and is
                # treated as reattachable there. Fall through to the reattach
                # path; _run_is_terminal above stays the authority each cycle.
                # (409 stays non-retryable globally: on POST /v1/agents it
                # means agent_busy and must not be blind-retried.)
                reattaches += 1
                if reattaches > MAX_STREAM_REATTACHES:
                    raise
                delay = min(
                    _REATTACH_BACKOFF_S * reattaches, _REATTACH_BACKOFF_CAP_S
                )
                logger.warning(
                    "SSE stream for run %s dropped (%s: %s) — reconnecting "
                    "with Last-Event-ID=%r in %.1fs",
                    self._run_id, type(exc).__name__, exc, last_event_id, delay,
                )
                if self._cancel_requested.wait(delay):
                    return result_data
                self._put(
                    "sse.reattached",
                    {"last_event_id": last_event_id, "attempt": reattaches},
                )
                continue

            # Stream closed without an exception. A terminal run's stream
            # replays fully and closes after `done` — but a LIVE run's
            # stream can also close early (stream_unavailable). The run
            # status decides whether to settle or reconnect.
            if result_data is not None or self._run_is_terminal(client):
                return result_data
            if self._cancel_requested.is_set():
                return result_data
            reattaches += 1
            if reattaches > MAX_STREAM_REATTACHES:
                raise RestNetworkError(
                    f"SSE stream for run {self._run_id} keeps closing "
                    f"({saw_stream_error or 'no error detail'}) while the "
                    "run is still live — reconnect budget exhausted"
                )
            delay = min(_REATTACH_BACKOFF_S * reattaches, _REATTACH_BACKOFF_CAP_S)
            logger.warning(
                "SSE stream for run %s closed early (%s%s) — reconnecting "
                "with Last-Event-ID=%r in %.1fs",
                self._run_id, saw_stream_error or "clean close",
                " after done" if saw_done else "", last_event_id, delay,
            )
            if self._cancel_requested.wait(delay):
                return result_data
            self._put(
                "sse.reattached",
                {"last_event_id": last_event_id, "attempt": reattaches},
            )

    def _mark_conversation(self) -> None:
        if self._saw_conversation:
            return
        self._saw_conversation = True
        # First real conversation traffic through a machine-routed run is
        # the routability proof for a fresh worker.
        if self._worker_record is not None and not self._worker_record.verified:
            try:
                _workers.mark_verified(self._worker_record.name)
            except Exception:
                logger.debug("mark_verified failed", exc_info=True)

    def _run_is_terminal(self, client: CursorRestClient) -> bool:
        terminal = self._final_status(client) in _TERMINAL_RUN_STATUSES
        if terminal:
            self._remote_terminal_observed = True
        return terminal

    def _final_status(self, client: CursorRestClient) -> str:
        """The run's status per GET runs/{id} — the settle authority."""
        try:
            return str(client.get_run(self._agent_id, self._run_id).get("status") or "")
        except RestClientError as exc:
            logger.warning("get_run failed: %s", exc)
            return ""

    # -- main flow -----------------------------------------------------------

    def run(self) -> None:
        watcher = threading.Thread(
            target=self._cancel_watcher, name="ghost-cursor-cloud-cancel", daemon=True
        )
        try:
            self._run_inner(watcher)
        except Exception as exc:  # belt-and-braces: never strand the consumer
            logger.exception("cursor cloud worker crashed")
            self._put("cloud.error", {"error": f"cursor cloud worker crashed: {exc}"})
        finally:
            self._settled = True
            # Release the per-run lease ONLY when a remote terminal state
            # was actually observed, or when it is POSITIVELY known that
            # no remote run can exist: the create was never attempted, or
            # the API authoritatively rejected it (a response was
            # received) with no identity returned. Everything else —
            # unconfirmed executor exit, network failure mid-create,
            # partial identity — KEEPS the lease (conservative safe leak)
            # rather than risk reaping a worker whose run may be live
            # server-side.
            no_remote_possible = not self._agent_id and (
                not self._create_attempted or self._create_rejected
            )
            if self._lease_held and self._worker_record is not None:
                if self._remote_terminal_observed or no_remote_possible:
                    _workers.release_lease(
                        self._worker_record.name, self._lease_id,
                        agent_id=self._agent_id,
                    )
                else:
                    logger.info(
                        "worker %s: lease %s retained (remote terminal not "
                        "observed) — a later run's terminal proof on the "
                        "same agent settles it",
                        self._worker_record.name, self._lease_id,
                    )
            self._put("__done__", {})

    def _run_inner(self, watcher: threading.Thread) -> None:
        try:
            client = make_client()
        except CloudRunnerError as exc:
            self._put("cloud.fatal", {"error": str(exc)})
            return
        self._client = client

        # PURE preflights first — nothing below may cost a worker spawn
        # or a lease. Model-catalog validation (clear error listing
        # valid ids) happens before any worker exists so a malformed
        # request never leaves an idle worker behind.
        catalog_error = model_catalog_error(client, self._model_id)
        if catalog_error:
            self._put("cloud.fatal", {"error": catalog_error})
            return

        # Worker (runtime=local): reuse-or-spawn with the per-RUN lease
        # installed ATOMICALLY under the same ownership lock — a failure
        # to lease fails the dispatch (fail closed), never yields an
        # unprotected worker.
        if self._runtime == "local":
            try:
                self._worker_record = _workers.ensure_worker(
                    self._repo,
                    lease_id=self._lease_id,
                    lease_session=self._session_title or "",
                )
            except _workers.WorkerError as exc:
                self._put("cloud.fatal", {"error": str(exc)})
                return
            # Held only when the returned record really carries our
            # lease (the atomic path guarantees it; ephemeral records
            # from tests/fakes without durable state lease nothing).
            self._lease_held = self._lease_id in (
                self._worker_record.leases or {}
            )

        try:
            agent_id, run_id, resumed, ui_url = self._establish(client)
        except RestClientError as exc:
            # An API RESPONSE (RestApiError) is an authoritative create
            # rejection — no remote run exists and the lease may be
            # released. A NETWORK failure mid-create is ambiguous (the
            # POST may have succeeded server-side): the lease is kept.
            self._create_rejected = isinstance(exc, RestApiError)
            model_hint = (
                f"the requested model {self._model_id!r}"
                if self._model_id
                else "the configured model"
            )
            self._put(
                "cloud.fatal",
                {
                    "error": (
                        f"cursor agent create failed ({exc}). Check "
                        f"{API_KEY_ENV} and {model_hint}."
                    )
                },
            )
            return
        if not agent_id or not run_id:
            self._put(
                "cloud.fatal",
                {"error": "cursor agent create returned no agent/run id"},
            )
            return
        self._agent_id, self._run_id = agent_id, run_id

        self._put(
            "cloud.session",
            {
                "agentId": agent_id,
                "run_id": run_id,
                "cwd": self._repo,
                "model": self._model_id or DEFAULT_MODEL,
                "resumed": resumed,
                "runtime": self._runtime,
                "worker": (
                    self._worker_record.name if self._worker_record else ""
                ),
                # "systemd" | "detached" | "legacy" — "detached" is the
                # clearly-surfaced DEGRADED supervision fallback.
                "worker_supervision": (
                    self._worker_record.supervision
                    if self._worker_record
                    else ""
                ),
                "agents_ui_url": ui_url,
                "repo_url": self._repo_url or "",
                "starting_ref": self._starting_ref or "",
            },
        )
        watcher.start()

        try:
            result_data = self._consume_stream(client)
        except RestClientError as exc:
            self._settled = True
            if self.abort_reason == "timeout":
                self._put("cloud.error", self._timeout_error())
            elif self.abort_reason == "first_event":
                self._put("cloud.error", self._first_event_error())
            elif self.abort_reason == "cancel":
                self._put("cloud.result", {"status": "cancelled"})
            else:
                self._put(
                    "cloud.error",
                    {
                        "error": (
                            "cursor event stream failed mid-run: "
                            f"{type(exc).__name__}: {exc}"
                        )
                    },
                )
            return

        # Settle: the result event + a final GET as the authority (the
        # simplified SSE status events lie about cancelled runs).
        authoritative_status = _lower_status(self._final_status(client))
        status = authoritative_status or _lower_status(
            (result_data or {}).get("status")
        )
        if authoritative_status in ("finished", "cancelled", "expired", "error"):
            # ONLY the GET authority proves the remote terminal state: a
            # replayed SSE result can lie (FINISHED on a cancelled run),
            # so the display fallback below never releases the lease.
            self._remote_terminal_observed = True
        self._settled = True
        if self.abort_reason == "timeout":
            self._put("cloud.error", self._timeout_error())
        elif self.abort_reason == "first_event":
            self._put("cloud.error", self._first_event_error())
        elif status == "error":
            self._put("cloud.error", self._terminal_error(result_data))
        elif status in ("finished", "cancelled", "expired"):
            self._put("cloud.result", {"status": status})
        else:
            self._put(
                "cloud.error",
                {"error": f"cursor run ended with status: {status or 'unknown'}"},
            )

    def _terminal_error(self, result_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """The typed payload for a run that settled with status ERROR."""
        if (
            self._runtime == "local"
            and not self._saw_conversation
            and self._worker_record is not None
            and not self._worker_record.verified
            and time.monotonic() - self._started_monotonic <= UNROUTABLE_WINDOW_S
        ):
            # Phase-0 signature: fast ERROR, zero conversation events, on
            # a never-verified worker → almost certainly a routing
            # conflict, not a model/run failure. Non-retryable: re-sending
            # the same prompt into the same broken routing can't succeed.
            return {
                "error": _workers.unroutable_hint(
                    self._worker_record.name, self._repo
                ),
                "run_status": "error",
                "unroutable_worker": True,
                "retryable": False,
            }
        text = str((result_data or {}).get("text") or "").strip()
        return {
            "error": (
                f"cursor run ended with status: error"
                + (f" — {text}" if text else "")
            ),
            "run_status": "error",
        }


# ---------------------------------------------------------------------------
# Synchronous generator facade (what the orchestration consumes)
# ---------------------------------------------------------------------------

def run_cloud(
    task: str,
    repo: str,
    inactivity_timeout_s: Optional[float] = None,
    max_wall_s: Optional[float] = None,
    cancel_check: Optional[Callable[[], bool]] = None,
    agent_id: Optional[str] = None,
    model: Optional[str] = None,
    runtime: str = "local",
    repo_url: Optional[str] = None,
    starting_ref: Optional[str] = None,
    session_title: Optional[str] = None,
    first_event_timeout_s: Optional[float] = None,
) -> Iterator[Tuple[str, Dict[str, Any]]]:
    """Run cursor on ``task`` as a cloud agent, yielding events.

    Yields ``("cloud.session"|"cloud.message"|"sse.reattached"|
    "cloud.result"|"cloud.error"|"cloud.model_warning", obj)`` tuples (see
    module docstring). Polls ``cancel_check`` (default: the Hermes
    per-thread interrupt flag) and the watchdogs between events; every
    trigger fires a REST ``cancel`` on the run.

    ``agent_id`` continues an existing cloud agent (``bc-...``) with a
    follow-up run — stateless, so it works across process restarts with no
    resume handshake. A follow-up whose agent no longer exists falls back
    to a fresh agent.

    ``runtime`` routes execution: "local" (default) targets the
    plugin-managed worker for ``repo`` (spawned on demand); "cloud" uses a
    cursor-hosted VM. ``repo_url``/``starting_ref`` are derived from the
    checkout via ``git -C`` when not supplied (local paths only).

    ``model`` accepts base ids AND the legacy string forms
    ("<id>-thinking-<level>", "<id>[k=v,...]") — :func:`translate_model`
    maps them before anything reaches the API; unparseable forms fall back
    to ``DEFAULT_MODEL`` with a ``cloud.model_warning`` event.

    ``first_event_timeout_s`` — optional tight first-event watchdog
    (issue #17, armed by the caller's auto-retry loop): abort when NO
    run SSE event has arrived this many seconds after dispatch. Fires a
    plain (non-timeout, no ``run_status``) ``cloud.error`` so the run
    settles failed and is never auto-retried again. Inert once the first
    event arrives; None/0 disables (the default).

    Raises:
        HarnessError: empty task / bad repo (preflight).
        CloudRunnerError: missing api key, non-github origin, detached
            HEAD, invalid runtime — actionable message, no run.
    """
    if not str(task).strip():
        raise HarnessError("empty task")
    runtime = str(runtime or "local").strip() or "local"
    if runtime not in VALID_RUNTIMES:
        raise CloudRunnerError(
            f"unknown runtime {runtime!r} — valid: {', '.join(VALID_RUNTIMES)}"
        )
    if not os.environ.get(API_KEY_ENV, "").strip():
        raise CloudRunnerError(
            f"{API_KEY_ENV} is not set — create an API key at "
            "https://cursor.com/dashboard (API Keys) and export it, e.g. "
            f"`export {API_KEY_ENV}=your-key`"
        )

    if runtime == "cloud" and normalize_github_url(repo):
        # runtime=cloud accepts a github url directly — no local checkout.
        workdir_str = normalize_github_url(repo)
        derived_url, derived_ref = workdir_str, None
    else:
        workdir = resolve_repo(repo)
        if runtime == "local":
            # Canonical worktree identity: worker naming, admission, and
            # the execution cwd all key the worktree TOPLEVEL, never a
            # subdir. LOCAL-runtime-only — cloud runs preserve the
            # caller's path exactly (origin/main parity).
            workdir_str = _workers.canonical_repo_path(str(workdir))
        else:
            workdir_str = str(workdir)
        if repo_url and starting_ref:
            derived_url, derived_ref = repo_url, starting_ref
        else:
            derived_url, derived_ref = derive_repo_ref(workdir_str)

    if cancel_check is None:
        cancel_check = _default_cancel_check

    inactivity_s = float(
        inactivity_timeout_s
        if inactivity_timeout_s is not None
        else DEFAULT_INACTIVITY_TIMEOUT_S
    )
    wall_s = float(max_wall_s if max_wall_s is not None else DEFAULT_MAX_WALL_S)

    model_value, model_warning = translate_model(model)
    if model_warning:
        logger.warning("ghost_cursor model translation: %s", model_warning)

    out_q: "queue.Queue[Tuple[str, Dict[str, Any]]]" = queue.Queue()
    cancel_requested = threading.Event()
    worker = _CloudWorker(
        task=str(task),
        repo=workdir_str,
        out_q=out_q,
        cancel_requested=cancel_requested,
        runtime=runtime,
        agent_id=(str(agent_id).strip() or None) if agent_id else None,
        model=model_value,
        repo_url=derived_url,
        starting_ref=derived_ref,
        session_title=session_title,
    )
    thread = threading.Thread(
        target=worker.run, name="ghost-cursor-cloud", daemon=True
    )
    thread.start()

    if model_warning:
        yield (
            "cloud.model_warning",
            {
                "warning": model_warning,
                "requested": str(model or "").strip(),
                "using": model_id_of(model_value) or DEFAULT_MODEL,
            },
        )

    first_event_s = float(first_event_timeout_s or 0.0)

    started = time.monotonic()
    fatal: Optional[str] = None
    try:
        while True:
            if not cancel_requested.is_set():
                now = time.monotonic()
                if (
                    first_event_s > 0
                    and not worker.stream_event_seen
                    and now - started >= first_event_s
                ):
                    worker.abort_reason = "first_event"
                    worker.abort_detail = (
                        "produced no stream events within "
                        f"{int(first_event_s)}s of dispatch"
                    )
                    cancel_requested.set()
                elif (
                    inactivity_s > 0
                    and now - worker.last_activity_monotonic >= inactivity_s
                    # An in-flight tool call IS activity: cursor streams no
                    # events while a long command runs, but it is busy.
                    and not worker.has_pending_tool_call()
                ):
                    worker.abort_reason = "timeout"
                    worker.abort_detail = f"no activity for {int(inactivity_s)}s"
                    cancel_requested.set()
                elif wall_s > 0 and now - started >= wall_s:
                    worker.abort_reason = "timeout"
                    worker.abort_detail = f"exceeded max wall time ({int(wall_s)}s)"
                    cancel_requested.set()
                elif cancel_check():
                    worker.abort_reason = "cancel"
                    cancel_requested.set()
            try:
                key, obj = out_q.get(timeout=_POLL_S)
            except queue.Empty:
                if not thread.is_alive() and out_q.empty():
                    break  # producer died without its sentinel — defensive
                continue
            if key == "__done__":
                break
            if key == "cloud.fatal":
                fatal = str(obj.get("error") or "cursor cloud failure")
                continue  # drain to the sentinel, then raise below
            yield key, obj
    finally:
        # Consumer abandoned us (exception/GeneratorExit) or normal exit:
        # make sure the run is cancelled and the worker unwinds.
        cancel_requested.set()
        thread.join(timeout=CANCEL_GRACE_S)

    if fatal is not None:
        raise CloudRunnerError(fatal)
