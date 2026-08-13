"""Task-backed session supervisor (RFC: docs/rfcs/session-supervisor.md).

Supervision state is durable and owned by a supervisor, not the tool call:
every dispatched session carries a ``supervision`` record on its handle
entry (``handles.supervision_of``)::

    {phase, current_attempt_id, attempt_n,
     last_seq_delivered: {subscriber: seq},
     watchdog: {last_poll_ts, last_remote_status, last_event_id}}

A **reconciler** (k8s controller pattern, RFC §1) runs at plugin init and
every :data:`RECONCILE_INTERVAL_S`: for every handle in a non-terminal
supervision phase with no live supervision in this process — no running
job (the in-process supervision executor) and no live
:class:`SessionSupervisor` task — it spawns a supervisor that RE-ATTACHES
to the remote run. A gateway restart is therefore a non-event: the
process comes back, the reconciler sees live handles, supervisors
re-attach, digests resume, and the completion is delivered exactly once
per subscriber instead of the session dying as
``failed (orphaned: plugin process restarted mid-run)``.

The re-attached supervisor is a single loop per session that owns
(RFC §1): SSE stream consumption, ingest (seq assignment + jsonl append),
digest ticks, completion delivery, and the poll watchdog.

Core invariants:

* **Push with poll fallback (RFC §2):** primary is the run's SSE stream
  with ``Last-Event-ID`` resume; a stream that is silent AND
  unreconnectable degrades to polling ``GET runs/{id}`` every
  :data:`WATCHDOG_INTERVAL_S`.
* **Terminal precedence (RFC §2):** the remote GET's terminal status
  ALWAYS wins over a replayed stream's terminal status — a cancelled
  run's replay emits ``status: FINISHED`` while the GET says CANCELLED
  (verified live). Settlement never reads terminal state from replay.
* **Single-writer settlement (RFC §3):** only the supervisor settles a
  session — the terminal phase write is an atomic live→terminal
  transition (``handles.transition_supervision``) and completion fan-out
  happens exactly once, behind that gate. Tool calls (send/stop) REQUEST
  transitions (:func:`request_stop` / :func:`stop_and_wait`); the
  supervisor applies them.
* **Ingest boundary (RFC §4):** ``interaction_update`` twins are deduped
  by provider event id BEFORE seq assignment (the seq is assigned by the
  jsonl append); every event is stamped with ``attemptId``;
  ``lifecycle.durable_progress`` is derived supervisor-side from observed
  ``file_diff`` / completed ``tool_result`` events — never trusted from
  agent self-report.
* **Retry policy (RFC §5):** implemented at the dispatch site (the
  in-process attempt loop in ``__init__._execute_cursor_run``) with the
  primitives here — :func:`begin_attempt` (cap :data:`MAX_AUTO_RETRIES`),
  ``lifecycle.retry_started`` / ``lifecycle.retry_suppressed``, and
  death-shape tagging (:func:`death_shape`) on the settled event. A
  RE-ATTACHED supervisor never auto-reprompts: the original prompt text
  is not durably recorded in full, so a zero-progress failure after a
  restart surfaces as a failure requiring explicit resume.
* **Delivery cursors:** ``last_seq_delivered`` advances only after a
  successful enqueue onto the completion queue
  (``handles.advance_delivery_cursor`` is advance-only). Duplicate
  digests are acceptable (consumers dedupe on delegation id); completion
  delivery is exactly-once per subscriber (the settle gate).

Threading model: one daemon thread per re-attached session plus a daemon
reconciler timer; stream consumption runs on a nested thread handing
events to the supervisor loop through a queue (the ``cloud_runner``
pattern), so digest ticks and the watchdog stay live while the SSE read
blocks.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from . import cloud_runner as _cloud
from . import eventlog as _eventlog
from . import events as _events
from . import handles as _handles
from . import render as _render
from . import workers as _workers
from .rest_client import RestApiError, RestClientError

logger = logging.getLogger(__name__)

# Reconciler cadence (RFC §1: plugin init + every 60s).
RECONCILE_INTERVAL_S = 60.0
# Watchdog: poll GET runs/{id} when the stream is silent AND
# unreconnectable this long (RFC §2). Module-level so tests can shrink it.
WATCHDOG_INTERVAL_S = 60.0
# Auto-retry cap for zero-durable-progress attempts (RFC §5).
MAX_AUTO_RETRIES = 3
# Death-shape boundary (RFC §5): a failed run under this age with
# lifecycle-only events is a ``fast_fail``; anything that streamed real
# events is ``mid_flight`` — the recovery playbook differs and the caller
# shouldn't have to re-derive it from the log.
FAST_FAIL_WINDOW_S = 120.0
# False-settle repair (remote authority wins in BOTH directions): a handle
# whose LOCAL status is terminal but whose remote run GETs as RUNNING was
# falsely settled (e.g. a send-time preflight failure overwrote a healthy
# run). The reconciler probes recently-settled terminal handles against the
# GET authority and un-settles them. Bounded by this window so the pass
# never polls the whole terminal backlog forever.
FALSE_SETTLE_REPAIR_WINDOW_S = 15 * 60.0
# Create-intent recovery (the API has NO idempotency key): an intent still
# "creating" after this age with no live job is presumed crashed and is
# recovered by title/time matching. Generous: POST /v1/agents has been
# observed to take >30s, and a healthy dispatcher clears the intent at the
# producer boundary within one request round-trip.
CREATE_INTENT_MIN_AGE_S = 300.0
# An adopted agent must have been created no earlier than this long before
# the intent was persisted (clock-skew allowance).
CREATE_MATCH_SKEW_S = 120.0

# Reconnect pacing for the re-attached stream (bounded per drop; the
# supervisor itself never gives up — it degrades to the poll watchdog).
_RECONNECT_BACKOFF_S = 2.0
_RECONNECT_BACKOFF_CAP_S = 10.0
# Consecutive drops before degrading from push to the poll watchdog.
_DROPS_BEFORE_POLL_FALLBACK = 3
# Queue poll granularity for the supervisor loop.
_POLL_S = 0.2

# Supervision phases (live set mirrors handles.SUPERVISION_LIVE_PHASES).
PHASE_SPAWNING = "spawning"
PHASE_STREAMING = "streaming"
PHASE_RETRYING = "retrying"
TERMINAL_PHASES = _handles.SUPERVISION_TERMINAL_PHASES

# REST run statuses that mean the run is over (upper-case wire form) and
# their local terminal-status mapping.
_REMOTE_TERMINAL = {
    "FINISHED": "completed",
    "ERROR": "failed",
    "CANCELLED": "cancelled",
    "EXPIRED": "timeout",
}


def mint_attempt_id() -> str:
    """A fresh stable attempt id, stamped on every event as ``attemptId``."""
    return f"att-{uuid.uuid4().hex[:12]}"


def stamp(envelope: Dict[str, Any], attempt_id: str) -> Dict[str, Any]:
    """The envelope with ``attemptId`` stamped (mandatory, RFC §4)."""
    if not attempt_id or envelope.get("attemptId"):
        return envelope
    return {**envelope, "attemptId": attempt_id}


def is_durable_evidence(envelope: Dict[str, Any]) -> bool:
    """Whether one canonical envelope is durable-progress evidence.

    Controller-derived from the observed event log (RFC §4), never from
    agent self-report: any ``file_diff``, or any COMPLETED ``tool_result``.
    The completed-tool rule is deliberately conservative — the envelope
    schema cannot prove a completed shell call was reversible, and the
    consumer of this signal (the auto-retry gate) must err toward never
    double-applying work.
    """
    kind = str(envelope.get("kind") or "")
    if kind == "file_diff":
        return True
    return (
        kind == "tool_result"
        and str(envelope.get("status") or "") == _events.STATUS_DONE
    )


def death_shape(elapsed_s: float, nonlifecycle_events: int) -> str:
    """``fast_fail`` vs ``mid_flight`` for a failed run (RFC §5)."""
    if elapsed_s < FAST_FAIL_WINDOW_S and nonlifecycle_events == 0:
        return "fast_fail"
    return "mid_flight"


# ---------------------------------------------------------------------------
# Supervision-record transitions (called from the dispatch/finalize paths)
# ---------------------------------------------------------------------------

def begin_attempt(session_name: str, attempt_n: int) -> str:
    """Open attempt ``attempt_n`` on a session: mint the attempt id and
    write the supervision record (phase ``spawning`` for the first
    attempt, ``retrying`` for retries). Returns the attempt id — the
    ``attemptId`` stamped on every event of this attempt."""
    attempt_id = mint_attempt_id()
    _handles.record_supervision(
        session_name,
        phase=PHASE_SPAWNING if attempt_n <= 1 else PHASE_RETRYING,
        current_attempt_id=attempt_id,
        attempt_n=int(attempt_n),
    )
    return attempt_id


def mark_streaming(session_name: str) -> None:
    """Phase ``streaming`` — the agent exists and events are flowing."""
    _handles.record_supervision(session_name, phase=PHASE_STREAMING)


def settle_from_job(job: Any, status: str) -> None:
    """Settle the supervision record for an in-process run (called by
    ``jobs._finalize`` — the single settle writer for supervised jobs).

    Writes the terminal phase and appends the supervisor-derived
    ``lifecycle.session.settled`` event carrying the attempt identity and
    the death-shape tag for failures (RFC §5). Never raises.
    """
    try:
        name = job.session_name or job.cursor_session_id
        if not name:
            return
        _handles.record_supervision(name, phase=str(status))
        elapsed = (job.finished_at or time.time()) - job.created_at
        shape = (
            {"death_shape": death_shape(elapsed, job.nonlifecycle_events)}
            if status != "completed"
            else {}
        )
        _eventlog.append(
            name,
            stamp(
                _events.lifecycle(
                    "session.settled",
                    status=str(status),
                    attempt_n=getattr(job, "attempt_n", 1),
                    **shape,
                ),
                str(getattr(job, "current_attempt_id", "") or ""),
            ),
        )
    except Exception:
        logger.debug("ghost_cursor supervision settle failed", exc_info=True)


# ---------------------------------------------------------------------------
# Re-attached supervisor (spawned by the reconciler)
# ---------------------------------------------------------------------------

def _enqueue_completion_event(evt: Dict[str, Any]) -> bool:
    """Put one event on the shared completion queue. False on failure."""
    try:
        from tools.process_registry import process_registry

        process_registry.completion_queue.put(evt)
        return True
    except Exception as exc:
        logger.error(
            "ghost_cursor supervisor: completion enqueue failed: %s", exc
        )
        return False


def _subscriber_suffix(session_key: str) -> str:
    """Short stable subscriber hash (mirrors progress.subscriber_suffix;
    duplicated to keep this module import-light)."""
    import hashlib

    return hashlib.sha1(str(session_key or "").encode("utf-8")).hexdigest()[:8]


class SessionSupervisor:
    """One re-attached session's supervisor loop (RFC §1).

    Owns stream consumption, ingest, digests, the watchdog, and
    settlement for a session whose dispatching process died. Spawned by
    the reconciler; exits when the session settles (or when it cannot
    make progress this pass — the reconciler re-attaches on its next
    sweep, so supervision never silently dies).
    """

    def __init__(self, session_name: str) -> None:
        self.session_name = str(session_name)
        self._stop_requested = threading.Event()
        self.settled = threading.Event()
        self._thread: Optional[threading.Thread] = None

        self._attempt_id = ""
        self._cancel_sent = False
        self._attached_monotonic = time.monotonic()
        # Ingest state.
        self._seen_event_ids: set = set()
        self._last_event_id: Optional[str] = None
        self._nonlifecycle_events = 0
        self._durable_emitted = False
        self._files: Dict[str, Dict[str, Any]] = {}
        self._prose_blocks: List[str] = []
        self._prose_open = False
        # In-flight tool calls (call id -> {tool, title, since}) + the
        # latest cursor plan snapshot — same digest context the live
        # runner's fold keeps (see __init__._fold_envelope).
        self._pending_tools: Dict[str, Dict[str, Any]] = {}
        self._plan_items: List[Dict[str, str]] = []
        # Digest state: subscriber key -> monotonic next-due timestamp.
        self._digest_due: Dict[str, float] = {}
        self._digest_n: Dict[str, int] = {}

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        thread = threading.Thread(
            target=self._run,
            name=f"ghost-cursor-supervisor-{self.session_name}",
            daemon=True,
        )
        self._thread = thread
        thread.start()

    def is_alive(self) -> bool:
        thread = self._thread
        return thread is not None and thread.is_alive()

    def request_stop(self) -> None:
        """Request a cancel transition (tool calls request, the supervisor
        applies — RFC §3)."""
        self._stop_requested.set()

    # -- main loop -----------------------------------------------------------

    def _run(self) -> None:
        try:
            self._supervise()
        except Exception:
            # Deliberately NOT settled: an unexpected crash leaves the
            # phase live so the next reconciler pass re-attaches.
            logger.exception(
                "ghost_cursor supervisor for %s crashed", self.session_name
            )
        finally:
            _forget(self.session_name, self)

    def _supervise(self) -> None:
        name = self.session_name
        entry = _handles.get(name) or {}
        sup = _handles.supervision_of(entry)
        self._attempt_id = sup["current_attempt_id"] or mint_attempt_id()
        self._last_event_id = (
            str(sup["watchdog"].get("last_event_id") or "") or None
        )

        agent_id = str(entry.get("cursor_session_id") or "")
        if not agent_id:
            # RFC §6: "orphaned" now only means a handle whose remote
            # agent id no longer resolves — nothing to re-attach to.
            self._settle(
                "failed",
                error="orphaned: no remote agent id recorded for this session",
            )
            return

        try:
            client = _cloud.make_client()
        except _cloud.CloudRunnerError as exc:
            # No API key / client preflight — cannot supervise this pass;
            # leave the phase live for the next reconciler sweep.
            logger.warning(
                "ghost_cursor supervisor for %s cannot attach: %s", name, exc
            )
            return

        run_id = str(entry.get("latest_run_id") or "")
        if not run_id:
            run_id = self._latest_run_id(client, agent_id)
        if not run_id:
            self._settle(
                "failed",
                error=(
                    "orphaned: the remote agent has no resolvable run to "
                    "re-attach to"
                ),
            )
            return

        self._ingest_lifecycle(
            "supervisor.reattached",
            agent_id=agent_id,
            run_id=run_id,
            last_event_id=self._last_event_id,
        )
        self._stream_and_settle(client, agent_id, run_id)

    def _latest_run_id(self, client: Any, agent_id: str) -> str:
        try:
            runs = client.list_runs(agent_id).get("runs")
        except RestClientError as exc:
            logger.warning(
                "ghost_cursor supervisor for %s: list_runs failed: %s",
                self.session_name, exc,
            )
            return ""
        if not isinstance(runs, list) or not runs:
            return ""
        return str((runs[0] or {}).get("id") or "")

    # -- push with poll fallback (RFC §2) ------------------------------------

    def _stream_and_settle(self, client: Any, agent_id: str, run_id: str) -> None:
        """Consume the stream (reconnecting on drops), fall back to the
        poll watchdog when unreconnectable, settle from the GET authority."""
        out_q: "queue.Queue[Tuple[str, Any]]" = queue.Queue()
        stream_done = threading.Event()

        def _pump() -> None:
            drops = 0
            try:
                while not self.settled.is_set() and not stream_done.is_set():
                    try:
                        for event in client.stream_run_events(
                            agent_id, run_id, last_event_id=self._last_event_id
                        ):
                            drops = 0
                            out_q.put(("event", event))
                        # Clean close: a terminal run's replay ends after
                        # `done`; a live run's stream can also close early.
                        # The GET decides — signal a status check.
                        out_q.put(("closed", None))
                    except RestClientError as exc:
                        drops += 1
                        out_q.put(("dropped", exc))
                        if isinstance(exc, RestApiError) and exc.status_code in (
                            404, 410,
                        ):
                            return  # gone/expired: poll watchdog owns it now
                        if drops >= _DROPS_BEFORE_POLL_FALLBACK:
                            return  # unreconnectable — degrade to polling
                        delay = min(
                            _RECONNECT_BACKOFF_S * drops,
                            _RECONNECT_BACKOFF_CAP_S,
                        )
                        if stream_done.wait(delay):
                            return
                        continue
                    # After a clean close, wait for the consumer to decide
                    # (it either settles or asks for a reconnect by not
                    # setting stream_done); pace the reconnect.
                    if stream_done.wait(_RECONNECT_BACKOFF_S):
                        return
            finally:
                out_q.put(("pump_exit", None))

        pump = threading.Thread(
            target=_pump,
            name=f"ghost-cursor-supervisor-stream-{self.session_name}",
            daemon=True,
        )
        pump.start()

        last_status_check = time.monotonic()
        pump_exited = False
        try:
            while not self.settled.is_set():
                if self._stop_requested.is_set() and not self._cancel_sent:
                    self._cancel_sent = True
                    self._ingest_lifecycle("interrupt_requested")
                    try:
                        client.cancel_run(agent_id, run_id)
                    except RestApiError as exc:
                        if exc.code != "run_not_cancellable":
                            logger.warning("supervisor cancel failed: %s", exc)
                    except RestClientError as exc:
                        logger.warning("supervisor cancel failed: %s", exc)

                try:
                    kind, payload = out_q.get(timeout=_POLL_S)
                except queue.Empty:
                    kind, payload = "", None

                now = time.monotonic()
                if kind == "event":
                    self._ingest_sse(payload)
                elif kind in ("closed", "dropped", "pump_exit"):
                    # Stream boundary: consult the settle authority now.
                    pump_exited = pump_exited or kind == "pump_exit"
                    last_status_check = now
                    if self._check_remote(client, agent_id, run_id):
                        return

                # Poll watchdog (RFC §2): a silent/unreconnectable stream
                # degrades to GET polling; a requested stop polls tightly
                # so the cancel is observed promptly.
                poll_every = (
                    1.0 if self._stop_requested.is_set() else WATCHDOG_INTERVAL_S
                )
                if now - last_status_check >= poll_every:
                    last_status_check = now
                    if self._check_remote(client, agent_id, run_id):
                        return

                self._maybe_digest()
        finally:
            stream_done.set()

    def _check_remote(self, client: Any, agent_id: str, run_id: str) -> bool:
        """Poll the settle authority; settle and return True when the
        remote run is terminal (terminal precedence, RFC §2)."""
        try:
            remote = str(client.get_run(agent_id, run_id).get("status") or "")
        except RestApiError as exc:
            if exc.status_code == 404:
                self._settle(
                    "failed",
                    error="orphaned: the remote run no longer resolves",
                )
                return True
            logger.warning("supervisor get_run failed: %s", exc)
            return False
        except RestClientError as exc:
            logger.warning("supervisor get_run failed: %s", exc)
            return False
        _handles.record_supervision(
            self.session_name,
            watchdog={
                "last_poll_ts": round(time.time(), 3),
                "last_remote_status": remote,
                "last_event_id": self._last_event_id,
            },
        )
        status = _REMOTE_TERMINAL.get(remote.upper())
        if status is None:
            return False
        error = (
            {"error": "cursor run ended with status: error"}
            if status == "failed"
            else {}
        )
        if self._stop_requested.is_set() and status == "cancelled":
            self._ingest_lifecycle("interrupted")
        self._settle(status, **error)
        return True

    # -- ingest boundary (RFC §4) ---------------------------------------------

    def _ingest_sse(self, event: Any) -> None:
        """One SSE event through the ingest boundary: dedupe by provider
        event id BEFORE seq assignment, convert, stamp, append."""
        event_id = getattr(event, "id", None)
        if event_id:
            if event_id in self._seen_event_ids:
                return  # replayed twin — already ingested
            self._seen_event_ids.add(event_id)
            self._last_event_id = event_id
        # interaction_update twins are dropped inside _message_from_sse
        # (they duplicate the simplified events under the same id).
        message = _cloud._message_from_sse(event)
        if message is None:
            return
        for envelope in self._normalizer.normalize("cloud.message", message):
            self._ingest_envelope(envelope)

    @property
    def _normalizer(self) -> _events.SdkNormalizer:
        normalizer = getattr(self, "_normalizer_obj", None)
        if normalizer is None:
            normalizer = _events.SdkNormalizer()
            self._normalizer_obj = normalizer
        return normalizer

    def _ingest_envelope(self, envelope: Dict[str, Any]) -> None:
        _eventlog.append(self.session_name, stamp(envelope, self._attempt_id))
        kind = str(envelope.get("kind") or "")
        if kind != "lifecycle":
            self._nonlifecycle_events += 1
        if kind == "file_diff":
            path = str(envelope.get("path") or "")
            entry = self._files.setdefault(
                path, {"path": path, "added": 0, "removed": 0}
            )
            entry["added"] += int(envelope.get("added") or 0)
            entry["removed"] += int(envelope.get("removed") or 0)
            entry["status"] = envelope.get("status")
            self._prose_open = False
        elif kind == "content":
            delta = str(envelope.get("delta") or "")
            if delta:
                if self._prose_open and self._prose_blocks:
                    self._prose_blocks[-1] += delta
                else:
                    self._prose_blocks.append(delta)
                    self._prose_open = True
        elif kind in ("tool_use", "tool_result"):
            self._prose_open = False
            call_id = str(envelope.get("id") or "tool")
            if kind == "tool_use":
                tool = str(envelope.get("tool") or "tool")
                detail = str(
                    envelope.get("title") or envelope.get("command") or ""
                ).strip()
                prior = self._pending_tools.get(call_id)
                self._pending_tools[call_id] = {
                    "tool": tool,
                    "title": f"{tool} — {detail}" if detail else tool,
                    "since": (prior or {}).get("since") or time.time(),
                }
                if tool == _events.TOOL_PLAN and envelope.get("plan_items"):
                    self._plan_items = list(envelope.get("plan_items") or [])
            else:
                self._pending_tools.pop(call_id, None)
        if not self._durable_emitted and is_durable_evidence(envelope):
            self._durable_emitted = True
            self._ingest_lifecycle("durable_progress", evidence=kind)

    def _ingest_lifecycle(self, event: str, **payload: Any) -> None:
        _eventlog.append(
            self.session_name,
            stamp(_events.lifecycle(event, **payload), self._attempt_id),
        )

    # -- digests (derived views; RFC non-goals) ---------------------------------

    def _maybe_digest(self) -> None:
        entry = _handles.get(self.session_name)
        subscribers = _handles.subscribers_of(entry)
        now = time.monotonic()
        for sub_key, interval_s in subscribers.items():
            due = self._digest_due.get(sub_key)
            if due is None:
                # First digest one interval after re-attach.
                self._digest_due[sub_key] = now + float(interval_s)
                continue
            if now < due:
                continue
            self._digest_due[sub_key] = now + float(interval_s)
            self._deliver_digest(entry or {}, sub_key, float(interval_s))

    def _deliver_digest(
        self, entry: Dict[str, Any], sub_key: str, interval_s: float
    ) -> None:
        name = self.session_name
        sup = _handles.supervision_of(entry)
        cursor = sup["last_seq_delivered"].get(sub_key, 0)
        stats = _eventlog.stats(name) or {}
        total = int(stats.get("total_events") or 0)
        new_count = max(total - cursor, 0)
        events: List[Dict[str, Any]] = []
        if new_count:
            page = _eventlog.read_events(
                name, offset=-1, limit=_render.DIGEST_MAX_EVENTS
            )
            events = [
                e for e in ((page or {}).get("events") or [])
                if int(e.get("seq") or 0) >= cursor
            ]

        n = self._digest_n.get(sub_key, 0) + 1
        now_wall = time.time()
        pending = [
            {
                "call_id": cid,
                "tool": str(p.get("tool") or ""),
                "title": str(p.get("title") or ""),
                "pending_s": (
                    round(now_wall - p["since"], 1)
                    if p.get("since") is not None
                    else None
                ),
            }
            for cid, p in self._pending_tools.items()
        ]
        pending.sort(key=lambda p: -(p["pending_s"] or 0))
        text = _render.digest_text(
            name=name,
            n=n,
            status="running",
            elapsed_s=None,
            last_activity_s=None,
            files=sorted(self._files.values(), key=lambda f: f["path"]),
            pending_tools=pending,
            plan=list(self._plan_items),
            events=events,
            new_count=new_count,
            next_update_s=interval_s,
        )
        evt = {
            "type": "async_delegation",
            # Unique per digest, per subscriber, AND per attempt — the
            # attempt id keeps post-restart digests distinct from the
            # pre-restart ticker's numbering under the TUI's
            # (delegation_id, type) dedup.
            "delegation_id": (
                f"{name}#progress-{n}@{_subscriber_suffix(sub_key)}"
                f"@{self._attempt_id}"
            ),
            "session_key": sub_key,
            "goal": (
                f"cursor progress update {n} for session '{name}' "
                "(run still active — supervision re-attached after a "
                "process restart; NOT the final result)"
            ),
            "context": None,
            "toolsets": None,
            "role": "cursor",
            "model": str(entry.get("model") or "cursor"),
            "status": "running",
            "summary": text,
            "error": None,
            "api_calls": 0,
            "duration_seconds": 0.0,
            "dispatched_at": None,
            "completed_at": time.time(),
            "cursor_progress_update": n,
            "cursor_session_id": str(entry.get("cursor_session_id") or ""),
        }
        if _enqueue_completion_event(evt):
            self._digest_n[sub_key] = n
            # Advance-only, and only after the successful enqueue.
            _handles.advance_delivery_cursor(name, sub_key, total)

    # -- settlement (single-writer, RFC §3) --------------------------------------

    def _settle(self, status: str, error: str = "") -> None:
        name = self.session_name
        if not _handles.transition_supervision(name, status):
            # Another writer settled first (or the phase was never live):
            # nothing to deliver — the settle gate is what makes
            # completion delivery exactly-once.
            self.settled.set()
            return

        elapsed = time.monotonic() - self._attached_monotonic
        shape = (
            {"death_shape": death_shape(elapsed, self._nonlifecycle_events)}
            if status != "completed"
            else {}
        )
        self._ingest_lifecycle(
            "session.settled", status=status, reattached=True, **shape
        )
        files = sorted(self._files.values(), key=lambda f: f["path"])
        _handles.record(
            name,
            status=status,
            files_changed_count=len(files) or None,
            **({"status_note": error} if error else {}),
        )
        self._deliver_completion(status, error, files)
        self.settled.set()
        logger.info(
            "ghost_cursor supervisor settled session %s: %s", name, status
        )

    def _deliver_completion(
        self, status: str, error: str, files: List[Dict[str, Any]]
    ) -> None:
        """Completion fan-out — one copy per subscriber plus the
        dispatching session, exactly once (guarded by the settle gate)."""
        name = self.session_name
        entry = _handles.get(name) or {}
        subscribers = _handles.subscribers_of(entry)
        dispatcher = str(entry.get("session_key") or "")
        recipients = sorted({str(k or "") for k in subscribers} | {dispatcher})

        prose = (
            self._prose_blocks[-1]
            if self._prose_open and self._prose_blocks
            else "\n\n".join(b for b in self._prose_blocks if b.strip())
        )
        stats = _eventlog.stats(name) or {}
        summary = _render.completion_text(
            name=name,
            status=status,
            elapsed_s=None,
            repo=str(entry.get("repo") or ""),
            summary=prose,
            files=files,
            error=error,
            total_events=stats.get("total_events", 0),
            last_prompt_seq=_handles.last_prompt_seq(entry),
        )
        result = {
            "success": status == "completed",
            "status": "completed" if status == "completed" else status,
            "repo": str(entry.get("repo") or ""),
            "summary": prose,
            "files_changed": files,
            "files_changed_count": len(files),
            "session": name,
            "session_id": str(entry.get("cursor_session_id") or ""),
            "resumed": True,
            **({"error": error} if error else {}),
        }
        base_evt = {
            "type": "async_delegation",
            "goal": f"cursor: {str(entry.get('task') or name)[:200]}",
            "context": None,
            "toolsets": None,
            "role": "cursor",
            "model": str(entry.get("model") or "cursor"),
            "status": status,
            "summary": (
                f"{summary}\n\nfollow up in this session: "
                f"cursor_send_message('{name}', ...)"
            ),
            "error": error or None,
            "api_calls": 0,
            "duration_seconds": 0.0,
            "dispatched_at": None,
            "completed_at": time.time(),
            "result": result,
            "cursor_session_id": str(entry.get("cursor_session_id") or ""),
        }
        for session_key in recipients:
            _enqueue_completion_event({
                **base_evt,
                "session_key": session_key,
                "delegation_id": (
                    name
                    if session_key == dispatcher
                    else f"{name}@{_subscriber_suffix(session_key)}"
                ),
            })


# ---------------------------------------------------------------------------
# Registry of live supervisor tasks + reconciler (RFC §1)
# ---------------------------------------------------------------------------

_lock = threading.Lock()
_supervisors: Dict[str, SessionSupervisor] = {}
_reconciler_timer: Optional[threading.Timer] = None
_reconciler_started = False


def _forget(session_name: str, sup: SessionSupervisor) -> None:
    with _lock:
        if _supervisors.get(session_name) is sup:
            _supervisors.pop(session_name, None)


def get_live(session_name: str) -> Optional[SessionSupervisor]:
    """The live re-attached supervisor for a session, or None."""
    with _lock:
        sup = _supervisors.get(str(session_name or ""))
    return sup if sup is not None and sup.is_alive() else None


def has_live(session_name: str) -> bool:
    return get_live(session_name) is not None


def request_stop(session_name: str) -> bool:
    """Ask a live re-attached supervisor to cancel its run (tool calls
    request transitions; the supervisor applies them — RFC §3)."""
    sup = get_live(session_name)
    if sup is None:
        return False
    sup.request_stop()
    return True


def stop_and_wait(session_name: str, wait_s: float) -> bool:
    """Request a stop and wait for the supervisor to settle the session.

    True when the session reached a terminal phase within the wait —
    the caller may then re-prompt safely.
    """
    sup = get_live(session_name)
    if sup is None:
        return True  # nothing live — already settled
    sup.request_stop()
    return sup.settled.wait(timeout=max(float(wait_s), 0.0))


def _job_is_live(session_name: str) -> bool:
    """Whether an in-process job currently supervises this session (its
    worker thread IS the supervision executor for in-process runs)."""
    from . import jobs as _jobs  # lazy: jobs imports this module

    job = _jobs.registry.get_by_name(session_name)
    return job is not None and job.status == "running"


def ensure_supervisor(session_name: str) -> Optional[SessionSupervisor]:
    """The live supervisor for a session, spawning one if needed.

    No-op (returns None) for handles that are not in a live supervision
    phase, or that a running in-process job already supervises.
    """
    name = str(session_name or "").strip()
    if not name:
        return None
    if not _handles.supervision_is_live(_handles.get(name)):
        return None
    if _job_is_live(name):
        return None
    with _lock:
        existing = _supervisors.get(name)
        if existing is not None and existing.is_alive():
            return existing
        sup = SessionSupervisor(name)
        _supervisors[name] = sup
    sup.start()
    return sup


def _adopt_legacy_handle(name: str, entry: Dict[str, Any]) -> bool:
    """Adopt a pre-supervisor handle into supervision. True when adopted.

    A handle whose top-level status is ``running`` but whose supervision
    record is missing/null/empty-phase predates the supervisor deploy —
    EXACTLY the handle the reconciler exists to re-attach; skipping it
    (the incident: two healthy pre-supervisor cloud runs left orphaned
    after a gateway restart) defeats the point. Seed a live supervision
    record — fresh attempt identity, delivery cursors left empty, no
    persisted Last-Event-ID (the adopted stream attaches from the live
    tail) — and let the normal re-attach path own it: the supervisor's
    GET authority streams a RUNNING run and settles a terminal one
    exactly once with its real terminal status.
    """
    if str(entry.get("status") or "") != "running":
        return False
    if _handles.supervision_of(entry)["phase"]:
        return False  # already supervised (live handled by the caller)
    if _job_is_live(name):
        return False  # the in-process job IS this session's supervision
    _handles.record_supervision(
        name,
        phase=PHASE_STREAMING,
        current_attempt_id=mint_attempt_id(),
        attempt_n=1,
    )
    _eventlog.append(
        name,
        _events.lifecycle(
            "supervision.adopted",
            note=(
                "pre-supervisor handle adopted by the reconciler — "
                "supervision record seeded, re-attaching"
            ),
        ),
    )
    logger.info("ghost_cursor reconciler adopted legacy handle %s", name)
    return True


def _repair_false_settle(
    name: str, entry: Dict[str, Any], client_factory: Any
) -> bool:
    """Un-settle a locally-terminal handle whose remote run is RUNNING.

    Remote authority wins in both directions: a handle falsely settled
    (e.g. a send-time preflight failure marked a healthy run failed) is
    flipped back to running/streaming and re-attached. Only handles
    settled within :data:`FALSE_SETTLE_REPAIR_WINDOW_S` are probed so a
    reconciler pass never GETs the whole terminal backlog. True when the
    handle was un-settled. Never raises.
    """
    was = str(entry.get("status") or "")
    if was not in _handles.SUPERVISION_TERMINAL_PHASES:
        return False
    if time.time() - float(entry.get("updated_at") or 0.0) > (
        FALSE_SETTLE_REPAIR_WINDOW_S
    ):
        return False
    agent_id = str(entry.get("cursor_session_id") or "")
    if not agent_id or _job_is_live(name):
        return False
    try:
        client = client_factory()
    except _cloud.CloudRunnerError as exc:
        # No API key / client preflight — nothing to probe with this pass.
        logger.debug(
            "ghost_cursor false-settle probe skipped (no client): %s", exc
        )
        return False
    try:
        run_id = str(entry.get("latest_run_id") or "")
        if not run_id:
            runs = client.list_runs(agent_id).get("runs")
            run_id = (
                str((runs[0] or {}).get("id") or "")
                if isinstance(runs, list) and runs
                else ""
            )
        if not run_id:
            return False
        remote = str(client.get_run(agent_id, run_id).get("status") or "")
    except Exception:
        logger.debug(
            "ghost_cursor false-settle probe failed for %s", name,
            exc_info=True,
        )
        return False
    if remote.upper() != "RUNNING":
        return False
    _handles.record(name, status="running", status_note="")
    _handles.record_supervision(
        name,
        phase=PHASE_STREAMING,
        current_attempt_id=mint_attempt_id(),
        attempt_n=max(_handles.supervision_of(entry)["attempt_n"], 1),
    )
    _eventlog.append(
        name,
        _events.lifecycle(
            "session.unsettled",
            was=was,
            remote_status=remote,
            note=(
                f"local terminal status '{was}' contradicted a RUNNING "
                "remote run — remote authority wins; un-settled and "
                "re-attaching"
            ),
        ),
    )
    logger.warning(
        "ghost_cursor reconciler un-settled session %s (local %r, remote "
        "RUNNING) and is re-attaching", name, was,
    )
    return True


def _parse_iso_ts(value: Any) -> Optional[float]:
    """Epoch seconds for an ISO-8601 string ('Z' suffix included), or
    None when unparseable."""
    import datetime as _dt

    try:
        raw = str(value or "").strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        return _dt.datetime.fromisoformat(raw).timestamp()
    except Exception:
        return None


def _recover_create_intent(
    name: str, entry: Dict[str, Any], client_factory: Any
) -> bool:
    """Resolve a crashed create: the durable intent said a non-idempotent
    ``POST /v1/agents`` was about to leave the box, and no identity was
    ever recorded.

    Bounded recovery (the API has no idempotency key and no name filter):
    list the newest agents and adopt the NEWEST one whose name equals the
    session title and whose createdAt falls inside
    ``[intent_ts - CREATE_MATCH_SKEW_S, now]``. Ambiguity (several
    matches) is adopted-newest and REPORTED — never auto-cancelled. A
    truncated listing with no match resolves to "unresolved" (check
    cursor.com/agents), never a false "failed"; an exhaustive listing
    with no match settles the handle failed. Returns True when an agent
    was adopted (the caller then attaches a supervisor). Never raises.
    """
    intent = entry.get("create_intent")
    if not isinstance(intent, dict) or str(intent.get("state") or "") != "creating":
        return False
    if str(entry.get("cursor_session_id") or ""):
        # Identity landed after all (e.g. legacy write path) — settle the
        # intent; nothing to recover.
        _handles.record(name, create_intent={**intent, "state": "recorded"})
        return False
    if _job_is_live(name):
        return False  # the dispatching process is still mid-create
    try:
        intent_ts = float(intent.get("ts") or 0.0)
    except (TypeError, ValueError):
        intent_ts = 0.0
    if time.time() - intent_ts < CREATE_INTENT_MIN_AGE_S:
        return False  # a live create POST can legitimately take >30s
    try:
        client = client_factory()
    except _cloud.CloudRunnerError as exc:
        logger.debug("create-intent recovery skipped (no client): %s", exc)
        return False
    try:
        response = client.list_agents(limit=100)
    except RestClientError as exc:
        logger.warning("create-intent recovery list_agents failed: %s", exc)
        return False
    items = response.get("items")
    items = items if isinstance(items, list) else []

    def _created_at(agent: Dict[str, Any]) -> float:
        return _parse_iso_ts((agent or {}).get("createdAt")) or 0.0

    matches = [
        agent for agent in items
        if isinstance(agent, dict)
        and str(agent.get("name") or "") == name
        and _created_at(agent) >= intent_ts - CREATE_MATCH_SKEW_S
    ]
    if matches:
        newest = max(matches, key=_created_at)
        agent_id = str(newest.get("id") or "")
        others = sorted(
            str(a.get("id") or "") for a in matches if a is not newest
        )
        note = (
            f"ambiguous create recovery: adopted {agent_id}; other agents "
            f"titled {name!r} in the window: {', '.join(others)} — review "
            "at cursor.com/agents (nothing was auto-cancelled)"
            if others else ""
        )
        _handles.record(
            name,
            cursor_session_id=agent_id,
            latest_run_id=str(newest.get("latestRunId") or "") or None,
            status="running",
            status_note=note,
            create_intent={**intent, "state": "recovered"},
        )
        _handles.record_supervision(
            name,
            phase=PHASE_STREAMING,
            current_attempt_id=mint_attempt_id(),
            attempt_n=1,
        )
        _eventlog.append(name, _events.lifecycle(
            "create.recovered",
            agent_id=agent_id,
            ambiguous_with=others or None,
            note=(
                "a crashed create was recovered by newest-title/time "
                "matching; re-attaching supervision"
            ),
        ))
        logger.warning(
            "ghost_cursor recovered crashed create for %s -> %s%s",
            name, agent_id, f" (ambiguous: {others})" if others else "",
        )
        return True

    truncated = bool(response.get("nextCursor")) or len(items) >= 100
    if truncated:
        _handles.record(
            name,
            create_intent={**intent, "state": "unresolved"},
            status_note=(
                "create outcome UNRESOLVED: the agent listing is truncated "
                "and no matching agent was visible — check "
                "https://cursor.com/agents for an agent titled "
                f"{name!r} before re-sending"
            ),
        )
        _eventlog.append(name, _events.lifecycle(
            "create.unresolved", note="agent listing truncated; not settled",
        ))
        return False

    _handles.record(
        name,
        status="failed",
        create_intent={**intent, "state": "failed"},
        status_note=(
            "the create never landed remotely (no agent with this title "
            "exists) — the dispatching process died before the POST; "
            "re-send to retry"
        ),
    )
    _eventlog.append(name, _events.lifecycle(
        "create.failed", note="no matching remote agent; settled failed",
    ))
    return False


def reconcile_once() -> List[str]:
    """One reconciler pass: spawn a supervisor for every handle in a
    non-terminal supervision phase with no live supervision in this
    process. Also ADOPTS pre-supervisor handles (top-level status running,
    supervision record missing/empty) and UN-SETTLES falsely-settled ones
    (local terminal, remote GET RUNNING). Returns the session names
    attached this pass. Never raises."""
    attached: List[str] = []
    try:
        # Worker controller housekeeping rides the same tick: retire dead
        # generations, reap idle LEASELESS workers (a leased worker is
        # never touched). Never raises.
        _workers.reconcile()
    except Exception:
        logger.exception("ghost_cursor worker reconcile pass failed")
    try:
        # One client per pass, built lazily (the repair probe needs the
        # GET authority; no probe-worthy handle → no client, no preflight).
        probe_client: Any = None

        def _probe_client() -> Any:
            nonlocal probe_client
            if probe_client is None:
                probe_client = _cloud.make_client()
            return probe_client

        for entry in _handles.entries(scope="all", limit=_handles.MAX_ENTRIES):
            name = str(entry.get("session") or "")
            if not name:
                continue
            if not _handles.supervision_is_live(entry):
                if not (
                    _recover_create_intent(name, entry, _probe_client)
                    or _adopt_legacy_handle(name, entry)
                    or _repair_false_settle(name, entry, _probe_client)
                ):
                    continue
            with _lock:
                existing = _supervisors.get(name)
                if existing is not None and existing.is_alive():
                    continue
            if ensure_supervisor(name) is not None:
                attached.append(name)
    except Exception:
        logger.exception("ghost_cursor reconciler pass failed")
    return attached


def _reconcile_tick() -> None:
    try:
        reconcile_once()
    finally:
        _arm_reconciler()


def _arm_reconciler() -> None:
    global _reconciler_timer
    with _lock:
        if not _reconciler_started:
            return
        timer = threading.Timer(RECONCILE_INTERVAL_S, _reconcile_tick)
        timer.daemon = True
        timer.name = "ghost-cursor-reconciler"
        _reconciler_timer = timer
    timer.start()


def start_reconciler() -> None:
    """Start the periodic reconciler (idempotent; called at plugin init).

    Runs one pass immediately — re-attaching supervisors to any handle
    left in a live phase by a previous process — then every
    :data:`RECONCILE_INTERVAL_S`.
    """
    global _reconciler_started
    with _lock:
        if _reconciler_started:
            return
        _reconciler_started = True
    reconcile_once()
    _arm_reconciler()


def _reset_for_tests() -> None:
    """Stop the reconciler and every supervisor (test isolation only)."""
    global _reconciler_timer, _reconciler_started
    with _lock:
        timer = _reconciler_timer
        _reconciler_timer = None
        _reconciler_started = False
        sups = list(_supervisors.values())
        _supervisors.clear()
    if timer is not None:
        timer.cancel()
    for sup in sups:
        sup.request_stop()
        sup.settled.set()
