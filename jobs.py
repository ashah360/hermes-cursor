"""Job table for cursor runs — one entry per dispatched run, keyed by handle.

v0.4 named-session model
------------------------
Every cursor run executes as a background :class:`CursorJob` on a worker
thread. The SINGLE public handle for a run is the session NAME minted by
``cursor_create_session`` (``job.session_name``); the cursor ``session_id``
(the cloud agent id, ``bc-...``) rides along as an alias. ``cursor_send_message``
dispatches into it, and ``cursor_status`` / ``cursor_stop`` /
``cursor_events`` take it back. This registry is the in-process job table
behind that handle — rolling progress buffer, status, files-changed
aggregation, and deliver-on-complete. A tiny JSON persistence layer
(``handles.py``) mirrors handle → status/repo across process restarts;
the live job state itself is process-local (like async delegation).

There is deliberately NO auto-resume heuristic here (the v0.2
``session_registry`` repo+timestamp guesswork is gone). Lookup is by
explicit handle only: :meth:`CursorJobRegistry.get_by_session`.

Completion delivery (reuses core infra — nothing reinvented)
------------------------------------------------------------
On EVERY terminal state (completed / failed / cancelled / timeout — never
silent), a job whose delivery flag is still set pushes an event onto the
shared ``tools.process_registry.process_registry.completion_queue`` with
``type="async_delegation"`` — the exact rail ``delegate_task(background=true)``
uses (see ``tools/async_delegation.py``). The CLI ``process_loop`` drain and
the gateway's ``_async_delegation_watcher`` already consume that queue while
the agent is idle and inject each event as a fresh turn, which keeps strict
message-role alternation legal and the prompt cache intact. Each event's
``session_key`` routes it to one gateway session (empty key = CLI); the
completion FANS OUT — one copy per subscriber in the handle entry's
``subscribers`` map, plus the dispatching session (``job.session_key``,
captured on the dispatching thread) even when it unsubscribed — see
``_push_completion_event``.

Delivery is ARMED, not assumed: a job is dispatched with ``deliver=False``
and the dispatching tool arms it (:meth:`CursorJob.arm_delivery`) only
after it has returned the running-handle shape to the caller. A run that
reaches a terminal state BEFORE that (handshake failure, ultra-fast
completion) is reported in-turn by the dispatching tool itself — arming
races finalize under the job lock, so the outcome lands exactly once:
either in the tool result or as a delivered message, never both, never
neither. Symmetrically, when ``cursor_stop`` / ``cursor_send`` settle a
run in-turn they disarm delivery first (:meth:`CursorJob.mark_handled`).

The completion payload carries the FULL final result dict (success / status /
summary / files_changed / session_id / resumed) both as a structured
``result`` field and rendered into the human-readable ``summary`` block, so
continuation (pass ``session_id`` to ``cursor_send``) survives the async
boundary.

Concurrency guard (same repo)
-----------------------------
Two agents editing one working tree corrupts it. ``dispatch()`` atomically
rejects a new job when an active job already holds the same resolved repo,
returning the existing job so the caller can surface its handle. Parallel
runs on DIFFERENT repos are fine — different handles.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from . import eventlog as _eventlog
from . import handles as _handles
from . import progress as _progress
from . import render as _render
from . import supervisor as _supervisor

logger = logging.getLogger(__name__)

# Rolling progress buffer cap — mirrors process_registry.MAX_OUTPUT_CHARS.
MAX_PROGRESS_CHARS = 200_000
# How many finished jobs to retain for cursor_status queries.
MAX_FINISHED_JOBS = 20
# Envelopes produced before the cursor agent (the spill-log key) exists are
# held here, then flushed the moment the handle arrives. Bounded so a run
# that never gets a session can't grow it forever.
MAX_PENDING_SPILL = 1_000

# Per-envelope trims for the rolling buffer (full-fidelity diffs live in the
# job's ``files`` aggregation, same caps as the final result).
_BUFFER_DIFF_CHARS = 2_000
_BUFFER_OUTPUT_CHARS = 1_000
# Trims for the completion payload / status snapshots.
_PAYLOAD_DIFF_CHARS = 2_000
_PAYLOAD_SUMMARY_CHARS = 4_000

TERMINAL_STATUSES = ("completed", "failed", "cancelled", "timeout")


def _clip(text: Any, limit: int) -> str:
    s = str(text or "")
    if len(s) <= limit:
        return s
    return s[:limit] + f"… [truncated {len(s) - limit} chars]"


def trim_result(result: Dict[str, Any]) -> Dict[str, Any]:
    """A copy of a final result dict safe to embed in a delivery payload.

    Per-file diffs and the prose summary are clipped; everything else —
    notably ``session_id`` / ``resumed`` / ``files_changed`` structure —
    passes through untouched so continuation keeps working.
    """
    out = dict(result)
    if isinstance(out.get("summary"), str):
        out["summary"] = _clip(out["summary"], _PAYLOAD_SUMMARY_CHARS)
    files = out.get("files_changed")
    if isinstance(files, list):
        trimmed = []
        for f in files:
            if isinstance(f, dict) and isinstance(f.get("diff"), str):
                f = {**f, "diff": _clip(f["diff"], _PAYLOAD_DIFF_CHARS)}
            trimmed.append(f)
        out["files_changed"] = trimmed
    return out


@dataclass
class CursorJob:
    """One dispatched cursor run (always a background worker thread)."""

    job_id: str  # internal dispatch id; the PUBLIC handle is session_name
    task: str
    repo: str
    # Watchdog knobs (see cloud_runner): abort after this much SILENCE (no
    # stream events) — activity resets the clock — plus an optional ceiling on
    # total run time (0 = disabled).
    inactivity_timeout_s: float
    max_wall_s: float = 0.0
    # Where the agent executes: "local" (machine-routed worker on this box)
    # or "cloud" (cursor-hosted VM). Set at dispatch from the session entry.
    runtime: str = "local"
    # THE public handle: the meaningful session title registered by
    # cursor_create_session (e.g. "Fix payment webhook retries"). The
    # cursor agent id (cursor_session_id below) stays a resolvable alias.
    # Empty only for direct registry use in tests.
    session_name: str = ""
    session_key: str = ""
    requested_session_id: Optional[str] = None
    requested_model: Optional[str] = None
    deliver: bool = False             # armed by the dispatching tool (see module doc)
    status: str = "running"
    created_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None
    cursor_session_id: str = ""       # the agent-id alias, set at cloud.session
    resumed: bool = False
    model: str = ""                   # actual model reported by cloud.session
    worker: str = ""                  # the "My Machines" worker (runtime=local)
    agents_ui_url: str = ""           # cursor.com/agents url for the agent
    # Wall-clock time of the last stream event received for this run (None
    # until the first event). Advisory only — feeds the last_activity_s
    # field of status snapshots so callers can flag silent runs; the
    # actual inactivity watchdog lives in cloud_runner.
    last_event_at: Optional[float] = None
    # --- aggregation state (guarded by _lock) ---
    files: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    # Streamed assistant prose as CONTIGUOUS blocks: each inner list is one
    # uninterrupted run of content deltas; tool calls / reasoning between
    # content start a new block. Lets the summary prefer the final wrap-up
    # message instead of raw-concatenating every interstitial narration
    # fragment (see summary_text).
    assistant_segments: List[List[str]] = field(default_factory=list)
    # True while the newest segment is still receiving deltas — i.e. the
    # last folded envelope was content, nothing interrupted it since.
    segment_open: bool = False
    reasoning_tail: str = ""
    # In-flight tool calls keyed by call id — set on tool_use, removed by
    # the matching tool_result — feeding the progress-digest header so a
    # long quiet call reads as busy, not stalled. A dict (not a single
    # string): cursor runs calls CONCURRENTLY (two parallel `task`
    # sub-agents captured live 2026-07-09), and a scalar field made the
    # second call overwrite the first, then the first result blank the
    # header while the other call was still running. Values:
    # {"tool", "title", "since"}.
    pending_tools: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    # Latest cursor-authored plan snapshot (todo_write): list of
    # {"content", "status"} — the digest's "plan: 2/5 done" line.
    plan_items: List[Dict[str, str]] = field(default_factory=list)
    progress_buffer: str = ""
    progress_events: int = 0
    # --- auto-retry progress evidence (guarded by _lock; issue #17) ---
    # Durable evidence that this prompt did real work, accumulated across
    # auto-retry attempts (a job IS one prompt) and read by the
    # zero-progress gate (__init__._made_progress). ``files`` above is the
    # third signal (any file_diff folded). The tool ids are a set keyed by
    # call id, so a re-observed stream can never double-count a completed
    # call; the plain event count only ever grows — nothing resets either
    # mid-job, so streamed progress is never forgotten by a later attempt.
    nonlifecycle_events: int = 0
    completed_tool_ids: Set[str] = field(default_factory=set)
    # --- supervision identity (RFC §1/§4) ---
    # The stable id of the CURRENT attempt, stamped on every event this
    # run spills as ``attemptId`` (mandatory — retry forensics). Minted by
    # supervisor.begin_attempt at dispatch and re-minted per auto-retry.
    current_attempt_id: str = ""
    attempt_n: int = 1
    # Attempt ids whose lifecycle.durable_progress event was already
    # emitted (derived once per attempt — see __init__._fold_envelope).
    durable_marked_attempts: Set[str] = field(default_factory=set)
    run_error: Optional[str] = None
    # Typed detail riding a terminal-error run.failed (see cloud_runner's
    # cloud.error payload): None = unknown, not "no".
    error_retryable: Optional[bool] = None
    error_retry_after: Optional[str] = None
    timed_out: bool = False
    completed: bool = False
    cancelled: bool = False
    result: Optional[Dict[str, Any]] = None
    # --- control ---
    _pending_spill: List[Dict[str, Any]] = field(default_factory=list, repr=False)
    cancel_event: threading.Event = field(default_factory=threading.Event, repr=False)
    done_event: threading.Event = field(default_factory=threading.Event, repr=False)
    # Set the instant the cursor agent is established (handle available).
    session_event: threading.Event = field(default_factory=threading.Event, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    _thread: Optional[threading.Thread] = field(default=None, repr=False)

    # -- control -------------------------------------------------------------

    def request_cancel(self) -> None:
        """Ask the run to stop via the REST cancel path."""
        self.cancel_event.set()

    def arm_delivery(self) -> bool:
        """Turn on completion delivery for this run.

        Called by the dispatching tool AFTER it has handed the running
        handle back to the caller — from then on the outcome must arrive as
        a delivered message. Returns False when the job already reached a
        terminal state (finalize won the race with ``deliver`` still False,
        so nothing was enqueued and the caller must report the final result
        in-turn instead). Atomic with respect to finalize via the job lock.
        """
        with self._lock:
            if self.status in TERMINAL_STATUSES:
                return False
            self.deliver = True
            return True

    def mark_handled(self) -> bool:
        """Suppress completion delivery — the caller reports the outcome in-turn.

        Used by ``cursor_stop`` / ``cursor_send`` before they cancel: the
        terminal state reaches the conversation through the tool result, so
        the async delivery would be a duplicate message. Returns False when
        the job already reached a terminal state (finalize won the race —
        delivery may already have fired; the caller just reads the result).
        """
        with self._lock:
            if self.status in TERMINAL_STATUSES:
                return False
            self.deliver = False
            return True

    # -- progress buffer -------------------------------------------------------

    def append_progress(self, envelope: Dict[str, Any]) -> None:
        """Append one canonical envelope to the rolling progress buffer.

        Mirrors the process-registry rolling-output pattern: bounded size,
        trimmed from the front at a line boundary. Full before/after file
        content is dropped (the diff carries the signal); diffs and shell
        output are clipped so one giant edit can't evict all history.

        The UNCLIPPED envelope is also spilled to the per-session JSONL
        event log (``eventlog.py``) so everything the compact buffer evicts
        or trims stays recoverable and pageable via ``cursor_status``.
        Envelopes that arrive before the session handle exists are held in
        a bounded pending list and flushed the moment it does.

        Ingest boundary (RFC §4): every envelope is stamped with the
        CURRENT attempt's ``attemptId`` before it is buffered or spilled.
        """
        if self.current_attempt_id and not envelope.get("attemptId"):
            envelope = {**envelope, "attemptId": self.current_attempt_id}
        compact = {k: v for k, v in envelope.items() if k not in ("before", "after")}
        if isinstance(compact.get("diff"), str):
            compact["diff"] = _clip(compact["diff"], _BUFFER_DIFF_CHARS)
        if isinstance(compact.get("output"), str):
            compact["output"] = _clip(compact["output"], _BUFFER_OUTPUT_CHARS)
        try:
            line = json.dumps(compact, ensure_ascii=False, default=str)
        except Exception:
            line = str(compact)
        with self._lock:
            self.progress_buffer += line + "\n"
            if len(self.progress_buffer) > MAX_PROGRESS_CHARS:
                cut = self.progress_buffer[-MAX_PROGRESS_CHARS:]
                nl = cut.find("\n")
                self.progress_buffer = cut[nl + 1:] if nl >= 0 else cut
            self.progress_events += 1
            # Spill key: the session NAME (stable across runs and agent
            # ids, so one named session = one log). Fallbacks keep
            # name-less jobs (direct registry use) spilling by agent id.
            spill_sid = (
                self.session_name
                or self.cursor_session_id
                or self.requested_session_id
                or ""
            )
            if spill_sid:
                to_spill = self._pending_spill + [envelope]
                self._pending_spill = []
            else:
                if len(self._pending_spill) < MAX_PENDING_SPILL:
                    self._pending_spill.append(dict(envelope))
                to_spill = []
        # File I/O outside the job lock (eventlog serializes internally);
        # only the worker thread appends progress, so order is preserved.
        for env in to_spill:
            _eventlog.append(spill_sid, env)

    # -- summary derivation -------------------------------------------------------

    def _pending_snapshot(self, now: float) -> List[Dict[str, Any]]:
        """Read-only copies of the in-flight calls, oldest first, with a
        computed per-call elapsed. Caller holds the lock."""
        out = []
        for call_id, entry in self.pending_tools.items():
            since = entry.get("since")
            out.append(
                {
                    "call_id": call_id,
                    "tool": str(entry.get("tool") or ""),
                    "title": str(entry.get("title") or ""),
                    "pending_s": (
                        round((self.finished_at or now) - since, 1)
                        if since is not None
                        else None
                    ),
                }
            )
        out.sort(key=lambda p: -(p["pending_s"] or 0))
        return out

    def summary_text(self) -> str:
        """The prose summary for status peeks and the final result.

        Prefers the FINAL contiguous content block of the turn — the actual
        wrap-up message the agent streamed last, with its deltas joined raw
        (they are fragments of one message). When the turn did not end on a
        content block (killed mid-tool-call, cancelled), there is no final
        message to prefer, so it falls back to every block joined with
        blank lines — never raw-concatenated, so interstitial narration
        sentences don't fuse ("...spike file.Now let me explore...").
        """
        with self._lock:
            blocks = [
                b
                for b in ("".join(seg).strip() for seg in self.assistant_segments)
                if b
            ]
            ended_on_content = self.segment_open
        if not blocks:
            return ""
        if ended_on_content:
            return blocks[-1]
        return "\n\n".join(blocks)

    # -- read-only view ---------------------------------------------------------

    def snapshot(self) -> Dict[str, Any]:
        """A read-only status snapshot (what ``cursor_status`` returns).

        STRICTLY read-only: takes the lock only to copy state. Never touches
        the cancel event, the cursor run, or the worker thread.
        """
        now = time.time()
        with self._lock:
            files = []
            for f in sorted(self.files.values(), key=lambda x: x.get("path", "")):
                entry = dict(f)
                if isinstance(entry.get("diff"), str):
                    entry["diff"] = _clip(entry["diff"], _PAYLOAD_DIFF_CHARS)
                files.append(entry)
            snap: Dict[str, Any] = {
                "session": self.session_name,
                "session_id": self.cursor_session_id,
                "status": self.status,
                "repo": self.repo,
                "task": _clip(self.task, 400),
                "elapsed_s": round((self.finished_at or now) - self.created_at, 1),
                # Seconds since the last stream event (advisory: lets callers
                # flag a silent run without touching it). Frozen at
                # finished_at for terminal runs; falls back to created_at
                # before the first event arrives.
                "last_activity_s": round(
                    (self.finished_at or now) - (self.last_event_at or self.created_at), 1
                ),
                "cursor_session_id": self.cursor_session_id,
                "resumed": self.resumed,
                "model": self.model or (self.requested_model or ""),
                "runtime": self.runtime,
                "worker": self.worker,
                "agents_ui_url": self.agents_ui_url,
                "summary_so_far": _clip(self.summary_text(), _PAYLOAD_SUMMARY_CHARS),
                "files_changed_so_far": files,
                "files_changed_count": len(files),
                "latest_reasoning": self.reasoning_tail[-1500:],
                "pending_tools": self._pending_snapshot(now),
                "plan": [dict(i) for i in self.plan_items],
                "progress_tail": self.progress_buffer[-4000:],
                "progress_events": self.progress_events,
            }
            # Back-compat scalar view: the OLDEST pending call (the one
            # waited on longest). Consumers should prefer pending_tools.
            oldest = min(
                self.pending_tools.values(),
                key=lambda p: p.get("since") or now,
                default=None,
            )
            snap["pending_tool"] = str(oldest.get("title") or "") if oldest else ""
            snap["pending_tool_s"] = (
                round((self.finished_at or now) - oldest["since"], 1)
                if oldest and oldest.get("since") is not None
                else None
            )
            if self.result is not None:
                snap["result"] = trim_result(self.result)
        return snap


class CursorJobRegistry:
    """Process-global tracker of cursor jobs (thread-safe)."""

    def __init__(self) -> None:
        self._jobs: Dict[str, CursorJob] = {}
        self._lock = threading.Lock()

    # -- dispatch ---------------------------------------------------------------

    def dispatch(
        self,
        *,
        runner: Callable[[CursorJob], Dict[str, Any]],
        task: str,
        repo: str,
        inactivity_timeout_s: float,
        max_wall_s: float = 0.0,
        session_name: str = "",
        session_key: str = "",
        requested_session_id: Optional[str] = None,
        requested_model: Optional[str] = None,
        runtime: str = "local",
    ) -> Tuple[Optional[CursorJob], Optional[CursorJob]]:
        """Start a cursor run on a worker thread.

        Returns ``(job, None)`` on success or ``(None, existing_job)`` when
        an active job already holds ``repo`` (same-repo concurrency guard —
        two cursor agents on one working tree corrupt it). The check and the
        insert happen under one lock hold, so two concurrent dispatches can't
        both pass.
        """
        job = CursorJob(
            job_id=f"cursor_{uuid.uuid4().hex[:8]}",
            task=task,
            repo=repo,
            inactivity_timeout_s=inactivity_timeout_s,
            max_wall_s=max_wall_s,
            session_name=session_name or "",
            session_key=session_key or "",
            requested_session_id=requested_session_id,
            requested_model=requested_model,
            runtime=runtime or "local",
        )
        with self._lock:
            # Same-repo concurrency guard applies to LOCAL runs only: two
            # cursor agents on one local working tree corrupt it. Cloud runs
            # each get their own ephemeral VM clone, so same-repo parallelism
            # is safe and intended there.
            if (runtime or "local") != "cloud":
                existing = self._find_active_for_repo_locked(repo)
                if existing is not None:
                    return None, existing
            self._jobs[job.job_id] = job
            self._prune_locked()

        thread = threading.Thread(
            target=self._worker,
            args=(job, runner),
            name=f"ghost-cursor-job-{job.job_id}",
            daemon=True,
        )
        job._thread = thread
        thread.start()
        logger.info(
            "Dispatched cursor run %s (repo=%s, resume=%s): %.80s",
            job.job_id, repo, requested_session_id or "-", task,
        )
        return job, None

    def _worker(self, job: CursorJob, runner: Callable[[CursorJob], Dict[str, Any]]) -> None:
        result: Dict[str, Any] = {}
        try:
            result = runner(job) or {}
        except Exception as exc:  # noqa: BLE001 — must never strand the job
            logger.exception("cursor job %s crashed", job.job_id)
            result = {"success": False, "error": f"{type(exc).__name__}: {exc}"}
        finally:
            if not isinstance(result, dict):
                result = {"success": False, "error": "cursor run produced no result"}
            self._finalize(job, result)

    # -- lifecycle ----------------------------------------------------------------

    @staticmethod
    def _terminal_status(job: CursorJob, result: Dict[str, Any]) -> str:
        """Map a final result dict onto the job's terminal status.

        The result dict itself keeps the run's exact status vocabulary (a
        native cancel is result status "failed" — unchanged); the JOB status
        distinguishes "cancelled" so status queries and the completion
        message name the real terminal state.
        """
        st = str(result.get("status") or "")
        if st == "timeout":
            return "timeout"
        if st == "completed" and result.get("success"):
            return "completed"
        if job.cancelled or job.cancel_event.is_set():
            return "cancelled"
        return "failed"

    def _finalize(self, job: CursorJob, result: Dict[str, Any]) -> None:
        """Settle the job and, when delivery is still on, push the completion.

        The status write and the delivery-flag read share one lock hold so a
        concurrent ``mark_handled()`` either lands before (delivery is
        suppressed, the caller reports in-turn) or loses (delivery fires) —
        never neither.
        """
        status = self._terminal_status(job, result)
        with job._lock:
            if job.status in TERMINAL_STATUSES:
                return  # defensive: never double-finalize / double-deliver
            job.result = result
            job.status = status
            job.finished_at = time.time()
            deliver = job.deliver
        # Cancel the pending progress-digest timer BEFORE enqueuing the
        # completion. A tick already in flight re-checks the status under
        # the job lock (progress._Ticker._deliver), so its digest either
        # landed on the queue before the flip above or is dropped — a
        # digest can never follow the completion event.
        _progress.cancel_for_job(job)
        logger.info("Cursor job %s finished: %s (deliver=%s)", job.job_id, status, deliver)
        # Settle the persistent handle table so the handle stays resolvable
        # (and correctly non-running) across process restarts. Keyed by the
        # session NAME (v0.4 handle); name-less jobs fall back to the sid.
        # Supervision settle (RFC §3): the terminal phase write + the
        # supervisor-derived session.settled event (death-shape tagged).
        #
        # EXCEPT: a dispatch that never established a run
        # (job.cursor_session_id empty — a follow-up bounced by 409
        # agent_busy, or any other preflight/create-time rejection) says
        # nothing about the RUN's health. When the persisted handle was
        # already running, its remote run may be alive and well — never
        # overwrite that status (a healthy remote run must not be falsely
        # settled by a send-time failure).
        handle_key = job.session_name or job.cursor_session_id
        preflight_bounce = (
            not job.cursor_session_id
            and bool(handle_key)
            and str((_handles.get(handle_key) or {}).get("status") or "")
            == "running"
        )
        if preflight_bounce:
            logger.warning(
                "Cursor job %s failed before establishing a run; handle %s "
                "stays running (remote run health unaffected by this "
                "dispatch)", job.job_id, handle_key,
            )
        else:
            _supervisor.settle_from_job(job, status)
            if handle_key:
                _handles.record(
                    handle_key,
                    status=status,
                    cursor_session_id=job.cursor_session_id or None,
                    files_changed_count=result.get("files_changed_count"),
                    duration_s=round(
                        (job.finished_at or time.time()) - job.created_at, 1
                    ),
                )
        # Enqueue BEFORE signalling done: anyone who observes the job as
        # finished (waiters, tests, drains) must also find the completion
        # event already on the queue — no observe-then-miss window.
        if deliver:
            self._push_completion_event(job, result)
        job.done_event.set()
        # Unblock anyone still waiting for a session that will never come
        # (e.g. worker/create failure before cloud.session).
        job.session_event.set()

    def _push_completion_event(self, job: CursorJob, result: Dict[str, Any]) -> None:
        """Deliver the terminal result to EVERY subscriber's session.

        Rides the shared ``process_registry.completion_queue`` as
        ``type="async_delegation"`` events — the same rail
        ``delegate_task(background=true)`` uses, already drained by the CLI
        process_loop and the gateway's async-delegation watcher and injected
        as a fresh turn. Fires for EVERY terminal state (unless the outcome
        was already reported in-turn via mark_handled); a failure to enqueue
        is logged loudly because it would mean a silently-lost result.

        Fan-out: one event copy per current subscriber of the session
        (``handles.subscribers_of``), routed by the subscriber's
        ``session_key``. The DISPATCHING session always gets its copy even
        when unsubscribed — the dispatcher's completion must never be lost
        — and recipients are deduped by session_key (the dispatcher is
        normally auto-subscribed; CLI/"" subscribers collapse to one copy).

        Delegation ids are producer-stable AND unique per RUN:
        ``<session>#done-<job_id>`` (plus a subscriber-key hash suffix for
        non-dispatcher copies). Consumers dedupe async completions by
        (delegation_id, type) for their whole lifetime, so a PLAIN
        session name here meant the first run on a session delivered and
        every follow-up's terminal completion was silently swallowed
        (verified live: the Cursor app showed completion, Slack never
        did). The job_id keeps retries of the SAME completion idempotent
        while distinct runs stay distinct; the human-visible session
        title is unchanged (it rides in goal/summary).
        """
        try:
            from tools.process_registry import process_registry
        except Exception as exc:  # pragma: no cover — core import failure
            logger.error(
                "Cursor job %s finished but process_registry import failed; "
                "completion delivery lost: %s", job.job_id, exc,
            )
            return

        handle_key = job.session_name or job.cursor_session_id
        subscribers = _handles.subscribers_of(
            _handles.get(handle_key) if handle_key else None
        )
        dispatcher = str(job.session_key or "")
        recipients = sorted({str(k or "") for k in subscribers} | {dispatcher})

        base_id = job.session_name or job.cursor_session_id or job.job_id
        # Stable per-run terminal id — see the docstring above.
        done_id = f"{base_id}#done-{job.job_id}"
        payload_result = trim_result(result)
        base_evt = {
            "type": "async_delegation",
            "goal": f"cursor: {_clip(job.task, 200)}",
            "context": None,
            "toolsets": None,
            "role": "cursor",
            "model": job.model or job.requested_model or "cursor",
            "status": job.status,
            "summary": self._completion_summary(job, result),
            "error": result.get("error"),
            "api_calls": 0,
            "duration_seconds": round((job.finished_at or time.time()) - job.created_at, 2),
            "dispatched_at": job.created_at,
            "completed_at": job.finished_at or time.time(),
            # Structured extras for programmatic consumers (formatters
            # ignore unknown keys). ``result`` carries the FULL final dict —
            # success/status/summary/files_changed/session_id/resumed — so
            # continuation survives the async boundary.
            "result": payload_result,
            "cursor_job_id": job.job_id,
            "cursor_session_id": job.cursor_session_id,
        }
        for session_key in recipients:
            evt = {
                **base_evt,
                "session_key": session_key,
                "delegation_id": (
                    done_id
                    if session_key == dispatcher
                    else f"{done_id}@{_progress.subscriber_suffix(session_key)}"
                ),
            }
            try:
                process_registry.completion_queue.put(evt)
            except Exception as exc:  # pragma: no cover
                logger.error(
                    "Cursor job %s: failed to enqueue completion event for "
                    "session_key %r; result lost for that session: %s",
                    job.job_id, session_key, exc,
                )

    @staticmethod
    def _completion_summary(job: CursorJob, result: Dict[str, Any]) -> str:
        """The delivered completion message — the v0.4 plain-text format
        (labeled headers, prose, raw fenced diffs; never a JSON blob)."""
        name = job.session_name or job.cursor_session_id or job.job_id
        # "events since prompt" — the spill log is keyed by the same
        # name-first precedence (see CursorJob.append_progress).
        stats = _eventlog.stats(name) or {}
        text = _render.completion_text(
            name=name,
            status=job.status,
            elapsed_s=(job.finished_at or time.time()) - job.created_at,
            repo=job.repo,
            summary=str(result.get("summary") or ""),
            files=result.get("files_changed") or [],
            error=str(result.get("error") or ""),
            total_events=stats.get("total_events", 0),
            last_prompt_seq=_handles.last_prompt_seq(_handles.get(name)),
            retryable=result.get("error_retryable"),
            retry_after=result.get("error_retry_after"),
        )
        return (
            f"{text}\n\nfollow up in this session: "
            f"cursor_send_message('{name}', ...)"
        )

    # -- queries ---------------------------------------------------------------

    def get(self, job_id: str) -> Optional[CursorJob]:
        with self._lock:
            return self._jobs.get(job_id)

    def get_by_name(self, session_name: str) -> Optional[CursorJob]:
        """The newest job dispatched under a session name (v0.4 handle)."""
        name = str(session_name or "").strip()
        if not name:
            return None
        with self._lock:
            jobs = sorted(self._jobs.values(), key=lambda j: j.created_at, reverse=True)
        for job in jobs:
            if job.session_name == name:
                return job
        return None

    def get_by_session(self, session_id: str) -> Optional[CursorJob]:
        """The newest job for a cursor session handle (explicit lookup only).

        Matches the ESTABLISHED handle first; falls back to the REQUESTED
        resume handle so a just-dispatched continuation (``cursor_send`` /
        follow-up) is addressable in the window before its ``cloud.session``
        event fires.
        """
        sid = str(session_id or "").strip()
        if not sid:
            return None
        with self._lock:
            jobs = sorted(self._jobs.values(), key=lambda j: j.created_at, reverse=True)
        for job in jobs:
            if job.cursor_session_id == sid:
                return job
        for job in jobs:
            if not job.cursor_session_id and job.requested_session_id == sid:
                return job
        return None

    def list_jobs(self) -> List[CursorJob]:
        with self._lock:
            return sorted(self._jobs.values(), key=lambda j: j.created_at)

    def find_active_for_repo(self, repo: str) -> Optional[CursorJob]:
        with self._lock:
            return self._find_active_for_repo_locked(repo)

    def _find_active_for_repo_locked(self, repo: str) -> Optional[CursorJob]:
        for job in self._jobs.values():
            if job.status != "running" or job.repo != repo:
                continue
            # Safety valve: a worker that died without finalizing (should be
            # impossible — the worker finalizes in a finally) must not hold
            # the repo hostage forever.
            thread = job._thread
            if thread is not None and not thread.is_alive():
                job.status = "failed"
                job.run_error = job.run_error or "worker thread died without finalizing"
                job.finished_at = job.finished_at or time.time()
                job.done_event.set()
                job.session_event.set()
                continue
            return job
        return None

    # -- housekeeping ------------------------------------------------------------

    def _prune_locked(self) -> None:
        finished = [j for j in self._jobs.values() if j.status in TERMINAL_STATUSES]
        if len(finished) <= MAX_FINISHED_JOBS:
            return
        finished.sort(key=lambda j: j.finished_at or j.created_at)
        for job in finished[: len(finished) - MAX_FINISHED_JOBS]:
            self._jobs.pop(job.job_id, None)

    def _reset_for_tests(self) -> None:
        """Cancel + drop everything (test isolation only)."""
        with self._lock:
            jobs = list(self._jobs.values())
            self._jobs.clear()
        for job in jobs:
            job.request_cancel()
        for job in jobs:
            thread = job._thread
            if thread is not None and thread.is_alive():
                thread.join(timeout=5)


# Module-level singleton (process-local, like the async-delegation records).
registry = CursorJobRegistry()
