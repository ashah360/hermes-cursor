"""Persistent handle table for cursor sessions — explicit handles, no heuristics.

EXPLICIT session handles, no auto-resume heuristics: ``cursor_create_session``
registers a session NAME — the caller-provided meaningful title, e.g.
``Fix payment webhook retries`` — and every other tool takes it back (the
cursor agent id resolves as an alias). This module is the tiny persistence
layer under that model — a JSON file mapping
``session_name -> {repo, status, task, model, cursor_session_id, ...}`` so a
handle registered on turn T is still resolvable on turn T+1 even across a
process restart (the live job table in ``jobs.py`` is process-local).

What this is NOT: there is no ``get_recent()``, no repo matching, no age
window, no auto-resume. Lookup is by exact handle only. If the caller lost
the handle, the run's completion delivery re-states it; nothing is guessed.

Session scoping: each entry records the dispatching Hermes ``session_key``
(empty string = CLI). LISTING (``known_handles``) is scoped to the caller's
session_key by default (``scope="session"``); ``scope="all"`` opts into the
global view. Direct ``get(session_id)`` lookups are deliberately UNSCOPED —
an explicit handle is explicit intent, and cross-session continuation must
keep working.

Digest subscriptions: the entry's ``subscribers`` map
(``{hermes_session_key: interval_s}``) is the source of truth for who
receives progress digests and completion fan-out — one subscription per
Hermes session per cursor session (see ``subscribers_of`` /
``set_subscriber``). The pre-multi-subscriber scalar ``update_interval_s``
is MIGRATED on read as a subscription by the entry's dispatching
``session_key``, and kept mirrored on write (the dispatching session's
interval) so older readers of the file keep working.

Storage: ``<HERMES_HOME>/state/ghost_cursor_handles.json``.

Bounded growth: terminal-state entries older than
:data:`PRUNE_TERMINAL_AFTER_S` are dropped on every write, and the table is
capped at :data:`MAX_ENTRIES` — evicting oldest TERMINAL entries first so a
crowd of finished runs can never push out another session's live handle. A
pruned/evicted entry takes its per-session JSONL event log with it
(best-effort delete via ``eventlog.log_path``) so the log directory's file
count stays bounded too.

Contract: never raises. A missing/corrupt/unwritable file degrades to the
in-memory (process-local) view.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Retain this many handles in the file. Sized for many concurrent Hermes
# sessions sharing one table (the entries are small); terminal entries are
# evicted before running ones, so the cap degrades gracefully under load.
MAX_ENTRIES = 500
# Terminal-state entries older than this are pruned on every write.
PRUNE_TERMINAL_AFTER_S = 7 * 24 * 3600

# Mirrors jobs.TERMINAL_STATUSES (duplicated to avoid a circular import —
# jobs.py imports this module).
_TERMINAL_STATUSES = ("completed", "failed", "cancelled", "timeout")

VALID_SCOPES = ("session", "all")

_lock = threading.Lock()
_table: Dict[str, Dict[str, Any]] = {}
_loaded = False


def _state_file() -> Optional[Path]:
    """Path to the JSON backing file (profile-aware). None if unresolvable."""
    try:
        try:
            from hermes_constants import get_hermes_home

            home = Path(get_hermes_home())
        except Exception:
            home = Path.home() / ".hermes"
        return home / "state" / "ghost_cursor_handles.json"
    except Exception:
        return None


def _load_locked() -> None:
    """Merge the backing file into the in-memory table (once per process)."""
    global _loaded
    if _loaded:
        return
    _loaded = True
    path = _state_file()
    try:
        if path is not None and path.is_file():
            data = json.loads(path.read_text("utf-8"))
            if isinstance(data, dict):
                for key, entry in data.items():
                    if isinstance(key, str) and isinstance(entry, dict):
                        # In-memory (this process) entries are fresher.
                        _table.setdefault(key, entry)
    except Exception:
        logger.debug("ghost_cursor handle table load failed", exc_info=True)


def _save_locked() -> bool:
    """Persist the table. Returns whether the write actually landed —
    ``record`` ignores this (degrade-to-memory contract) but
    :func:`record_required` propagates it for writes that MUST be
    durable (create intents / agent identity)."""
    path = _state_file()
    if path is None:
        return False
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(json.dumps(_table, ensure_ascii=False), "utf-8")
        tmp.replace(path)
        return True
    except Exception:
        logger.debug("ghost_cursor handle table save failed", exc_info=True)
        return False


def _is_terminal(entry: Dict[str, Any]) -> bool:
    return str(entry.get("status") or "") in _TERMINAL_STATUSES


def _drop_log(name: str) -> None:
    """Best-effort delete of a pruned session's JSONL event log.

    Without this every pruned/evicted handle leaks one file under
    ``state/ghost_cursor/logs/`` forever — an unbounded file-count leak.
    The import is lazy so this module stays import-light and can never
    join an import cycle with ``eventlog``. Never raises.
    """
    try:
        from . import eventlog

        path = eventlog.log_path(name)
        if path is not None:
            path.unlink(missing_ok=True)
    except Exception:
        logger.debug("ghost_cursor pruned log delete failed", exc_info=True)


def _prune_locked(now: Optional[float] = None) -> None:
    """Age out old terminal entries, then enforce the size cap.

    Eviction order under the cap: oldest TERMINAL entries first; only when
    the table is over cap with no terminal entries left do the oldest
    non-terminal (running/unknown) entries go — a burst of finished runs
    can't push out live handles. Every dropped entry takes its JSONL event
    log with it (:func:`_drop_log`).
    """
    now = time.time() if now is None else now
    for key in [
        k for k, e in _table.items()
        if _is_terminal(e)
        and now - float(e.get("updated_at") or 0.0) > PRUNE_TERMINAL_AFTER_S
    ]:
        _table.pop(key, None)
        _drop_log(key)

    if len(_table) <= MAX_ENTRIES:
        return
    by_age = sorted(
        _table.items(),
        key=lambda kv: (not _is_terminal(kv[1]), float(kv[1].get("updated_at") or 0.0)),
    )
    for key, _ in by_age[: len(_table) - MAX_ENTRIES]:
        _table.pop(key, None)
        _drop_log(key)


def record(session_id: Optional[str], **fields: Any) -> None:
    """Merge ``fields`` into the entry for ``session_id``. Never raises.

    No-ops on a missing session_id (e.g. a run that failed before the
    cursor agent was ever established has no handle to record).
    """
    try:
        if not session_id:
            return
        with _lock:
            _load_locked()
            entry = _table.setdefault(str(session_id), {})
            entry.update({k: v for k, v in fields.items() if v is not None})
            entry["updated_at"] = time.time()
            _prune_locked()
            _save_locked()
    except Exception:
        logger.debug("ghost_cursor handle record failed", exc_info=True)


def record_required(session_id: Optional[str], **fields: Any) -> bool:
    """Like :func:`record`, but FAIL-CLOSED: returns whether the write is
    durably on disk. For the narrow set of writes that gate side effects
    with no recovery path — the pre-POST create intent (the POST is not
    idempotent) and the returned agent identity. Never raises.
    """
    try:
        if not session_id:
            return False
        with _lock:
            _load_locked()
            entry = _table.setdefault(str(session_id), {})
            entry.update({k: v for k, v in fields.items() if v is not None})
            entry["updated_at"] = time.time()
            _prune_locked()
            return _save_locked()
    except Exception:
        logger.warning("ghost_cursor required handle write failed", exc_info=True)
        return False


def subscribers_of(entry: Optional[Dict[str, Any]]) -> Dict[str, float]:
    """The entry's digest subscribers: ``{hermes_session_key: interval_s}``.

    The ``subscribers`` map is the source of truth; when an entry predates
    it, the legacy scalar ``update_interval_s`` migrates on read as a
    subscription by the entry's dispatching ``session_key``. Only positive
    intervals are subscriptions (0 = unsubscribed = absent). Never raises.
    """
    try:
        subs = (entry or {}).get("subscribers")
        if isinstance(subs, dict):
            out: Dict[str, float] = {}
            for key, value in subs.items():
                try:
                    interval = float(value)
                except (TypeError, ValueError):
                    continue
                if interval > 0:
                    out[str(key)] = interval
            return out
        try:
            legacy = float((entry or {}).get("update_interval_s"))
        except (TypeError, ValueError):
            return {}
        if legacy > 0:
            return {str((entry or {}).get("session_key") or ""): legacy}
        return {}
    except Exception:
        logger.debug("ghost_cursor subscribers read failed", exc_info=True)
        return {}


def set_subscriber(
    session_id: Optional[str], session_key: str, interval_s: float
) -> None:
    """Set (interval > 0) or remove (interval <= 0) ONE Hermes session's
    subscription in the entry's ``subscribers`` map. Other subscribers are
    untouched. Read-modify-write under the table lock, migrating the
    legacy scalar first so an old entry's subscription is never clobbered.
    The legacy scalar is kept mirrored to the DISPATCHING session's
    interval (0 when it is unsubscribed) for older readers. Never raises.
    """
    try:
        if not session_id:
            return
        key = str(session_key or "")
        interval = max(float(interval_s), 0.0)
        with _lock:
            _load_locked()
            entry = _table.setdefault(str(session_id), {})
            subs = subscribers_of(entry)
            if interval > 0:
                subs[key] = interval
            else:
                subs.pop(key, None)
            entry["subscribers"] = subs
            entry["update_interval_s"] = subs.get(
                str(entry.get("session_key") or ""), 0.0
            )
            entry["updated_at"] = time.time()
            _prune_locked()
            _save_locked()
    except Exception:
        logger.debug("ghost_cursor subscriber write failed", exc_info=True)


# ---------------------------------------------------------------------------
# Supervision record (RFC: docs/rfcs/session-supervisor.md §1)
# ---------------------------------------------------------------------------
# Each entry grows a ``supervision`` sub-record owned by the supervisor loop
# (supervisor.py) — the durable state that makes gateway restarts a
# non-event. Shape:
#
#     {
#       "phase": "streaming" | ... | "completed" | ...,
#       "current_attempt_id": "att-...",   # stamped on every event as attemptId
#       "attempt_n": 1,                    # display/debug only
#       "last_seq_delivered": {subscriber_key: seq},
#       "watchdog": {"last_poll_ts": epoch, "last_remote_status": "RUNNING"},
#     }
#
# Only the SUPERVISOR writes phase transitions (single-writer settlement,
# RFC §3); tool calls request transitions through supervisor APIs.

# Non-terminal phases: a reconciler pass re-attaches a supervisor to any
# handle in one of these with no live supervisor task.
SUPERVISION_LIVE_PHASES = ("spawning", "streaming", "retrying")
# Terminal phases mirror jobs.TERMINAL_STATUSES.
SUPERVISION_TERMINAL_PHASES = _TERMINAL_STATUSES


def supervision_of(entry: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """The entry's supervision record, normalized with defaults. Never raises.

    An entry that predates the supervisor (or was never dispatched through
    it) reads as phase "" — neither live nor terminal. A RUNNING entry in
    that state is adopted by the reconciler (supervisor._adopt_legacy_handle
    seeds a live record and re-attaches); settled/never-run entries keep
    working untouched.
    """
    try:
        raw = (entry or {}).get("supervision")
        raw = raw if isinstance(raw, dict) else {}
        cursors = raw.get("last_seq_delivered")
        watchdog = raw.get("watchdog")
        return {
            "phase": str(raw.get("phase") or ""),
            "current_attempt_id": str(raw.get("current_attempt_id") or ""),
            "attempt_n": max(int(raw.get("attempt_n") or 0), 0),
            "last_seq_delivered": (
                {
                    str(k): int(v)
                    for k, v in cursors.items()
                    if isinstance(v, (int, float))
                }
                if isinstance(cursors, dict)
                else {}
            ),
            "watchdog": dict(watchdog) if isinstance(watchdog, dict) else {},
        }
    except Exception:
        logger.debug("ghost_cursor supervision read failed", exc_info=True)
        return {
            "phase": "",
            "current_attempt_id": "",
            "attempt_n": 0,
            "last_seq_delivered": {},
            "watchdog": {},
        }


def supervision_is_live(entry: Optional[Dict[str, Any]]) -> bool:
    """True when the entry's supervision phase needs a live supervisor."""
    return supervision_of(entry)["phase"] in SUPERVISION_LIVE_PHASES


def record_supervision(session_id: Optional[str], **fields: Any) -> None:
    """Merge ``fields`` into the entry's supervision record. Never raises.

    Read-modify-write under the table lock; None values are skipped (same
    contract as :func:`record`). ``last_seq_delivered`` cursors have their
    own advance-only writer (:func:`advance_delivery_cursor`) and are
    deliberately NOT writable here.
    """
    try:
        if not session_id:
            return
        fields.pop("last_seq_delivered", None)
        with _lock:
            _load_locked()
            name = _resolve_locked(str(session_id).strip()) or str(session_id)
            entry = _table.setdefault(name, {})
            current = entry.get("supervision")
            sup = dict(current) if isinstance(current, dict) else {}
            sup.update({k: v for k, v in fields.items() if v is not None})
            entry["supervision"] = sup
            entry["updated_at"] = time.time()
            _prune_locked()
            _save_locked()
    except Exception:
        logger.debug("ghost_cursor supervision record failed", exc_info=True)


def transition_supervision(session_id: Optional[str], to_phase: str) -> bool:
    """Atomically move the supervision phase from a LIVE phase to
    ``to_phase``. Returns True only for the writer that actually
    transitioned — the settle gate that makes completion fan-out
    exactly-once (RFC §3: single-writer settlement). A phase that is
    already terminal (or empty — never supervised) refuses the
    transition. Never raises (a failure reads as "did not transition").
    """
    try:
        if not session_id:
            return False
        with _lock:
            _load_locked()
            name = _resolve_locked(str(session_id).strip()) or str(session_id)
            entry = _table.setdefault(name, {})
            current = entry.get("supervision")
            sup = dict(current) if isinstance(current, dict) else {}
            if str(sup.get("phase") or "") not in SUPERVISION_LIVE_PHASES:
                return False
            sup["phase"] = str(to_phase)
            entry["supervision"] = sup
            entry["updated_at"] = time.time()
            _save_locked()
            return True
    except Exception:
        logger.debug("ghost_cursor supervision transition failed", exc_info=True)
        return False


def advance_delivery_cursor(
    session_id: Optional[str], subscriber_key: str, seq: int
) -> None:
    """Advance ONE subscriber's ``last_seq_delivered`` cursor. Never raises.

    Advance-only (RFC non-goals: the cursor moves only after successful
    delivery onto the completion queue; a stale writer can never rewind
    it — duplicate digests are acceptable, lost events are not).
    """
    try:
        if not session_id:
            return
        seq = int(seq)
        key = str(subscriber_key or "")
        with _lock:
            _load_locked()
            name = _resolve_locked(str(session_id).strip()) or str(session_id)
            entry = _table.setdefault(name, {})
            current = entry.get("supervision")
            sup = dict(current) if isinstance(current, dict) else {}
            cursors = sup.get("last_seq_delivered")
            cursors = dict(cursors) if isinstance(cursors, dict) else {}
            try:
                prior = int(cursors.get(key, -1))
            except (TypeError, ValueError):
                prior = -1
            if seq <= prior:
                return
            cursors[key] = seq
            sup["last_seq_delivered"] = cursors
            entry["supervision"] = sup
            entry["updated_at"] = time.time()
            _save_locked()
    except Exception:
        logger.debug("ghost_cursor delivery cursor write failed", exc_info=True)


def _resolve_locked(identifier: str) -> Optional[str]:
    """Canonical handle name for ``identifier`` (name or UUID alias)."""
    if identifier in _table:
        return identifier
    for name, entry in _table.items():
        if (
            isinstance(entry, dict)
            and str(entry.get("cursor_session_id") or "") == identifier
        ):
            return name
    return None


def resolve(identifier: Optional[str]) -> Optional[str]:
    """The canonical session NAME for a name-or-UUID identifier, or None.

    The table is keyed by the meaningful session title (``Fix payment
    webhook retries``); the cursor agent id is recorded on the entry as
    ``cursor_session_id`` and stays a working alias — this is the lookup
    that makes UUIDs resolve everywhere a name is accepted. Never raises.
    """
    try:
        ident = str(identifier or "").strip()
        if not ident:
            return None
        with _lock:
            _load_locked()
            return _resolve_locked(ident)
    except Exception:
        logger.debug("ghost_cursor handle resolve failed", exc_info=True)
        return None


def get(session_id: Optional[str]) -> Optional[Dict[str, Any]]:
    """The persisted entry for a name or UUID alias, or None. Never raises.

    Deliberately UNSCOPED: an explicit handle is explicit intent, so direct
    lookups resolve across Hermes sessions (see module docstring).
    """
    try:
        if not session_id:
            return None
        with _lock:
            _load_locked()
            name = _resolve_locked(str(session_id).strip())
            entry = _table.get(name) if name else None
        return dict(entry) if isinstance(entry, dict) else None
    except Exception:
        logger.debug("ghost_cursor handle lookup failed", exc_info=True)
        return None


def runtime_of(entry: Optional[Dict[str, Any]]) -> str:
    """The entry's runtime: "local" | "cloud" | "legacy". Never raises.

    Entries written since the cloud migration always carry ``runtime``
    (recorded at create). An entry WITHOUT one predates the migration —
    a bridge-era session whose agent id is not a cloud agent — and reads
    as "legacy" so sends can refuse it with an actionable message.
    """
    try:
        return str((entry or {}).get("runtime") or "legacy")
    except Exception:
        return "legacy"


def last_prompt_seq(entry: Optional[Dict[str, Any]]) -> int:
    """The event-log position recorded when the session was last prompted.

    ``cursor_send_message`` stamps ``last_prompt_seq`` (the log's total
    event count at dispatch) onto the handle entry, so
    ``total_events - last_prompt_seq`` is "events since the last prompt".
    Legacy entries predate the field and a corrupt value must not crash a
    status call — both sanitize to 0, which counts the whole log. Never
    raises.
    """
    try:
        return max(int((entry or {}).get("last_prompt_seq") or 0), 0)
    except (TypeError, ValueError):
        return 0


def _in_scope(entry: Dict[str, Any], scope: str, session_key: str) -> bool:
    if scope == "all":
        return True
    # Session scope: entries recorded by the same Hermes session. Entries
    # written before session_key existed (legacy) count as CLI (key "").
    return str(entry.get("session_key") or "") == session_key


def known_handles(
    limit: int = 10,
    scope: str = "session",
    session_key: str = "",
) -> List[str]:
    """The most recently updated handles (for actionable error messages).

    ``scope="session"`` (default) lists only handles recorded by
    ``session_key``; ``scope="all"`` lists everything. An unknown scope
    value falls back to "session" — the conservative view.
    """
    try:
        scope = scope if scope in VALID_SCOPES else "session"
        with _lock:
            _load_locked()
            items = sorted(
                (
                    kv for kv in _table.items()
                    if _in_scope(kv[1], scope, str(session_key or ""))
                ),
                key=lambda kv: float(kv[1].get("updated_at") or 0.0),
                reverse=True,
            )
        return [k for k, _ in items[: max(int(limit), 0)]]
    except Exception:
        return []


def entries(
    scope: str = "session",
    session_key: str = "",
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Scoped handle entries, most recently updated first (for cursor_list).

    Each item is a copy of the persisted entry with the handle name added
    under ``"session"``. Never raises.
    """
    try:
        scope = scope if scope in VALID_SCOPES else "session"
        with _lock:
            _load_locked()
            items = sorted(
                (
                    kv for kv in _table.items()
                    if isinstance(kv[1], dict)
                    and _in_scope(kv[1], scope, str(session_key or ""))
                ),
                key=lambda kv: float(kv[1].get("updated_at") or 0.0),
                reverse=True,
            )
            return [
                {"session": name, **entry}
                for name, entry in items[: max(int(limit), 0)]
            ]
    except Exception:
        return []
