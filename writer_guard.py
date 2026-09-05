"""One writer per canonical LOCAL worktree, across Cursor and Codex.

The reservation is a claim file ``<codex state>/claims/<key>.json`` written
under a per-worktree ``flock`` (``<key>.lock``). Both dispatch paths call
:func:`reserve` inside that lock, so two writers can never both pass the
check: the check and the write are one critical section, across threads,
gateway processes and profiles (the claims dir is machine-global). The
claim is held from BEFORE the dispatch is sent (ambiguous outcomes keep it)
until the run is observed terminal.

Claim liveness (a stale claim is ignored and overwritten):

* ``backend: codex`` — see ``codex_protocol.codex_claim_live``: a
  provisional gateway claim lives with the gateway pid; a claim the
  controller made durable stays live until the controller releases it with
  the matching owner (after the turn settles, or after cleanup is proven).
  Controller death never frees it — its app-server scope may still run.
* ``backend: cursor`` — the dispatching gateway pid is alive, OR the local
  handle for that session is still recorded ``running`` (the supervisor
  keeps that truthful across gateway restarts), OR the worktree's Cursor
  worker still holds a run lease (machine-global evidence from
  ``workers.py``).

Cursor cloud sessions have no local worktree and never reserve.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional, Tuple

from . import codex_protocol as _proto
from . import handles as _handles
from . import workers as _workers

logger = logging.getLogger(__name__)


def claim_key(path: str) -> str:
    return hashlib.sha1(os.path.realpath(path).encode("utf-8")).hexdigest()[:16]


def _pid_birth(pid: int) -> Optional[int]:
    try:
        with open(f"/proc/{pid}/stat", "r") as fh:
            return int(fh.read().rsplit(")", 1)[1].split()[19])
    except Exception:
        return None


def _pid_alive(pid: int, birth: Any) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    try:
        with open(f"/proc/{pid}/stat", "r") as fh:
            if fh.read().rsplit(")", 1)[1].split()[0] == "Z":
                return False
    except Exception:
        pass
    return birth is None or _pid_birth(pid) == birth


def _holder_alive(claim: Dict[str, Any]) -> bool:
    try:
        pid = int(claim.get("holder_pid") or 0)
    except (TypeError, ValueError):
        return False
    return _pid_alive(pid, claim.get("holder_birth"))


def _worker_lease_held(canonical: str) -> bool:
    """Machine-global Cursor evidence: the worktree's worker holds a run lease."""
    try:
        record = _workers._read_record(_workers.worker_name_for(canonical))
    except Exception:
        return False
    return bool(record is not None and record.leases)


def _profile_id() -> str:
    from . import codex_client as _codex

    return _codex.profile_id()


def claim_live(claim: Dict[str, Any]) -> bool:
    if not isinstance(claim, dict):
        return False
    codex = _proto.codex_claim_live(claim)
    if codex is not None:
        return codex  # durable controller claims survive controller death
    if _holder_alive(claim):
        return True
    if str(claim.get("backend") or "") == "cursor":
        # The running-handle check is meaningful only for THIS profile's
        # handle table; another profile's claim falls back to lease evidence.
        if str(claim.get("profile") or "") == _profile_id():
            entry = _handles.get(str(claim.get("session") or ""))
            if entry is not None and str(entry.get("status") or "") == "running":
                return True
        return _worker_lease_held(str(claim.get("cwd") or ""))
    return False


def _paths(canonical: str, claims_dir: str) -> Tuple[str, str]:
    key = claim_key(canonical)
    return os.path.join(claims_dir, f"{key}.json"), os.path.join(claims_dir, f"{key}.lock")


@contextmanager
def _locked(lock_path: str) -> Iterator[None]:
    os.makedirs(os.path.dirname(lock_path), mode=0o700, exist_ok=True)
    fh = open(lock_path, "a+")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(fh, fcntl.LOCK_UN)
        finally:
            fh.close()


def _read(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else None
    except FileNotFoundError:
        return None
    except Exception:
        logger.debug("claim read failed", exc_info=True)
        return None


def _write(path: str, claim: Dict[str, Any]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(claim, fh)
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)


def current(repo: str, claims_dir: str) -> Optional[Dict[str, Any]]:
    """The LIVE claim on ``repo``'s worktree, or None (read-only)."""
    canonical = _workers.canonical_repo_path(repo)
    path, _ = _paths(canonical, claims_dir)
    claim = _read(path)
    return claim if claim is not None and claim_live(claim) else None


def cursor_writer(repo: str) -> Optional[str]:
    """The Cursor session (or worker) actively writing ``repo``'s worktree, or None."""
    try:
        from . import jobs as _jobs

        canonical = _workers.canonical_repo_path(repo)
        job = _jobs.registry.find_active_for_repo(canonical)
        if job is not None and job.runtime != "cloud":
            return job.session_name or job.job_id
        for entry in _handles.entries(scope="all", limit=_handles.MAX_ENTRIES):
            if _handles.backend_of(entry) != "cursor" or _handles.runtime_of(entry) != "local":
                continue
            if str(entry.get("status") or "") != "running":
                continue
            recorded = str(entry.get("repo") or "")
            if recorded and os.path.isdir(recorded) and _workers.canonical_repo_path(recorded) == canonical:
                return str(entry.get("session") or "")
        if _worker_lease_held(canonical):
            return f"cursor worker {_workers.worker_name_for(canonical)} (run lease held)"
    except Exception:
        logger.debug("cursor writer probe failed", exc_info=True)
    return None


def reserve(
    repo: str, backend: str, session: str, claims_dir: str, owner: str, **extra: Any
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Atomically reserve ``repo``'s canonical worktree for one dispatch.

    ``owner`` identifies the RUN (a dispatch intent id), not just the session:
    a second send to the same session is another owner and is refused while
    the first is live, so a loser can never overwrite or release a winner.
    Returns ``(claim, None)`` when reserved (or re-asserted by the same
    owner), ``(None, holder)`` when another live writer holds it. Check and
    write happen under the worktree flock.
    """
    canonical = _workers.canonical_repo_path(repo)
    path, lock_path = _paths(canonical, claims_dir)
    with _locked(lock_path):
        existing = _read(path)
        if existing is not None and str(existing.get("owner") or "") != str(owner) and claim_live(existing):
            return None, existing
        if backend == "codex":
            other = cursor_writer(canonical)
            if other and other != session:
                return None, {"backend": "cursor", "session": other, "cwd": canonical}
        pid = os.getpid()
        claim = {
            "cwd": canonical, "backend": backend, "session": str(session), "owner": str(owner),
            "profile": _profile_id(), "holder_pid": pid, "holder_birth": _pid_birth(pid),
            "claimed_at": round(time.time(), 3), **extra,
        }
        _write(path, claim)
        return claim, None


def release(repo: str, session: str, claims_dir: str, owner: str) -> bool:
    """Drop ``owner``'s claim on the worktree. True when a claim was removed;
    another owner's claim (same session or not) is never touched."""
    canonical = _workers.canonical_repo_path(repo)
    path, lock_path = _paths(canonical, claims_dir)
    with _locked(lock_path):
        existing = _read(path)
        if existing is None or str(existing.get("session")) != str(session):
            return False
        if str(existing.get("owner") or "") != str(owner):
            return False
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        return True


def describe(holder: Optional[Dict[str, Any]]) -> str:
    """Human name for a busy holder. A Cursor holder is its plain handle (so
    the rejection text stays a usable cursor_* argument); a Codex holder is
    '<title> (codex backend)' with the profile prefix stripped."""
    holder = holder or {}
    session = str(holder.get("session") or "?")
    backend = str(holder.get("backend") or "unknown")
    if backend == "cursor":
        return session
    if ":" in session and backend == "codex":
        session = session.split(":", 1)[1]
    return f"{session} ({backend} backend)"
