"""Cross-backend writer exclusion for one canonical LOCAL worktree.

Two agents writing one working tree corrupt it, whichever backend drives
them. Both dispatch paths ask here before starting a turn:

* Cursor writer evidence: an in-process running local job on the repo, or
  a persisted local Cursor handle still recorded ``running`` (the
  supervisor keeps that record truthful across gateway restarts).
* Codex writer evidence: the controller's active-turn claim file
  (``<codex state>/claims/<sha1(realpath)[:16]>.json``) whose holder pid
  is alive with the same process birth. A dead holder's claim is stale
  and ignored.

Cursor cloud sessions have no local worktree and are never blocked.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from typing import Any, Dict, Optional

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


def _holder_alive(claim: Dict[str, Any]) -> bool:
    try:
        pid = int(claim.get("holder_pid") or 0)
    except (TypeError, ValueError):
        return False
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
    birth = claim.get("holder_birth")
    return birth is None or _pid_birth(pid) == birth


def codex_writer(repo: str, claims_dir: str) -> Optional[Dict[str, Any]]:
    """The live Codex claim on ``repo``'s canonical worktree, or None."""
    try:
        canonical = _workers.canonical_repo_path(repo)
        path = os.path.join(claims_dir, f"{claim_key(canonical)}.json")
        with open(path, "r", encoding="utf-8") as fh:
            claim = json.load(fh)
    except FileNotFoundError:
        return None
    except Exception:
        logger.debug("codex claim read failed", exc_info=True)
        return None
    if not isinstance(claim, dict) or not _holder_alive(claim):
        return None
    return claim


def cursor_writer(repo: str) -> Optional[str]:
    """The Cursor session name actively writing ``repo``'s worktree, or None."""
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
    except Exception:
        logger.debug("cursor writer probe failed", exc_info=True)
    return None
