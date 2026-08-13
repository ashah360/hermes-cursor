"""Detached "My Machines" worker manager for runtime="local" sessions.

A cloud agent routed with ``env: {"type": "machine", "name": <worker>}``
executes its tool calls inside a self-hosted worker process (the ``agent``
CLI's ``worker start``) registered against the repo's checkout. This module
owns those workers:

* ``ensure_worker(repo_path)`` — reuse the live worker already serving that
  exact checkout, else spawn a fresh one detached and wait (bounded) for
  its "Worker is now running" line.
* Deterministic names: ``<hostname-slug>-<8-char sha256 of the realpath>``
  — one worker per checkout per box, so the plugin can never start a
  SECOND worker on a checkout it already serves (phase-0 lesson: a second
  worker on the same checkout registers fine but NEVER receives
  assignments; runs targeting it hard-fail in ~35s with an empty
  conversation).
* State (``<name>.json``) and logs (``<name>.log``) live under
  ``<HERMES_HOME>/state/ghost_cursor/workers/``. Dead pidfiles are cleaned
  lazily on read.
* Workers are NEVER killed on plugin shutdown — they are cheap, stateless
  between runs, and killing one would strand any in-flight run routed to
  it. A worker that dies is simply respawned on the next send.

Routability: a FRESH worker may still be unroutable (external worker on
the same checkout — e.g. a manually-started one this module doesn't know
about). That failure signature (run goes ERROR within ~60s with zero
conversation events) is detected by the run loop (``cloud_runner``), which
uses :func:`live_workers` to name the likely conflict; ``verified`` is
flipped on the record after the first run that streams real events.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import socket
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

AGENT_CLI = "agent"
# The line the CLI prints when registration succeeded (verified live,
# phase 0/1 probes).
READY_LINE = "Worker is now running"
# How long a fresh spawn may take to print READY_LINE. Module-level so
# tests can shrink it.
READY_TIMEOUT_S = 20.0
_READY_POLL_S = 0.25
# Log tail included in spawn-failure errors.
_ERROR_LOG_TAIL_CHARS = 2_000


class WorkerError(RuntimeError):
    """Worker spawn/registration failure — actionable message, no worker."""


def canonical_repo_path(path: str) -> str:
    """The canonical worktree identity for a local path.

    ``realpath(git rev-parse --show-toplevel)``: a subdirectory keys the
    same worker (and admission identity) as its worktree root, while
    sibling linked worktrees of one repository stay distinct (rev-parse
    returns the WORKTREE root, not the main checkout). Non-git paths
    degrade to their plain realpath. Never raises.
    """
    real = os.path.realpath(str(path))
    try:
        proc = subprocess.run(
            ["git", "-C", real, "rev-parse", "--show-toplevel"],
            capture_output=True, text=True, timeout=10,
        )
        top = (proc.stdout or "").strip()
        if proc.returncode == 0 and top:
            return os.path.realpath(top)
    except Exception:
        logger.debug("canonical_repo_path git probe failed", exc_info=True)
    return real


@dataclass
class WorkerRecord:
    """One managed worker (the persisted ``<name>.json`` shape)."""

    name: str
    repo_path: str
    pid: int
    log_path: str
    started_at: float
    verified: bool = False


def state_dir() -> Path:
    """``<HERMES_HOME>/state/ghost_cursor/workers`` (profile-aware)."""
    try:
        from hermes_constants import get_hermes_home

        home = Path(get_hermes_home())
    except Exception:
        home = Path(os.environ.get("HERMES_HOME", "") or (Path.home() / ".hermes"))
    return home / "state" / "ghost_cursor" / "workers"


def worker_name_for(repo_path: str) -> str:
    """Deterministic worker name for a worktree: one worker per canonical
    worktree per box. The hostname slug keeps names readable in
    cursor.com's machine list; the hash pins the exact worktree."""
    real = canonical_repo_path(repo_path)
    digest = hashlib.sha256(real.encode("utf-8")).hexdigest()[:8]
    host = re.sub(r"[^a-z0-9]+", "-", socket.gethostname().lower()).strip("-")
    return f"{host[:24] or 'host'}-{digest}"


# ---------------------------------------------------------------------------
# Process-table probes (module-level seams for the faked-process tests)
# ---------------------------------------------------------------------------

def _pid_alive(pid: int) -> bool:
    """True when ``pid`` exists AND looks like an agent worker process.

    ``os.kill(pid, 0)`` alone is vulnerable to pid reuse, so the command
    line is checked too (best-effort: an unreadable process table falls
    back to the existence check rather than declaring a live worker dead).
    """
    try:
        os.kill(int(pid), 0)
    except (ProcessLookupError, ValueError):
        return False
    except PermissionError:
        return True  # exists, owned by someone else — treat as alive
    try:
        # -ww: unlimited width — CI runners truncate ps output at 80 cols,
        # which cut "worker start" out of the agent CLI's long cmdline and
        # made live workers look dead.
        proc = subprocess.run(
            ["ps", "-ww", "-p", str(int(pid)), "-o", "command="],
            capture_output=True, text=True, timeout=5,
        )
        command = (proc.stdout or "").strip()
        if proc.returncode == 0 and command:
            return "worker" in command
    except Exception:
        pass
    return True


def _agent_cli_path() -> Optional[str]:
    """The ``agent`` CLI binary, probing ~/.local/bin like the other
    cursor binaries (see runner.subprocess_env)."""
    path = os.environ.get("PATH", "")
    local_bin = str(Path.home() / ".local" / "bin")
    if local_bin not in path.split(":"):
        path = f"{local_bin}:{path}" if path else local_bin
    return shutil.which(AGENT_CLI, path=path)


def _spawn_env() -> Dict[str, str]:
    # CI loader/interpreter vars (e.g. setup-python's LD_LIBRARY_PATH) kill the node-based agent binary.
    return {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("LD_", "DYLD_"))
        and key not in ("PYTHONPATH", "PYTHONHOME")
    }


def _spawn_worker(name: str, repo_path: str, log_path: Path) -> int:
    """Start ``agent worker start`` detached, output to ``log_path``.

    Returns the pid. The process gets its own session so it outlives the
    plugin (never killed on shutdown) and never inherits our terminal.
    """
    cli = _agent_cli_path()
    if not cli:
        raise WorkerError(
            f"the '{AGENT_CLI}' CLI is not on PATH — install the cursor "
            "agent CLI (it provides `agent worker start`) or use "
            "runtime='cloud'"
        )
    env = _spawn_env()
    argv = [cli, "worker", "start", "--name", name, "--worker-dir", str(repo_path)]
    # Temporary CI spawn diagnostics (GHOST_CURSOR_SPAWN_DIAG=1).
    diag = (
        Path(os.environ.get("RUNNER_TEMP") or "/tmp") / f"gc-spawn-diag-{name}.txt"
        if os.environ.get("GHOST_CURSOR_SPAWN_DIAG")
        else None
    )
    if diag is not None:
        argv = ["bash", "-c", f'echo "wrapper up pid=$$" >> "{diag}"; exec "$@"', "bash", *argv]
        redacted = {
            key: (f"<redacted len={len(value)}>" if re.search(r"KEY|TOKEN|SECRET|PASSWORD", key) else value)
            for key, value in sorted(env.items())
        }
        diag.write_text(
            json.dumps(
                {"cli": cli, "cwd": str(repo_path), "argv": argv, "env": redacted},
                indent=1,
            )
            + "\n"
        )
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "ab") as log_file:
        proc = subprocess.Popen(
            argv,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            cwd=str(repo_path),
            env=env,
            start_new_session=True,
        )
    if diag is not None:
        time.sleep(2.0)
        exit_status = proc.poll()  # authoritative: reaps if it exited
        ps = subprocess.run(
            ["ps", "-ww", "-p", str(proc.pid), "-o", "stat=,command="],
            capture_output=True, text=True,
        )
        log_bytes = log_path.stat().st_size if log_path.exists() else -1
        with open(diag, "a") as fh:
            fh.write(
                f"after 2s: pid={proc.pid} poll={exit_status} "
                f"pid_alive={_pid_alive(proc.pid)} ps_rc={ps.returncode} "
                f"ps={ps.stdout.strip()!r} log_bytes={log_bytes}\n"
                f"log: {_log_tail(log_path)!r}\n"
            )
    return proc.pid


# ---------------------------------------------------------------------------
# Record persistence
# ---------------------------------------------------------------------------

def _record_path(name: str) -> Path:
    return state_dir() / f"{name}.json"


def _read_record(name: str) -> Optional[WorkerRecord]:
    path = _record_path(name)
    try:
        data = json.loads(path.read_text("utf-8"))
        return WorkerRecord(
            name=str(data["name"]),
            repo_path=str(data["repo_path"]),
            pid=int(data["pid"]),
            log_path=str(data["log_path"]),
            started_at=float(data.get("started_at") or 0.0),
            verified=bool(data.get("verified")),
        )
    except FileNotFoundError:
        return None
    except Exception:
        logger.warning("unreadable worker record %s — removing", path, exc_info=True)
        path.unlink(missing_ok=True)
        return None


def _write_record(record: WorkerRecord) -> None:
    path = _record_path(record.name)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(asdict(record)), "utf-8")
    tmp.replace(path)


def mark_verified(name: str) -> None:
    """Flip the record's ``verified`` flag after a run streamed real
    events through this worker (the routability proof)."""
    record = _read_record(str(name))
    if record is not None and not record.verified:
        _write_record(WorkerRecord(**{**asdict(record), "verified": True}))


def live_workers() -> List[WorkerRecord]:
    """Every managed worker whose process is still alive. Dead pidfiles
    are cleaned up as they are discovered (lazy cleanup)."""
    directory = state_dir()
    if not directory.is_dir():
        return []
    records: List[WorkerRecord] = []
    for path in sorted(directory.glob("*.json")):
        record = _read_record(path.stem)
        if record is None:
            continue
        if _pid_alive(record.pid):
            records.append(record)
        else:
            logger.info("cleaning dead worker record %s (pid %d)", record.name, record.pid)
            path.unlink(missing_ok=True)
    return records


# ---------------------------------------------------------------------------
# ensure_worker
# ---------------------------------------------------------------------------

def _log_tail(log_path: Path) -> str:
    try:
        return log_path.read_text("utf-8", errors="replace")[-_ERROR_LOG_TAIL_CHARS:]
    except Exception:
        return ""


def _wait_ready(record: WorkerRecord) -> None:
    """Poll the spawn's log for READY_LINE, bounded by READY_TIMEOUT_S."""
    log_path = Path(record.log_path)
    deadline = time.monotonic() + READY_TIMEOUT_S
    dead_reads = 0
    while time.monotonic() < deadline:
        if READY_LINE in _log_tail(log_path):
            return
        # Two consecutive dead readings before declaring death: right after
        # fork the child's cmdline is still the parent's, so a single
        # _pid_alive probe can misread a healthy spawn.
        dead_reads = dead_reads + 1 if not _pid_alive(record.pid) else 0
        if dead_reads >= 2:
            raise WorkerError(
                f"worker '{record.name}' exited during startup — log tail:\n"
                f"{_log_tail(log_path) or '(empty log)'}"
            )
        time.sleep(_READY_POLL_S)
    # The process is still alive but never reported ready. It is NOT
    # killed (it may finish registering late and be reused next send);
    # this send fails actionably instead of dispatching into the void.
    raise WorkerError(
        f"worker '{record.name}' did not report ready within "
        f"{int(READY_TIMEOUT_S)}s — log tail:\n"
        f"{_log_tail(log_path) or '(empty log)'}"
    )


def ensure_worker(repo_path: str) -> WorkerRecord:
    """The live worker serving ``repo_path``, spawning one when needed.

    Reuse first (the common case, and the second-worker trap avoider):
    a live managed worker for this exact worktree is returned as-is. A
    dead record is cleaned and replaced. Raises :class:`WorkerError` when
    the spawn fails or never reports ready.
    """
    real = canonical_repo_path(repo_path)
    name = worker_name_for(real)

    record = _read_record(name)
    if record is not None:
        if _pid_alive(record.pid):
            return record
        logger.info(
            "worker %s (pid %d) is dead — respawning", name, record.pid
        )
        _record_path(name).unlink(missing_ok=True)

    log_path = state_dir() / f"{name}.log"
    pid = _spawn_worker(name, real, log_path)
    record = WorkerRecord(
        name=name,
        repo_path=real,
        pid=pid,
        log_path=str(log_path),
        started_at=time.time(),
        verified=False,
    )
    _write_record(record)
    logger.info("spawned worker %s (pid %d) for %s", name, pid, real)
    _wait_ready(record)
    return record


def unroutable_hint(name: str, repo_path: str) -> str:
    """The actionable message for the phase-0 unroutable-worker signature
    (run ERRORs fast with zero conversation events on a fresh worker):
    most likely a conflicting worker already serves this checkout."""
    others = [
        record for record in live_workers()
        if record.name != name
        and os.path.realpath(record.repo_path) == os.path.realpath(str(repo_path))
    ]
    other_note = (
        "; managed worker(s) already serving this checkout: "
        + ", ".join(record.name for record in others)
        if others
        else (
            "; no OTHER managed worker serves this checkout — check for a "
            "manually-started `agent worker` on it (only one worker per "
            "checkout receives assignments)"
        )
    )
    return (
        f"worker '{name}' is not routable — the run errored without any "
        f"conversation events, the signature of a second worker on the "
        f"same checkout ({repo_path}){other_note}"
    )
