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

import fcntl
import hashlib
import json
import logging
import os
import re
import shutil
import socket
import subprocess
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

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


RECORD_VERSION = 2

# Worker record states. "spawning" = process up, readiness not yet proven
# THIS generation; "ready" = readiness proven; "failed" = the generation
# died during startup (kept for forensics until the next ensure).
STATE_SPAWNING = "spawning"
STATE_READY = "ready"
STATE_FAILED = "failed"


@dataclass
class WorkerRecord:
    """One managed worker (the persisted, versioned ``<name>.json`` shape).

    v2 adds durable supervision identity: ``generation`` fences every
    record write (a losing/stale generation can never overwrite the
    authoritative record), ``unit``/``supervision`` carry the systemd
    transient-service identity (or the degraded detached fallback),
    ``data_dir`` is the worker's isolated ``CURSOR_DATA_DIR``,
    ``management_addr`` the CLI's health/readiness endpoint, ``pid_birth``
    the /proc starttime pid-reuse fence, ``log_offset`` the byte position
    readiness evidence must appear AFTER (generation-scoped readiness),
    and ``leases`` the per-RUN leases that protect the worker from the
    idle reaper. A v1 record (no ``version``) is adopted on read — never
    killed — as supervision="legacy".
    """

    name: str
    repo_path: str
    pid: int
    log_path: str
    started_at: float
    verified: bool = False
    version: int = RECORD_VERSION
    generation: str = ""
    unit: str = ""
    supervision: str = "detached"  # "systemd" | "detached" | "legacy"
    data_dir: str = ""
    management_addr: str = ""
    pid_birth: Optional[int] = None
    state: str = STATE_READY
    ready_at: Optional[float] = None
    last_active_at: float = 0.0
    last_error: str = ""
    log_offset: int = 0
    leases: Dict[str, Dict[str, Any]] = field(default_factory=dict)


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
# Cross-process serialization + record persistence (versioned, fenced)
# ---------------------------------------------------------------------------

@contextmanager
def _worker_lock(name: str) -> Iterator[None]:
    """Per-worker cross-process mutex (flock) serializing every
    reconcile/spawn/record mutation for one worker name."""
    path = state_dir() / "locks" / f"{name}.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def _pid_birth(pid: int) -> Optional[int]:
    """The process's /proc starttime (clock ticks since boot) — the cheap
    pid-reuse fence. None when unreadable (non-Linux, dead, permission)."""
    try:
        stat = Path(f"/proc/{int(pid)}/stat").read_text("utf-8")
        # Field 22, counting from 1 AFTER the parenthesized comm (which
        # may itself contain spaces/parens).
        after_comm = stat.rsplit(")", 1)[1].split()
        return int(after_comm[19])
    except Exception:
        return None


def _record_path(name: str) -> Path:
    return state_dir() / f"{name}.json"


def _mint_generation(prefix: str = "det") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _read_record(name: str) -> Optional[WorkerRecord]:
    """The persisted record, adopting v1 (pre-versioned) shapes safely.

    A legacy record — written before the v2 controller — is adopted as
    ``supervision="legacy"`` with a deterministic ``legacy-<pid>``
    generation and state "ready" (it WAS serving runs). It is never
    killed on read; it is replaced only when it dies or idles out.
    """
    path = _record_path(name)
    try:
        data = json.loads(path.read_text("utf-8"))
        base = dict(
            name=str(data["name"]),
            repo_path=str(data["repo_path"]),
            pid=int(data["pid"]),
            log_path=str(data["log_path"]),
            started_at=float(data.get("started_at") or 0.0),
            verified=bool(data.get("verified")),
        )
        if not data.get("version"):
            return WorkerRecord(
                **base,
                version=RECORD_VERSION,
                generation=f"legacy-{int(data['pid'])}",
                supervision="legacy",
                state=STATE_READY,
                last_active_at=float(data.get("started_at") or 0.0),
            )
        leases = data.get("leases")
        return WorkerRecord(
            **base,
            version=int(data.get("version") or RECORD_VERSION),
            generation=str(data.get("generation") or ""),
            unit=str(data.get("unit") or ""),
            supervision=str(data.get("supervision") or "detached"),
            data_dir=str(data.get("data_dir") or ""),
            management_addr=str(data.get("management_addr") or ""),
            pid_birth=(
                int(data["pid_birth"])
                if data.get("pid_birth") is not None
                else None
            ),
            state=str(data.get("state") or STATE_READY),
            ready_at=(
                float(data["ready_at"])
                if data.get("ready_at") is not None
                else None
            ),
            last_active_at=float(data.get("last_active_at") or 0.0),
            last_error=str(data.get("last_error") or ""),
            log_offset=int(data.get("log_offset") or 0),
            leases=dict(leases) if isinstance(leases, dict) else {},
        )
    except FileNotFoundError:
        return None
    except Exception:
        logger.warning("unreadable worker record %s — removing", path, exc_info=True)
        path.unlink(missing_ok=True)
        return None


def _write_record(record: WorkerRecord) -> None:
    """Persist ``record`` atomically. Callers hold the worker's flock for
    any read-modify-write; new-generation writes happen inside
    ``ensure_worker``'s lock hold."""
    path = _record_path(record.name)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(asdict(record)), "utf-8")
    tmp.replace(path)


def _update_record(name: str, expected_generation: str, **fields: Any) -> bool:
    """Fenced read-modify-write: merge ``fields`` into the record ONLY
    when its generation still equals ``expected_generation``.

    Returns False (and writes nothing) when the record is gone or another
    generation owns it — a failed/losing generation can never overwrite
    the authoritative record. Takes the worker flock itself; do not call
    while already holding it (use :func:`_update_record_locked`).
    """
    with _worker_lock(name):
        return _update_record_locked(name, expected_generation, **fields)


def _update_record_locked(
    name: str, expected_generation: str, **fields: Any
) -> bool:
    record = _read_record(name)
    if record is None or record.generation != expected_generation:
        return False
    _write_record(WorkerRecord(**{**asdict(record), **fields}))
    return True


def mark_verified(name: str) -> None:
    """Flip the record's ``verified`` flag after a run streamed real
    events through this worker — the routability proof, which holds for
    the CURRENT generation only (a respawn resets it)."""
    with _worker_lock(str(name)):
        record = _read_record(str(name))
        if record is not None and not record.verified:
            _update_record_locked(
                str(name), record.generation, verified=True
            )


def _record_alive(record: WorkerRecord) -> bool:
    """Process-level liveness with the pid-birth fence: the recorded pid
    must exist AND (when a birth was recorded) be the SAME incarnation —
    a reused pid never reads as a live worker."""
    if not _pid_alive(record.pid):
        return False
    if record.pid_birth is not None:
        birth = _pid_birth(record.pid)
        if birth is not None and birth != record.pid_birth:
            return False
    return True


def live_workers() -> List[WorkerRecord]:
    """Every managed worker whose process is still alive. Dead records
    are cleaned up as they are discovered (lazy cleanup, under the
    per-worker flock so a spawner is never raced)."""
    directory = state_dir()
    if not directory.is_dir():
        return []
    records: List[WorkerRecord] = []
    for path in sorted(directory.glob("*.json")):
        record = _read_record(path.stem)
        if record is None:
            continue
        if _record_alive(record):
            records.append(record)
        else:
            with _worker_lock(record.name):
                current = _read_record(record.name)
                if current is not None and not _record_alive(current):
                    logger.info(
                        "cleaning dead worker record %s (pid %d)",
                        current.name, current.pid,
                    )
                    _cleanup_generation(current)
    return records


# ---------------------------------------------------------------------------
# ensure_worker
# ---------------------------------------------------------------------------

def _log_tail(log_path: Path) -> str:
    try:
        return log_path.read_text("utf-8", errors="replace")[-_ERROR_LOG_TAIL_CHARS:]
    except Exception:
        return ""


def _log_since(log_path: Path, offset: int) -> str:
    """The log content written AFTER ``offset`` — the only bytes that
    count as THIS generation's evidence (stale-readiness fix: an old
    incarnation's ready line must never prove a fresh spawn). A file now
    SHORTER than the offset was truncated/rotated since the spawn — all
    of its content is newer than the offset, so it all counts."""
    try:
        offset = max(int(offset), 0)
        if log_path.stat().st_size < offset:
            offset = 0
        with log_path.open("rb") as fh:
            fh.seek(offset)
            return fh.read().decode("utf-8", errors="replace")
    except Exception:
        return ""


def _ready_evidence(record: WorkerRecord) -> bool:
    """Whether THIS generation has produced readiness evidence."""
    return READY_LINE in _log_since(Path(record.log_path), record.log_offset)


def _wait_ready(record: WorkerRecord) -> None:
    """Poll for THIS generation's readiness, bounded by READY_TIMEOUT_S.

    Evidence is generation-scoped: only log content past the spawn-time
    ``log_offset`` counts. Raises :class:`WorkerError` on startup death
    or timeout; the caller persists the failure onto the record.
    """
    log_path = Path(record.log_path)
    deadline = time.monotonic() + READY_TIMEOUT_S
    dead_reads = 0
    while time.monotonic() < deadline:
        if _ready_evidence(record):
            return
        # Two consecutive dead readings before declaring death: right after
        # fork the child's cmdline is still the parent's, so a single
        # _pid_alive probe can misread a healthy spawn.
        dead_reads = dead_reads + 1 if not _pid_alive(record.pid) else 0
        if dead_reads >= 2:
            raise WorkerError(
                f"worker '{record.name}' exited during startup — log tail:\n"
                f"{_log_since(log_path, record.log_offset) or _log_tail(log_path) or '(empty log)'}"
            )
        time.sleep(_READY_POLL_S)
    # The process is still alive but never reported ready. It is NOT
    # killed (it may finish registering late; the next ensure re-proves
    # readiness before reuse); this send fails actionably instead of
    # dispatching into the void.
    raise WorkerError(
        f"worker '{record.name}' did not report ready within "
        f"{int(READY_TIMEOUT_S)}s — log tail:\n"
        f"{_log_since(log_path, record.log_offset) or '(empty log)'}"
    )


def _cleanup_generation(record: WorkerRecord) -> None:
    """Remove a dead/finished generation's record and its per-generation
    log (caller holds the flock). Process-tree teardown for still-live
    remnants is the stop path (:func:`_stop_generation`); this only
    retires the durable state."""
    _record_path(record.name).unlink(missing_ok=True)
    # Only per-generation logs are removed; a legacy record's shared
    # <name>.log is left for post-mortems.
    if record.generation and record.generation in Path(record.log_path).name:
        Path(record.log_path).unlink(missing_ok=True)


def _revalidate_ready(record: WorkerRecord) -> WorkerRecord:
    """Re-prove readiness for a live-but-never-ready generation.

    A generation that timed out during its original ready wait is never
    reused as healthy on trust: it must show readiness evidence NOW
    (management probe when available, else this generation's log slice).
    Success flips the record to ready; failure raises the same
    actionable not-ready error. Caller holds the flock.
    """
    if _probe_ready(record) or _ready_evidence(record):
        _update_record_locked(
            record.name, record.generation,
            state=STATE_READY, ready_at=time.time(), last_error="",
        )
        return _read_record(record.name) or record
    raise WorkerError(
        f"worker '{record.name}' is running but has not proven readiness "
        f"(state {record.state!r}; last error: "
        f"{record.last_error or 'none recorded'}) — not dispatching into "
        "an unregistered worker; retry shortly or stop it"
    )


def _probe_ready(record: WorkerRecord) -> bool:
    """Management-endpoint readiness (connected to the backend), when the
    generation exposes one. False on no endpoint / unreachable."""
    return bool((_probe_management(record) or {}).get("connected"))


def _probe_management(record: WorkerRecord) -> Optional[Dict[str, Any]]:
    """GET /readyz off the worker's management endpoint.

    Returns the parsed body ({status, connected, claimed}) regardless of
    the HTTP status (503 = alive-but-busy/unregistered, still signal),
    or None when the generation has no endpoint or it is unreachable.
    """
    addr = str(record.management_addr or "")
    if not addr:
        return None
    import urllib.error
    import urllib.request

    try:
        request = urllib.request.urlopen(  # noqa: S310 — localhost only
            f"http://{addr}/readyz", timeout=2.0
        )
        body = request.read()
    except urllib.error.HTTPError as exc:  # 503 not_ready still has a body
        try:
            body = exc.read()
        except Exception:
            return None
    except Exception:
        return None
    try:
        data = json.loads(body.decode("utf-8", errors="replace"))
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def ensure_worker(repo_path: str) -> WorkerRecord:
    """The live worker serving ``repo_path``, spawning one when needed.

    Serialized cross-process by the per-worker flock. Reuse first (the
    common case, and the second-worker trap avoider): a live managed
    worker for this worktree is returned as-is when READY — a live
    generation that never proved readiness is re-proven before reuse,
    never trusted. A dead record is cleaned and replaced by a fresh
    generation whose record is only written once its process exists, so
    a losing spawner can never poison the winner's record. Raises
    :class:`WorkerError` when the spawn fails or never reports ready.
    """
    real = canonical_repo_path(repo_path)
    name = worker_name_for(real)

    with _worker_lock(name):
        record = _read_record(name)
        if record is not None:
            if _record_alive(record) and record.state != STATE_FAILED:
                if record.state == STATE_READY:
                    return record
                return _revalidate_ready(record)
            logger.info(
                "worker %s (pid %d) is dead — respawning", name, record.pid
            )
            _stop_generation(record)
            _cleanup_generation(record)
        return _spawn_generation(name, real)


def _spawn_generation(name: str, real: str) -> WorkerRecord:
    """Spawn a fresh generation for ``name`` (caller holds the flock).

    Each generation gets its OWN log file (``<name>-<generation>.log``)
    so readiness evidence is structurally generation-scoped: an old
    incarnation's 'Worker is now running' line can never prove a fresh
    spawn ready.
    """
    generation = _mint_generation()
    log_path = state_dir() / f"{name}-{generation}.log"
    log_offset = log_path.stat().st_size if log_path.exists() else 0
    pid = _spawn_worker(name, real, log_path)
    record = WorkerRecord(
        name=name,
        repo_path=real,
        pid=pid,
        log_path=str(log_path),
        started_at=time.time(),
        verified=False,
        generation=generation,
        state=STATE_SPAWNING,
        log_offset=log_offset,
        pid_birth=_pid_birth(pid),
        last_active_at=time.time(),
    )
    _write_record(record)
    logger.info(
        "spawned worker %s (pid %d, generation %s) for %s",
        name, pid, generation, real,
    )
    try:
        _wait_ready(record)
    except WorkerError as exc:
        state = STATE_FAILED if not _pid_alive(record.pid) else STATE_SPAWNING
        _update_record_locked(
            name, generation, state=state, last_error=str(exc)[:500],
        )
        raise
    _update_record_locked(
        name, generation,
        state=STATE_READY, ready_at=time.time(), last_error="",
    )
    return _read_record(name) or record


def _stop_generation(record: WorkerRecord) -> None:
    """Best-effort teardown of a generation's process remnants (caller
    holds the flock). Fleshed out by the supervision layer (systemd unit
    stop / process-group kill); a dead record is a no-op."""
    if not _record_alive(record):
        return
    _kill_detached(record)


def _kill_detached(record: WorkerRecord) -> None:
    """Bounded TERM→KILL of a detached generation's process group."""
    import signal

    for sig, wait_s in ((signal.SIGTERM, 5.0), (signal.SIGKILL, 2.0)):
        try:
            os.killpg(int(record.pid), sig)
        except (ProcessLookupError, PermissionError, OSError):
            try:
                os.kill(int(record.pid), sig)
            except Exception:
                return
        deadline = time.monotonic() + wait_s
        while time.monotonic() < deadline:
            if not _pid_alive(record.pid):
                return
            time.sleep(0.1)


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
