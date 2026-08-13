""""My Machines" worker controller for runtime="local" sessions (v2).

A cloud agent routed with ``env: {"type": "machine", "name": <worker>}``
executes its tool calls inside a self-hosted worker process (the ``agent``
CLI's ``worker start``) registered against a worktree. This module is the
controller that owns those workers:

* **Canonical identity** — every local path resolves through
  ``realpath(git rev-parse --show-toplevel)``: one logical worker per
  canonical worktree per box. ``/repo`` and ``/repo/subdir`` are ONE
  identity; sibling linked git worktrees stay distinct. Deterministic
  names: ``<hostname-slug>-<8-char sha256 of the canonical path>`` — the
  plugin can never start a SECOND worker on a worktree it already serves
  (phase-0 lesson: a second worker on the same checkout registers fine
  but NEVER receives assignments).
* **Isolation** — each worker gets a private deterministic
  ``CURSOR_DATA_DIR`` (the CLI takes SQLite BEGIN EXCLUSIVE on
  ``<data-dir>/worker.lock``, so isolation is what makes parallel
  worktrees possible; auth lives in the config dir and stays shared) and
  a localhost ``--management-addr`` exposing /healthz /readyz /metrics.
* **Supervision** — a deterministic transient user systemd service per
  worker (``cursor-worker-<name>.service``): gateway-independent
  lifetime, authoritative MainPID + InvocationID (the record's
  generation), bounded full-cgroup stop, adoption across restarts.
  Without a user manager the spawn DEGRADES (clearly surfaced) to a
  detached process with bounded process-group teardown.
* **Generation-fenced records** — versioned ``<name>.json`` under
  ``<HERMES_HOME>/state/ghost_cursor/workers/``; every mutation is
  serialized by a per-worker flock and fenced by generation, so a
  losing/stale generation can never overwrite the authoritative record.
  v1 records are adopted, never killed. Readiness evidence is
  generation-scoped (per-generation log + management probe); a
  generation that never proved readiness is re-proven before reuse.
* **Per-RUN leases** — a run leases its worker from dispatch until the
  observed remote-terminal settle; :func:`reconcile` (the supervisor's
  tick) reaps only LEASELESS workers past :data:`IDLE_TTL_S`. At the
  cap (:func:`_max_workers`) idle workers are reclaimed first; an
  all-leased fleet raises an honest capacity error.

Health, registration, and routability are three DISTINCT facts: process
liveness (pid + birth identity / unit state), backend registration (the
management endpoint's ``connected``), and proven routing (``verified`` —
flipped by the first run that streams real events through this
generation; the phase-0 unroutable signature is detected by
``cloud_runner``, which uses :func:`live_workers` to name the conflict).
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
# A stop that could NOT be verified complete: the record is retained
# (fail closed — never an untracked remnant or a duplicate spawn) until
# a later verified stop succeeds.
STATE_DRAINING = "draining"


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


def _profile_state_dir() -> Path:
    """The pre-v2 PROFILE-local worker-state location (migration source)."""
    try:
        from hermes_constants import get_hermes_home

        home = Path(get_hermes_home())
    except Exception:
        home = Path(os.environ.get("HERMES_HOME", "") or (Path.home() / ".hermes"))
    return home / "state" / "ghost_cursor" / "workers"


# Migration is idempotent but cheap to skip: one pass per target dir per
# process (tests re-point XDG_STATE_HOME per test and reset this).
_migrated_dirs: set = set()


def _reset_migration_for_tests() -> None:
    _migrated_dirs.clear()


def state_dir() -> Path:
    """The MACHINE-global worker control directory.

    Worker identity is machine-global (one worker per worktree per box;
    systemd unit names are machine-global), so records, locks, and data
    roots must be shared across Hermes profiles — otherwise profile B
    reads profile A's live unit as an unmanaged remnant and stops it.
    Session handles stay profile-local. Pre-v2 profile-local records are
    migrated here by ADOPTION (copied, never killed) on first use.
    """
    base = os.environ.get("XDG_STATE_HOME", "").strip() or str(
        Path.home() / ".local" / "state"
    )
    target = Path(base) / "ghost_cursor" / "workers"
    key = str(target)
    if key not in _migrated_dirs:
        _migrated_dirs.add(key)
        _migrate_profile_records(target)
    return target


def _migrate_profile_records(target: Path) -> None:
    """Copy pre-v2 profile-local records into the machine-global control
    dir (adoption: live workers keep running untouched; the originals are
    left behind for any old plugin version still reading them). Never
    raises."""
    try:
        source = _profile_state_dir()
        if not source.is_dir() or source == target:
            return
        for path in source.glob("*.json"):
            dest = target / path.name
            if dest.exists():
                continue
            target.mkdir(parents=True, exist_ok=True)
            dest.write_text(path.read_text("utf-8"), "utf-8")
            logger.info(
                "migrated profile-local worker record %s to the "
                "machine-global control dir (adopted, not respawned)",
                path.name,
            )
    except Exception:
        logger.warning("worker record migration failed", exc_info=True)


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


def _spawn_command(
    cli: str,
    name: str,
    repo_path: str,
    data_dir: str = "",
    management_addr: str = "",
) -> tuple:
    """(argv, env) for one worker generation.

    ``data_dir`` isolates the worker's ``CURSOR_DATA_DIR`` (the CLI takes
    a SQLite BEGIN EXCLUSIVE on <data-dir>/worker.lock, so isolation is
    what makes parallel worktrees possible; auth lives in the config dir
    and stays shared). ``management_addr`` exposes the CLI's
    /healthz /readyz /metrics endpoint (localhost only).
    """
    env = _spawn_env()
    if data_dir:
        env["CURSOR_DATA_DIR"] = str(data_dir)
    argv = [cli, "worker", "start", "--name", name, "--worker-dir", str(repo_path)]
    if management_addr:
        argv += ["--management-addr", str(management_addr)]
    return argv, env


# ---------------------------------------------------------------------------
# User-systemd transient services (seams — tests fake these four)
# ---------------------------------------------------------------------------

UNIT_PREFIX = "cursor-worker-"
# Bounded full-cgroup stop: systemd escalates TERM → KILL at this bound.
STOP_TIMEOUT_S = 20
# How long a started unit may take to expose a MainPID.
_UNIT_PID_WAIT_S = 10.0

_SYSTEMCTL_TIMEOUT_S = 30


def _unit_name(worker_name: str) -> str:
    return f"{UNIT_PREFIX}{worker_name}.service"


def _run_systemctl(args: List[str], timeout: float = _SYSTEMCTL_TIMEOUT_S):
    return subprocess.run(
        args, capture_output=True, text=True, timeout=timeout,
    )


def _systemd_available() -> bool:
    """Whether the per-user systemd manager can own worker services."""
    try:
        proc = _run_systemctl(
            ["systemctl", "--user", "is-system-running"], timeout=10
        )
        return (proc.stdout or "").strip() in ("running", "degraded")
    except Exception:
        return False


def _linger_enabled() -> Optional[bool]:
    """Whether the user session lingers (units survive logout). None when
    undeterminable. Advisory: surfaced as a warning, never a hard fail."""
    try:
        proc = _run_systemctl(
            ["loginctl", "show-user", os.environ.get("USER") or str(os.getuid()),
             "--property=Linger"], timeout=10,
        )
        out = (proc.stdout or "").strip()
        if out.startswith("Linger="):
            return out == "Linger=yes"
    except Exception:
        pass
    return None


def _systemd_run_command(
    unit: str, argv: List[str], env: Dict[str, str], cwd: str, log_path: Path
) -> List[str]:
    """The exact ``systemd-run`` invocation for one worker generation
    (pure construction — tested directly): KillMode=control-group +
    TimeoutStopSec give bounded full-cgroup teardown; Restart=no keeps
    respawn decisions in this controller; --collect garbage-collects
    failed transient units; stdout/stderr append to ``log_path`` so the
    log contract matches the detached path; only the allowlisted env
    keys cross into the unit."""
    cmd = [
        "systemd-run", "--user", f"--unit={unit}", "--collect",
        "--service-type=exec",
        "--property=KillMode=control-group",
        f"--property=TimeoutStopSec={STOP_TIMEOUT_S}",
        "--property=Restart=no",
        f"--property=StandardOutput=append:{log_path}",
        f"--property=StandardError=append:{log_path}",
        f"--working-directory={cwd}",
    ]
    for key in ("CURSOR_DATA_DIR", "PATH", "HOME"):
        if key in env:
            cmd.append(f"--setenv={key}={env[key]}")
    return cmd + ["--", *argv]


def _systemd_start(
    unit: str, argv: List[str], env: Dict[str, str], cwd: str, log_path: Path
) -> None:
    """Start ``argv`` as a transient user service (no unit file).

    The user manager forks the worker — zero process-tree coupling to
    the gateway (see :func:`_systemd_run_command` for the exact unit
    properties)."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    proc = _run_systemctl(_systemd_run_command(unit, argv, env, cwd, log_path))
    if proc.returncode != 0:
        raise WorkerError(
            f"systemd-run failed for unit {unit} (rc {proc.returncode}): "
            f"{(proc.stderr or proc.stdout or '').strip()[:500]}"
        )


def _systemd_show(unit: str) -> Dict[str, str]:
    """{ActiveState, SubState, MainPID, InvocationID} for a unit ({} on
    failure)."""
    try:
        proc = _run_systemctl([
            "systemctl", "--user", "show", unit,
            "--property=ActiveState,SubState,MainPID,InvocationID",
        ], timeout=10)
        out: Dict[str, str] = {}
        for line in (proc.stdout or "").splitlines():
            key, sep, value = line.partition("=")
            if sep:
                out[key.strip()] = value.strip()
        return out
    except Exception:
        return {}


def _systemd_stop(unit: str) -> bool:
    """VERIFIED stop of a unit: run ``systemctl stop`` (blocks until the
    cgroup is empty or TimeoutStopSec escalates to SIGKILL), then
    confirm via ``show`` that the unit is genuinely gone/inactive.
    Returns False when the stop cannot be confirmed — callers must then
    fail closed (retain the record; never spawn a duplicate)."""
    try:
        proc = _run_systemctl(
            ["systemctl", "--user", "stop", unit],
            timeout=STOP_TIMEOUT_S + 15,
        )
        if proc.returncode != 0:
            stderr = (proc.stderr or "").lower()
            # "not loaded" means the unit does not exist — already gone.
            if "not loaded" not in stderr:
                logger.warning(
                    "systemctl stop %s failed (rc %d): %s",
                    unit, proc.returncode, (proc.stderr or "").strip()[:300],
                )
                return _unit_stopped(unit)
    except Exception:
        logger.warning("systemctl stop %s failed", unit, exc_info=True)
        return _unit_stopped(unit)
    try:
        _run_systemctl(["systemctl", "--user", "reset-failed", unit], timeout=10)
    except Exception:
        pass
    return _unit_stopped(unit)


def _unit_stopped(unit: str) -> bool:
    """Whether a unit is confirmed inactive/gone (its cgroup is empty —
    systemd only reports inactive/failed once every member exited)."""
    state = str(_systemd_show(unit).get("ActiveState") or "")
    return state in ("", "inactive", "failed", "dead", "not-found")


# ---------------------------------------------------------------------------
# Isolation: per-worker data root + management endpoint
# ---------------------------------------------------------------------------

# Management ports: deterministic per-name base inside this range, linear
# probe past squatters. Localhost only — the endpoint is unauthenticated.
_MGMT_PORT_BASE = 42600
_MGMT_PORT_RANGE = 300


def _data_dir_for(name: str) -> Path:
    return state_dir() / "data" / name


def _alloc_management_port(name: str) -> int:
    """A currently-bindable localhost port, deterministically seeded by
    the worker name so restarts tend to reuse the same port.

    Ports reserved by OTHER live worker records are skipped outright —
    together with the machine-global admission lock this closes the
    probe-then-bind race between two concurrent spawns (a not-yet-bound
    winner's port is already visible in its record)."""
    reserved = set()
    for record in live_workers():
        if record.name != name and ":" in (record.management_addr or ""):
            try:
                reserved.add(int(record.management_addr.rsplit(":", 1)[1]))
            except (TypeError, ValueError):
                pass
    base = _MGMT_PORT_BASE + (
        int(hashlib.sha256(name.encode("utf-8")).hexdigest(), 16)
        % _MGMT_PORT_RANGE
    )
    for offset in range(_MGMT_PORT_RANGE):
        port = _MGMT_PORT_BASE + (
            (base - _MGMT_PORT_BASE + offset) % _MGMT_PORT_RANGE
        )
        if port in reserved:
            continue
        try:
            probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                probe.bind(("127.0.0.1", port))
            finally:
                probe.close()
            return port
        except OSError:
            continue
    raise WorkerError(
        f"no free management port in {_MGMT_PORT_BASE}-"
        f"{_MGMT_PORT_BASE + _MGMT_PORT_RANGE - 1} for worker '{name}'"
    )


def _spawn_worker(
    name: str,
    repo_path: str,
    log_path: Path,
    data_dir: str = "",
    management_addr: str = "",
) -> int:
    """Start ``agent worker start`` detached (the DEGRADED fallback path
    — no user systemd), output to ``log_path``.

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
    argv, env = _spawn_command(
        cli, name, repo_path,
        data_dir=data_dir, management_addr=management_addr,
    )
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


@contextmanager
def _admission_lock() -> Iterator[None]:
    """SHORT machine-global admission mutex: capacity reservation,
    management-port reservation, spawn, and the authoritative record
    write form ONE critical section, so two distinct worktree spawns at
    limit-1 can never both pass (per-worker locks cannot see each
    other). The bounded ready-wait happens OUTSIDE this lock."""
    path = state_dir() / "locks" / "admission.lock"
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
    are cleaned up as they are discovered (lazy cleanup, under a
    NON-BLOCKING per-worker flock: a contended worker is someone else's
    business right now — callers may already hold their own worker's
    flock, and blocking here could deadlock two at-capacity spawners)."""
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
            with _try_worker_lock(record.name) as acquired:
                if not acquired:
                    continue
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
        if _ready_evidence(record) or _probe_ready(record):
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
    if Path(record.log_path).name.startswith(f"{record.name}-"):
        Path(record.log_path).unlink(missing_ok=True)


def _unit_identity_matches(record: WorkerRecord) -> bool:
    """Adoption validation for systemd records: the unit's live
    ActiveState/MainPID/InvocationID must match the persisted identity.
    A mismatch means the unit is NOT the generation this record owns —
    it is never adopted (the caller respawns through the verified-stop
    path)."""
    if record.supervision != "systemd" or not record.unit:
        return True
    show = _systemd_show(record.unit)
    if str(show.get("ActiveState") or "") != "active":
        return False
    if str(show.get("MainPID") or "0") != str(record.pid):
        return False
    invocation = str(show.get("InvocationID") or "")
    if invocation and record.generation and invocation != record.generation:
        return False
    return True


def _validate_for_handout(record: WorkerRecord) -> Optional[WorkerRecord]:
    """Validate a live generation before handing it to a dispatch.

    Three distinct facts, checked in order (caller holds the flock):

    * **unit identity** (systemd records): live ActiveState/MainPID/
      InvocationID must match the persisted record — else None (respawn).
    * **registration**: when the management endpoint answers, its
      ``connected`` field is authoritative. connected=True hands out
      (``claimed`` is busy-ness, NOT ill health — preserved distinction);
      connected=False on a READY record means the worker lost its
      backend registration and cannot receive assignments — None
      (respawn); connected=False on a SPAWNING record is still
      registering — actionable retry error, not churn.
    * **fallback evidence** (endpoint silent/absent): a READY record
      stands on process liveness; a SPAWNING record must show its own
      generation's log evidence or fail with the retry error.

    Returns the (possibly state-flipped) record to hand out, or None
    when the generation must be replaced.
    """
    if not _unit_identity_matches(record):
        logger.warning(
            "worker %s: unit identity mismatch (unit %s no longer matches "
            "pid %d / generation %s) — not adopting",
            record.name, record.unit or "-", record.pid, record.generation,
        )
        return None
    probe = _probe_management(record)
    if probe is not None:
        if probe.get("connected"):
            if record.state != STATE_READY:
                _update_record_locked(
                    record.name, record.generation,
                    state=STATE_READY, last_error="",
                )
                return _read_record(record.name) or record
            return record
        if record.state == STATE_READY:
            logger.warning(
                "worker %s: management endpoint reports connected=false — "
                "the worker lost backend registration; replacing it",
                record.name,
            )
            return None
    else:
        if record.state == STATE_READY:
            return record
    if _ready_evidence(record):
        _update_record_locked(
            record.name, record.generation,
            state=STATE_READY, last_error="",
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


def ensure_worker(
    repo_path: str,
    lease_id: Optional[str] = None,
    lease_session: str = "",
) -> WorkerRecord:
    """The live worker serving ``repo_path``, spawning one when needed —
    and, when ``lease_id`` is given, LEASED atomically under the same
    ownership lock (the reaper can never win an ensure→lease window,
    and a caller that cannot be granted the lease gets an exception,
    never an unprotected worker).

    Serialized cross-process by the per-worker flock; capacity + port
    reservation + spawn + the authoritative record write additionally
    hold the short machine-global admission lock. Reuse first (the
    common case, and the second-worker trap avoider): a live managed
    worker for this worktree is handed out when READY — a live
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
            usable: Optional[WorkerRecord] = None
            if _record_alive(record) and record.state not in (
                STATE_FAILED, STATE_DRAINING,
            ):
                usable = _validate_for_handout(record)  # may raise (retry)
            if usable is not None:
                # Bump the idle clock at hand-out: the caller is about
                # to use this worker.
                _update_record_locked(
                    name, usable.generation, last_active_at=time.time(),
                )
                usable = _read_record(name) or usable
                return _install_lease_locked(usable, lease_id, lease_session)
            logger.info(
                "worker %s (pid %d, state %s) is not adoptable — replacing",
                name, record.pid, record.state,
            )
            if not _stop_generation(record):
                _update_record_locked(
                    name, record.generation,
                    state=STATE_DRAINING,
                    last_error="stop unverified before respawn",
                )
                raise WorkerError(
                    f"worker '{name}' could not be confirmed stopped — "
                    "refusing to spawn a duplicate on its data root; "
                    "retry shortly"
                )
            _cleanup_generation(record)
        # Admission: capacity reservation, management-port reservation,
        # spawn, and the record write are ONE machine-global critical
        # section (two distinct worktree spawns at limit-1 must not both
        # pass). The bounded ready-wait below runs outside it.
        with _admission_lock():
            _enforce_capacity(name, real)
            record = _launch_generation(name, real)
        _await_generation_ready(record)
        record = _read_record(name) or record
        return _install_lease_locked(record, lease_id, lease_session)


def _install_lease_locked(
    record: WorkerRecord, lease_id: Optional[str], lease_session: str
) -> WorkerRecord:
    """Install the caller's per-run lease (caller holds the flock).
    No-op without a lease_id. Raises when the fenced write is refused —
    the caller must never proceed with an unprotected worker."""
    if not lease_id:
        return record
    pid = os.getpid()
    leases = dict(record.leases or {})
    leases[str(lease_id)] = {
        "session": str(lease_session or ""),
        "holder_pid": pid,
        "holder_birth": _pid_birth(pid),
        "acquired_at": time.time(),
        "agent_id": "",
        "run_id": "",
    }
    if not _update_record_locked(
        record.name, record.generation,
        leases=leases, last_active_at=time.time(),
    ):
        raise WorkerError(
            f"worker '{record.name}' changed generation while being "
            "leased — retry the dispatch"
        )
    return _read_record(record.name) or record


def _launch_generation(name: str, real: str) -> WorkerRecord:
    """Start a fresh generation and write its authoritative record
    (caller holds the flock AND the admission lock).

    Isolation: a deterministic per-worker ``CURSOR_DATA_DIR`` (the CLI's
    worker.lock lives inside it, so no two worktrees ever contend) and a
    per-worker localhost management endpoint. Supervision: a transient
    user systemd service when the user manager is available (gateway-
    independent lifetime, MainPID/InvocationID identity, bounded full-
    cgroup stop), else the clearly-surfaced degraded detached fallback.
    Each generation logs to its OWN file (``<name>-<gen>.log``) so
    readiness evidence is structurally generation-scoped.
    """
    minted = _mint_generation()
    log_path = state_dir() / f"{name}-{minted}.log"
    data_dir = _data_dir_for(name)
    data_dir.mkdir(parents=True, exist_ok=True)
    addr = f"127.0.0.1:{_alloc_management_port(name)}"

    if _systemd_available():
        supervision = "systemd"
        unit = _unit_name(name)
        pid, generation = _spawn_unit(unit, name, real, log_path, data_dir, addr)
    else:
        supervision = "detached"
        unit = ""
        generation = minted
        logger.warning(
            "worker %s: user systemd unavailable — DEGRADED detached "
            "supervision (no cgroup containment; stop is a bounded "
            "process-group kill)", name,
        )
        pid = _spawn_worker(
            name, real, log_path,
            data_dir=str(data_dir), management_addr=addr,
        )

    record = WorkerRecord(
        name=name,
        repo_path=real,
        pid=pid,
        log_path=str(log_path),
        started_at=time.time(),
        verified=False,
        generation=generation,
        unit=unit,
        supervision=supervision,
        data_dir=str(data_dir),
        management_addr=addr,
        state=STATE_SPAWNING,
        pid_birth=_pid_birth(pid),
        last_active_at=time.time(),
    )
    _write_record(record)
    logger.info(
        "spawned worker %s (pid %d, generation %s, %s) for %s",
        name, pid, generation, supervision, real,
    )
    return record


def _await_generation_ready(record: WorkerRecord) -> None:
    """Bounded ready-wait for a just-launched generation (caller holds
    the flock, NOT the admission lock); persists the outcome."""
    try:
        _wait_ready(record)
    except WorkerError as exc:
        state = STATE_FAILED if not _pid_alive(record.pid) else STATE_SPAWNING
        _update_record_locked(
            record.name, record.generation,
            state=state, last_error=str(exc)[:500],
        )
        raise
    _update_record_locked(
        record.name, record.generation, state=STATE_READY, last_error="",
    )


def _spawn_unit(
    unit: str,
    name: str,
    real: str,
    log_path: Path,
    data_dir: Path,
    addr: str,
) -> tuple:
    """Start one transient service and resolve its (MainPID,
    InvocationID) identity. Caller holds the flock."""
    cli = _agent_cli_path()
    if not cli:
        raise WorkerError(
            f"the '{AGENT_CLI}' CLI is not on PATH — install the cursor "
            "agent CLI (it provides `agent worker start`) or use "
            "runtime='cloud'"
        )
    if _linger_enabled() is False:
        logger.warning(
            "user lingering is disabled (loginctl enable-linger %s) — "
            "worker services will die at logout",
            os.environ.get("USER") or os.getuid(),
        )
    # A remnant unit with no live record is unmanaged (the record is the
    # authority and it is dead/absent here) — clear it so the fresh
    # generation's name is free and no shadow worker swallows routing.
    # The stop is VERIFIED: an unconfirmed remnant means fail closed
    # (never start a duplicate next to an unkillable shadow).
    show = _systemd_show(unit)
    if show.get("ActiveState") in ("active", "activating", "deactivating"):
        logger.warning(
            "stopping unmanaged remnant unit %s (record was dead/absent)",
            unit,
        )
    if not _systemd_stop(unit):
        raise WorkerError(
            f"remnant unit {unit} could not be confirmed stopped — "
            "refusing to spawn a duplicate worker; inspect it with "
            f"`systemctl --user status {unit}`"
        )
    argv, env = _spawn_command(
        cli, name, real, data_dir=str(data_dir), management_addr=addr
    )
    _systemd_start(unit, argv, env, cwd=real, log_path=log_path)
    deadline = time.monotonic() + _UNIT_PID_WAIT_S
    while True:
        show = _systemd_show(unit)
        pid = int(show.get("MainPID") or 0)
        if pid > 0:
            return pid, str(show.get("InvocationID") or "") or _mint_generation("unit")
        if show.get("ActiveState") in ("failed", "inactive") or (
            time.monotonic() >= deadline
        ):
            raise WorkerError(
                f"unit {unit} started but exposed no MainPID "
                f"(ActiveState={show.get('ActiveState')!r}) — log tail:\n"
                f"{_log_tail(log_path) or '(empty log)'}"
            )
        time.sleep(0.1)


def _stop_generation(record: WorkerRecord) -> bool:
    """VERIFIED bounded teardown of a generation's whole process tree
    (caller holds the flock). Returns False when the teardown could not
    be confirmed — callers fail closed (record retained as draining).

    systemd records ALWAYS stop through the unit — even when the leader
    pid is already dead, the unit's cgroup may still hold descendants
    (the observed leaked-tmux-child scope); ``systemctl stop`` empties
    the cgroup within TimeoutStopSec and the result is verified against
    the unit's post-stop state. Detached records degrade to a bounded
    TERM→KILL of the process group verified against the leader pid.
    """
    if record.supervision == "systemd" and record.unit:
        return _systemd_stop(record.unit)
    if not _record_alive(record):
        return True
    _kill_detached(record)
    return not _record_alive(record)


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


# ---------------------------------------------------------------------------
# Per-RUN leases + idle reaping + capacity
# ---------------------------------------------------------------------------

# A worker with no fresh lease for this long is reaped by reconcile().
IDLE_TTL_S = 1800.0
# An UNBOUND lease (dispatch acquired it, but no agent/run identity was
# ever bound — the create failed, was lost, or is still in flight) stops
# protecting the worker past this age even while its holder lives:
# binding happens within one create round-trip, and intent recovery
# resolves lost creates well inside this window.
UNBOUND_LEASE_MAX_AGE_S = 600.0
# An unbound lease whose HOLDER process died keeps protecting the worker
# this long past acquisition (the settle window of a crashed dispatcher).
LEASE_STALE_GRACE_S = 300.0


def _plugin_config(key: str) -> Any:
    """A ``plugins.ghost_cursor.<key>`` config.yaml value, or None."""
    try:
        from hermes_cli.config import cfg_get, read_raw_config

        return cfg_get(read_raw_config(), "plugins", "ghost_cursor", key)
    except Exception:
        return None


def _max_workers() -> int:
    """The worker cap for this box: ``plugins.ghost_cursor.max_workers``
    in config.yaml, else 10 (Cursor's documented per-user self-hosted
    quota). Robust to junk values."""
    try:
        value = int(_plugin_config("max_workers"))
        if value >= 1:
            return value
    except (TypeError, ValueError):
        pass
    return 10


def _idle_ttl_s() -> float:
    """The idle reap TTL: ``plugins.ghost_cursor.worker_idle_ttl_s`` in
    config.yaml, else :data:`IDLE_TTL_S`. Robust to junk values."""
    try:
        value = float(_plugin_config("worker_idle_ttl_s"))
        if value > 0:
            return value
    except (TypeError, ValueError):
        pass
    return IDLE_TTL_S


def _holder_alive(lease: Dict[str, Any]) -> bool:
    try:
        pid = int(lease.get("holder_pid") or 0)
    except (TypeError, ValueError):
        return False
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        pass
    birth = lease.get("holder_birth")
    if birth is None:
        return True
    return _pid_birth(pid) in (None, int(birth))


def _lease_fresh(lease: Dict[str, Any], now: float) -> bool:
    """Whether one lease still protects its worker.

    A lease BOUND to real agent/run identity protects the worker until
    an OBSERVED remote terminal releases it (supervisor settle or the
    reconciler's remote probe) — never a timer, and holder death is
    irrelevant (the run lives server-side; a restarted gateway must find
    the worker still protected). An UNBOUND dispatch lease is
    transitional: it expires ``UNBOUND_LEASE_MAX_AGE_S`` after
    acquisition even with a live holder (a create that never bound
    within that window failed or was lost — intent recovery owns it),
    and ``LEASE_STALE_GRACE_S`` after a dead holder.
    """
    if str(lease.get("agent_id") or "") and str(lease.get("run_id") or ""):
        return True
    try:
        acquired = float(lease.get("acquired_at") or 0.0)
    except (TypeError, ValueError):
        acquired = 0.0
    if _holder_alive(lease):
        return now - acquired < UNBOUND_LEASE_MAX_AGE_S
    return now - acquired < LEASE_STALE_GRACE_S


def _fresh_leases(record: WorkerRecord, now: float) -> Dict[str, Dict[str, Any]]:
    return {
        key: lease for key, lease in (record.leases or {}).items()
        if isinstance(lease, dict) and _lease_fresh(lease, now)
    }


def acquire_lease(name: str, lease_id: str, session: str = "") -> bool:
    """Lease the worker for one RUN (never a whole conversation).

    Prefer the atomic path — ``ensure_worker(repo, lease_id=...)`` —
    which installs the lease under the same ownership lock as the
    hand-out. This standalone variant exists for callers that already
    hold a record. Returns False when no record exists.
    """
    name, lease_id = str(name), str(lease_id)
    if not name or not lease_id:
        return False
    with _worker_lock(name):
        record = _read_record(name)
        if record is None:
            return False
        pid = os.getpid()
        leases = dict(record.leases or {})
        leases[lease_id] = {
            "session": str(session or ""),
            "holder_pid": pid,
            "holder_birth": _pid_birth(pid),
            "acquired_at": time.time(),
            "agent_id": "",
            "run_id": "",
        }
        return _update_record_locked(
            name, record.generation,
            leases=leases, last_active_at=time.time(),
        )


def bind_lease(name: str, lease_id: str, agent_id: str, run_id: str) -> None:
    """Bind a dispatch lease to its real remote identity (called at the
    producer boundary, the instant the create/follow-up returned).

    From here the lease protects the worker until an OBSERVED remote
    terminal — across gateway restarts and stream failures. Never
    raises."""
    try:
        with _worker_lock(str(name)):
            record = _read_record(str(name))
            if record is None or str(lease_id) not in (record.leases or {}):
                return
            leases = dict(record.leases)
            leases[str(lease_id)] = {
                **leases[str(lease_id)],
                "agent_id": str(agent_id or ""),
                "run_id": str(run_id or ""),
            }
            _update_record_locked(
                str(name), record.generation,
                leases=leases, last_active_at=time.time(),
            )
    except Exception:
        logger.warning("bind_lease(%s, %s) failed", name, lease_id, exc_info=True)


def release_leases_for_session(session_name: str) -> None:
    """Release every lease held for one session — called by whoever just
    OBSERVED the session's remote run reach a terminal state (the
    re-attached supervisor's settle, or the reconciler's remote probe).
    Never raises."""
    try:
        session_name = str(session_name or "")
        if not session_name:
            return
        for record in live_workers():
            if not any(
                str((lease or {}).get("session") or "") == session_name
                for lease in (record.leases or {}).values()
            ):
                continue
            with _worker_lock(record.name):
                current = _read_record(record.name)
                if current is None:
                    continue
                kept = {
                    key: lease for key, lease in (current.leases or {}).items()
                    if str((lease or {}).get("session") or "") != session_name
                }
                if kept != (current.leases or {}):
                    _update_record_locked(
                        current.name, current.generation,
                        leases=kept, last_active_at=time.time(),
                    )
    except Exception:
        logger.warning(
            "release_leases_for_session(%s) failed", session_name, exc_info=True
        )


def bound_leases() -> List[Dict[str, Any]]:
    """Every live worker's leases that are BOUND to remote identity —
    the reconciler probes these against the GET authority and releases
    the settled ones. Items: {worker, lease_id, agent_id, run_id,
    session}. Never raises."""
    out: List[Dict[str, Any]] = []
    try:
        for record in live_workers():
            for lease_id, lease in (record.leases or {}).items():
                agent_id = str((lease or {}).get("agent_id") or "")
                run_id = str((lease or {}).get("run_id") or "")
                if agent_id and run_id:
                    out.append({
                        "worker": record.name,
                        "lease_id": lease_id,
                        "agent_id": agent_id,
                        "run_id": run_id,
                        "session": str((lease or {}).get("session") or ""),
                    })
    except Exception:
        logger.warning("bound_leases scan failed", exc_info=True)
    return out


def release_lease(name: str, lease_id: str) -> None:
    """Release one run's lease (idempotent, never raises). Bumps the
    idle clock so a just-finished worker gets a full TTL."""
    try:
        name, lease_id = str(name), str(lease_id)
        if not name or not lease_id:
            return
        with _worker_lock(name):
            record = _read_record(name)
            if record is None or lease_id not in (record.leases or {}):
                return
            leases = dict(record.leases)
            leases.pop(lease_id, None)
            _update_record_locked(
                name, record.generation,
                leases=leases, last_active_at=time.time(),
            )
    except Exception:
        logger.warning("release_lease(%s, %s) failed", name, lease_id, exc_info=True)


def _evict_locked(record: WorkerRecord, reason: str) -> bool:
    """Stop + retire one generation (caller holds its flock).

    The stop is VERIFIED before the record is deleted; a stop that
    cannot be confirmed retains the record as ``draining`` with the
    failure recorded — never an untracked remnant. Returns whether the
    generation was actually retired."""
    logger.info(
        "evicting worker %s (pid %d, generation %s): %s",
        record.name, record.pid, record.generation, reason,
    )
    if not _stop_generation(record):
        logger.warning(
            "worker %s: stop could not be verified — retaining the record "
            "as draining (fail closed)", record.name,
        )
        _update_record_locked(
            record.name, record.generation,
            state=STATE_DRAINING,
            last_error=f"stop unverified during eviction ({reason})"[:500],
        )
        return False
    _cleanup_generation(record)
    return True


def reconcile(now: Optional[float] = None) -> List[str]:
    """One reaper pass: retire dead generations and reap IDLE (leaseless
    past :data:`IDLE_TTL_S`) workers with a bounded full-tree stop.
    A leased worker is never touched. Returns the reaped names.
    Never raises (a broken worker must not break the caller's tick)."""
    reaped: List[str] = []
    now = time.time() if now is None else now
    directory = state_dir()
    if not directory.is_dir():
        return reaped
    for path in sorted(directory.glob("*.json")):
        try:
            with _try_worker_lock(path.stem) as acquired:
                if not acquired:
                    continue  # someone is actively touching it — next tick
                record = _read_record(path.stem)
                if record is None:
                    continue
                if not _record_alive(record):
                    if _evict_locked(record, "process dead"):
                        reaped.append(record.name)
                    continue
                if record.supervision == "legacy":
                    # A generation-0 worker may be serving a pre-v2 run
                    # this controller cannot see (no lease protocol,
                    # possibly another profile's session). Never TTL-reap
                    # it; it retires at process death or respawn.
                    continue
                fresh = _fresh_leases(record, now)
                if fresh != (record.leases or {}):
                    # Trim expired leases so the map cannot grow forever.
                    _update_record_locked(
                        record.name, record.generation, leases=fresh,
                    )
                if fresh:
                    continue
                ttl = _idle_ttl_s()
                if now - float(record.last_active_at or record.started_at) > ttl:
                    if _evict_locked(record, f"idle past {int(ttl)}s TTL"):
                        reaped.append(record.name)
        except Exception:
            logger.warning(
                "reconcile pass failed for %s", path.stem, exc_info=True
            )
    return reaped


@contextmanager
def _try_worker_lock(name: str) -> Iterator[bool]:
    """Non-blocking flock variant for capacity eviction: a contended
    worker is skipped (someone is actively touching it), never waited on
    — two at-capacity spawners can never deadlock on each other."""
    path = state_dir() / "locks" / f"{name}.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o644)
    acquired = False
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except OSError:
            pass
        yield acquired
    finally:
        try:
            if acquired:
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def _enforce_capacity(spawning_name: str, real: str) -> None:
    """Make room for a new worker under the cap, or fail honestly.

    Reclaims LEASELESS workers (least-recently-active first, regardless
    of TTL — capacity pressure beats idle patience) until the new spawn
    fits. Never evicts a leased worker, never queues, never silently
    serializes: when every slot is held by a leased worker this raises
    a WorkerError naming them.
    """
    limit = _max_workers()
    now = time.time()
    others = [r for r in live_workers() if r.name != spawning_name]
    if len(others) < limit:
        return
    idle = sorted(
        (
            r for r in others
            # Legacy workers may serve runs this controller cannot see —
            # they are never reclaim candidates (they retire at death).
            if r.supervision != "legacy" and not _fresh_leases(r, now)
        ),
        key=lambda r: float(r.last_active_at or r.started_at),
    )
    need = len(others) - limit + 1
    for record in idle:
        if need <= 0:
            break
        with _try_worker_lock(record.name) as acquired:
            if not acquired:
                continue  # contended — someone is using it; skip
            current = _read_record(record.name)
            if (
                current is None
                or current.generation != record.generation
                or _fresh_leases(current, time.time())
            ):
                continue  # changed under us — no longer safe to evict
            if _evict_locked(current, "capacity reclaim"):
                need -= 1
    if need > 0:
        busy = sorted(
            f"{r.name} ({r.repo_path}"
            + (", legacy" if r.supervision == "legacy" else "")
            + ")"
            for r in live_workers()
            if r.name != spawning_name
            and (r.supervision == "legacy" or _fresh_leases(r, time.time()))
        )
        raise WorkerError(
            f"worker capacity exhausted: {len(busy)} of {limit} workers "
            f"are protected (active run lease or legacy) and none can be "
            f"reclaimed for {real} — busy: "
            f"{', '.join(busy) or '(none visible)'}. Wait for a run to "
            "finish, stop one (cursor_stop), or raise "
            "plugins.ghost_cursor.max_workers in config.yaml if your "
            "Cursor plan allows more self-hosted workers."
        )


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
