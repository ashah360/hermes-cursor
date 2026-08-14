""""My Machines" worker controller for runtime="local" sessions.

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
  ``CURSOR_API_KEY`` reaches a supervised worker ONLY through a 0600
  environment file referenced by path (never argv, never logs); the
  detached fallback passes it via the subprocess environment.
* **Supervision** — a deterministic transient user systemd SERVICE per
  worker (``cursor-worker-<name>.service``): gateway-independent
  lifetime, authoritative MainPID + InvocationID (the record's
  generation), bounded full-cgroup TERM→KILL stop, adoption across
  restarts. Without a user manager the spawn DEGRADES (clearly surfaced)
  to a detached process with bounded process-group teardown.
* **Generation-fenced records** — versioned ``<name>.json`` under the
  MACHINE-global control dir (``$XDG_STATE_HOME/ghost_cursor/workers``,
  independent of the Hermes profile); every mutation is serialized by a
  per-worker flock and fenced by generation, so a losing/stale
  generation can never overwrite the authoritative record. Pre-v2
  profile-local records are adopted (never killed); records named for a
  repo SUBDIRECTORY are re-keyed by canonical-alias adoption; multiple
  live candidates for one canonical worktree FAIL CLOSED as an explicit
  conflict (no winner, no stop, no spawn). Readiness evidence is
  generation-scoped (per-generation log + management probe).
* **Per-RUN leases** — a run leases its worker atomically at ensure,
  binds the lease to the real agent/run identity right after the create
  returned, and the lease is released only on OBSERVED remote-terminal
  proof (``cloud_runner``'s GET settle authority). :func:`reconcile`
  (lazy: plugin init + every ensure) reaps only LEASELESS workers past
  :data:`IDLE_TTL_S`. At the cap (:func:`_max_workers`) idle leaseless
  workers are reclaimed first; an all-leased fleet raises an honest
  capacity error.

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
from typing import Any, Dict, Iterable, Iterator, List, Optional

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
# The deterministic unit's live identity does not match this record's
# generation — it may belong to another generation/controller. Never
# adopted, never stopped by us; retained for operator/reconciler
# resolution (it self-heals once the foreign unit is gone).
STATE_CONFLICT = "conflict"


@dataclass
class WorkerRecord:
    """One managed worker (the persisted, versioned ``<name>.json`` shape).

    v2 adds durable supervision identity: ``generation`` fences every
    record write (a losing/stale generation can never overwrite the
    authoritative record), ``unit``/``supervision`` carry the systemd
    transient-service identity (or the degraded detached fallback),
    ``data_dir`` is the worker's isolated ``CURSOR_DATA_DIR``,
    ``management_addr`` the CLI's health/readiness endpoint, ``pid_birth``
    the /proc starttime pid-reuse fence (readiness evidence is
    generation-scoped structurally: each generation logs to its own
    file), and ``leases`` the per-RUN leases that protect the worker from
    the idle reaper. A v1 record (no ``version``) is adopted on read —
    never killed — as supervision="legacy".
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
    last_active_at: float = 0.0
    last_error: str = ""
    leases: Dict[str, Dict[str, Any]] = field(default_factory=dict)


def _profile_state_dir() -> Path:
    """The current profile's pre-v2 worker-state location."""
    try:
        from hermes_constants import get_hermes_home

        home = Path(get_hermes_home())
    except Exception:
        home = Path(os.environ.get("HERMES_HOME", "") or (Path.home() / ".hermes"))
    return home / "state" / "ghost_cursor" / "workers"


def _profile_state_dirs() -> List[Path]:
    """EVERY discoverable pre-v2 profile-local worker-state location:
    the current profile, the default profile, and each named profile
    under ``<root>/profiles/*`` — worker identity is machine-global, so
    any profile's records are adoption candidates. Never raises."""
    roots: List[Path] = []

    def _add_root(root: Path) -> None:
        try:
            root = Path(root)
        except Exception:
            return
        if root not in roots:
            roots.append(root)
        # A root that IS a named profile sits under <base>/profiles/<x>:
        # its base (the default profile) and sibling profiles count too.
        if root.parent.name == "profiles":
            _add_root(root.parent.parent)

    try:
        from hermes_constants import get_hermes_home

        _add_root(Path(get_hermes_home()))
    except Exception:
        pass
    env_home = os.environ.get("HERMES_HOME", "").strip()
    if env_home:
        _add_root(Path(env_home))
    if not roots:
        _add_root(Path.home() / ".hermes")
    for root in list(roots):
        profiles = root / "profiles"
        try:
            if profiles.is_dir():
                for child in sorted(profiles.iterdir()):
                    if child.is_dir() and child not in roots:
                        roots.append(child)
        except Exception:
            continue
    return [root / "state" / "ghost_cursor" / "workers" for root in roots]


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


def _conflict_path(target: Path, name: str) -> Path:
    return target / f"{name}.conflict.json"


def _read_conflict_candidates(name: str) -> Optional[List[Dict[str, Any]]]:
    """The preserved ownership-evidence candidates for a conflicted
    worker (raw record dicts, each with a ``_source`` key), or None when
    the worker is not in a migration-collision conflict. Never raises."""
    try:
        path = _conflict_path(state_dir(), str(name))
        if not path.is_file():
            return None
        data = json.loads(path.read_text("utf-8"))
        candidates = data.get("candidates")
        return candidates if isinstance(candidates, list) else None
    except Exception:
        logger.warning("conflict evidence read failed for %s", name, exc_info=True)
        return None


def _note_migration_conflict(
    target: Path, name: str, incoming: Dict[str, Any], source: Path
) -> None:
    """Record an EXPLICIT machine-global collision: two profiles claim
    the same worker name with materially different records. No winner is
    picked — both candidates' full records are preserved as evidence,
    the authoritative record turns CONFLICT, and nothing is adopted,
    stopped, deleted, or replaced until resolution positively proves a
    candidate dead/invalid."""
    conflict_file = _conflict_path(target, name)
    try:
        existing = json.loads(conflict_file.read_text("utf-8"))
        candidates = existing.get("candidates") or []
    except Exception:
        candidates = []
    if not candidates:
        # First collision: the already-migrated record is candidate #1.
        try:
            current = json.loads((target / f"{name}.json").read_text("utf-8"))
            candidates.append({**current, "_source": "machine-global"})
        except Exception:
            pass
    key = (int(incoming.get("pid") or 0), str(incoming.get("generation") or ""))
    if key not in [
        (int(c.get("pid") or 0), str(c.get("generation") or ""))
        for c in candidates
    ]:
        candidates.append({**incoming, "_source": str(source)})
    conflict_file.write_text(json.dumps({
        "noted_at": time.time(),
        "candidates": candidates,
    }), "utf-8")
    # The authoritative record becomes an explicit conflict marker
    # (v2-shaped so the state survives the read path), keeping the
    # first-seen candidate's identity fields for diagnosis only.
    try:
        marker = _parse_record(candidates[0])
    except Exception:
        marker = _parse_record(incoming)
    _write_record(WorkerRecord(**{
        **asdict(marker),
        "state": STATE_CONFLICT,
        "last_error": (
            "migration collision: multiple profiles claim this worker "
            f"with different records (pids "
            f"{sorted(int(c.get('pid') or 0) for c in candidates)}) — "
            "no winner picked; see the .conflict.json evidence"
        ),
    }))
    logger.error(
        "worker %s: migration COLLISION across profiles (candidate pids "
        "%s) — conflict state recorded, nothing adopted or stopped",
        name, sorted(int(c.get("pid") or 0) for c in candidates),
    )


def _migrate_profile_records(target: Path) -> None:
    """Copy pre-v2 profile-local records — from EVERY discoverable
    profile — into the machine-global control dir (adoption: live
    workers and their deterministic units keep running untouched; the
    originals are left behind for any old plugin version still reading
    them).

    Collisions FAIL CLOSED: a second source claiming the same worker
    name with a materially different record (different pid/generation)
    never overwrites — it becomes an explicit CONFLICT with both
    candidates preserved as evidence (:func:`_note_migration_conflict`);
    an identical record is skipped silently. Never raises."""
    try:
        for source in _profile_state_dirs():
            try:
                if not source.is_dir() or source == target:
                    continue
                for path in sorted(source.glob("*.json")):
                    if path.name.endswith(".conflict.json"):
                        continue
                    dest = target / path.name
                    if dest.exists():
                        try:
                            incoming = json.loads(path.read_text("utf-8"))
                            current = json.loads(dest.read_text("utf-8"))
                        except Exception:
                            continue
                        same = (
                            int(incoming.get("pid") or 0)
                            == int(current.get("pid") or 0)
                            and str(incoming.get("generation") or "")
                            == str(current.get("generation") or "")
                        )
                        if not same:
                            # A DEAD incoming candidate is positively
                            # invalid — skip it (also prevents stale
                            # profile leftovers from re-conflicting a
                            # previously resolved worker forever).
                            try:
                                incoming_alive = _pid_alive(
                                    int(incoming.get("pid") or 0)
                                )
                            except (TypeError, ValueError):
                                incoming_alive = False
                            if incoming_alive:
                                _note_migration_conflict(
                                    target, path.stem, incoming, source
                                )
                        continue
                    target.mkdir(parents=True, exist_ok=True)
                    dest.write_text(path.read_text("utf-8"), "utf-8")
                    logger.info(
                        "migrated profile-local worker record %s (from %s) "
                        "to the machine-global control dir (adopted, not "
                        "respawned)", path.name, source,
                    )
            except Exception:
                logger.warning(
                    "worker record migration from %s failed", source,
                    exc_info=True,
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


def _auth_env_path(name: str) -> Path:
    return state_dir() / "secrets" / f"{name}.env"


def _write_auth_env(name: str, env: Dict[str, str]) -> str:
    """Persist the worker's ``CURSOR_API_KEY`` to a 0600 env file (0700
    dir) under the MACHINE-global state dir — never inside the worktree.
    Returns the file path, or "" when no key is present in ``env``.

    The transient service reads it via ``EnvironmentFile=`` so the
    secret is referenced ONLY by path: it never appears in systemd-run
    argv, unit Environment metadata, journal lines, or worker logs
    (verified with a live transient-unit probe on systemd 255).
    """
    key = str(env.get("CURSOR_API_KEY") or "")
    if not key:
        return ""
    path = _auth_env_path(name)
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    # systemd EnvironmentFile parsing: double-quoted value, no variable
    # expansion — only backslash and double-quote need escaping.
    escaped = key.replace("\\", "\\\\").replace('"', '\\"')
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as fh:
        fh.write(f'CURSOR_API_KEY="{escaped}"\n')
    os.chmod(path, 0o600)  # tighten a pre-existing file too
    return str(path)


def _systemd_run_command(
    unit: str,
    argv: List[str],
    env: Dict[str, str],
    cwd: str,
    log_path: Path,
    env_file: str = "",
) -> List[str]:
    """The exact ``systemd-run`` invocation for one worker generation
    (pure construction — tested directly): KillMode=control-group +
    TimeoutStopSec give bounded full-cgroup teardown; Restart=no keeps
    respawn decisions in this controller; --collect garbage-collects
    failed transient units; stdout/stderr append to ``log_path`` so the
    log contract matches the detached path.

    Only NON-SECRET allowlisted env keys cross via ``--setenv``. The
    ``CURSOR_API_KEY`` secret NEVER rides in this argv: when
    ``env_file`` is given, the unit reads it via
    ``EnvironmentFile=<path>`` — the command carries the file PATH only.
    """
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
    if env_file:
        cmd.append(f"--property=EnvironmentFile={env_file}")
    # The transient service has a fresh environment: hand over the
    # worker's isolated data root plus the basics the CLI needs.
    for key in ("CURSOR_DATA_DIR", "PATH", "HOME"):
        if key in env:
            cmd.append(f"--setenv={key}={env[key]}")
    return cmd + ["--", *argv]


def _systemd_start(
    unit: str,
    argv: List[str],
    env: Dict[str, str],
    cwd: str,
    log_path: Path,
    env_file: str = "",
) -> None:
    """Start ``argv`` as a transient user service (no unit file).

    The user manager forks the worker — zero process-tree coupling to
    the gateway (see :func:`_systemd_run_command` for the exact unit
    properties and the secret-handoff contract)."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    proc = _run_systemctl(
        _systemd_run_command(unit, argv, env, cwd, log_path, env_file=env_file)
    )
    if proc.returncode != 0:
        raise WorkerError(
            f"systemd-run failed for unit {unit} (rc {proc.returncode}): "
            f"{(proc.stderr or proc.stdout or '').strip()[:500]}"
        )


def _systemd_show(unit: str) -> Dict[str, str]:
    """{ActiveState, SubState, MainPID, InvocationID, LoadState} for a
    unit. {} means UNKNOWN (the query itself failed) — callers must
    treat that as no evidence, never as 'stopped'."""
    try:
        proc = _run_systemctl([
            "systemctl", "--user", "show", unit,
            "--property=ActiveState,SubState,MainPID,InvocationID,LoadState",
        ], timeout=10)
        if proc.returncode != 0:
            return {}
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
    """Whether a unit is confirmed stopped on POSITIVE evidence only:
    a successful ``show`` reporting not-found or inactive/failed/dead
    (systemd reports those only once every cgroup member exited). An
    empty/failed show is UNKNOWN — never counted as stopped."""
    show = _systemd_show(unit)
    if not show:
        return False  # UNKNOWN — no positive evidence
    if str(show.get("LoadState") or "") == "not-found":
        return True
    return str(show.get("ActiveState") or "") in ("inactive", "failed", "dead")


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
    The API key crosses via the subprocess ENVIRONMENT (inherited from
    the gateway's env) — never argv.
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

    A legacy record — written before this controller — is adopted as
    ``supervision="legacy"`` with a deterministic ``legacy-<pid>``
    generation and state "ready" (it WAS serving runs). It is never
    killed on read; it is replaced only when it dies or idles out.
    """
    path = _record_path(name)
    try:
        data = json.loads(path.read_text("utf-8"))
        return _parse_record(data)
    except FileNotFoundError:
        return None
    except Exception:
        logger.warning("unreadable worker record %s — removing", path, exc_info=True)
        path.unlink(missing_ok=True)
        return None


def _parse_record(data: Dict[str, Any]) -> WorkerRecord:
    """One record dict (v1 or v2 shape) parsed into a WorkerRecord.
    Raises on junk — callers decide the failure policy."""
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
        last_active_at=float(data.get("last_active_at") or 0.0),
        last_error=str(data.get("last_error") or ""),
        leases=dict(leases) if isinstance(leases, dict) else {},
    )


def _write_record(record: WorkerRecord) -> None:
    """Persist ``record`` atomically. Callers hold the worker's flock for
    any read-modify-write; new-generation writes happen inside
    ``ensure_worker``'s lock hold."""
    path = _record_path(record.name)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(asdict(record)), "utf-8")
    tmp.replace(path)


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
        if path.name.endswith(".conflict.json"):
            continue
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
                if (
                    current is not None
                    and not _record_alive(current)
                    # Conflicted records are never lazily cleaned: another
                    # candidate may be alive; resolution owns them.
                    and current.state != STATE_CONFLICT
                ):
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


def _log_head(log_path: Path, limit: int = 262_144) -> str:
    """The first ``limit`` bytes of a log (the CLI prints its readiness
    line at startup, so the head is where evidence lives)."""
    try:
        with log_path.open("rb") as fh:
            return fh.read(limit).decode("utf-8", errors="replace")
    except Exception:
        return ""


def _ready_evidence(record: WorkerRecord) -> bool:
    """Whether THIS generation has produced readiness evidence. Each
    generation logs to its own file, so scoping is structural."""
    return READY_LINE in _log_head(Path(record.log_path))


def _wait_ready(record: WorkerRecord) -> None:
    """Poll for THIS generation's readiness, bounded by READY_TIMEOUT_S.

    Evidence is generation-scoped structurally (each generation logs to
    its own file, and the management probe hits this generation's
    endpoint). Raises :class:`WorkerError` on startup death or timeout;
    the caller persists the failure onto the record.
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
                f"{_log_tail(log_path) or '(empty log)'}"
            )
        time.sleep(_READY_POLL_S)
    # The process is still alive but never reported ready. It is NOT
    # killed (it may finish registering late; the next ensure re-proves
    # readiness before reuse); this send fails actionably instead of
    # dispatching into the void.
    raise WorkerError(
        f"worker '{record.name}' did not report ready within "
        f"{int(READY_TIMEOUT_S)}s — log tail:\n"
        f"{_log_tail(log_path) or '(empty log)'}"
    )


def _cleanup_generation(record: WorkerRecord) -> None:
    """Remove a dead/finished generation's record and its per-generation
    log (caller holds the flock). Process-tree teardown for still-live
    remnants is the stop path (:func:`_stop_generation`); this only
    retires the durable state."""
    _record_path(record.name).unlink(missing_ok=True)
    _auth_env_path(record.name).unlink(missing_ok=True)
    # Only per-generation logs are removed; a legacy record's shared
    # <name>.log is left for post-mortems.
    if Path(record.log_path).name.startswith(f"{record.name}-"):
        Path(record.log_path).unlink(missing_ok=True)


def _unit_identity_matches(record: WorkerRecord) -> bool:
    """Adoption validation for systemd records: the unit's live
    ActiveState, MainPID, AND InvocationID must POSITIVELY match the
    persisted identity. A missing InvocationID is not valid fencing —
    identity that cannot be proven is not identity."""
    if record.supervision != "systemd" or not record.unit:
        return True
    show = _systemd_show(record.unit)
    if str(show.get("ActiveState") or "") != "active":
        return False
    if str(show.get("MainPID") or "0") != str(record.pid):
        return False
    invocation = str(show.get("InvocationID") or "")
    return bool(invocation) and invocation == record.generation


def _validate_for_handout(record: WorkerRecord) -> Optional[WorkerRecord]:
    """Validate a live, identity-verified generation before handing it
    to a dispatch (caller holds the flock; unit identity was already
    positively matched by the caller).

    Registration is the authority: when the management endpoint
    answers, its ``connected`` field decides — connected=True hands out
    (``claimed`` is busy-ness, NOT ill health — preserved distinction);
    connected=False on a READY record means lost backend registration —
    None (replace); connected=False on a SPAWNING record is still
    registering — actionable retry error, not churn.

    An UNREACHABLE/missing endpoint is NOT proof of connectivity: a
    READY v2 record fails the dispatch with an actionable retry (kept,
    not killed — the outage may be transient); only legacy generation-0
    records (which predate the endpoint) stand on process liveness. A
    SPAWNING record may still prove itself with its own generation's
    fresh log evidence (bootstrap) or fails with the retry error.
    """
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
        if record.supervision == "legacy":
            return record  # pre-endpoint generation — adoption promise
        if record.state == STATE_READY:
            raise WorkerError(
                f"worker '{record.name}' is alive but its management "
                f"endpoint ({record.management_addr or 'none recorded'}) "
                "is unreachable — not proven connected, not dispatching; "
                "retry shortly (the worker is kept), or stop it "
                "(cursor_stop) to force a respawn"
            )
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


def _live_canonical_aliases(name: str, real: str) -> List[WorkerRecord]:
    """LIVE records under OTHER names that serve the same canonical
    worktree ``real`` — the shape left behind by pre-canonical naming
    (a record keyed by a repo SUBDIRECTORY's realpath) or a hostname
    change. v2 records store canonical paths, so plain equality decides;
    legacy records may hold a subdirectory and are canonicalized."""
    out: List[WorkerRecord] = []
    for record in live_workers():
        if record.name == name or record.state == STATE_CONFLICT:
            continue
        path = str(record.repo_path or "")
        if path != real:
            if record.supervision != "legacy":
                continue
            if canonical_repo_path(path) != real:
                continue
        out.append(record)
    return out


def _adopt_alias_locked(
    alias: WorkerRecord, lease_id: Optional[str], lease_session: str
) -> WorkerRecord:
    """Adopt the SINGLE live worker that serves this canonical worktree
    under another name (caller holds the canonical name's flock): the
    record is reused in place — its registered routing name stays THE
    name — so the checkout never grows a second worker. The alias's own
    flock is taken non-blocking (contention = someone is actively
    touching it — retry, never deadlock). Nothing is ever stopped from
    this path; an unusable alias fails the dispatch actionably."""
    with _try_worker_lock(alias.name) as acquired:
        if not acquired:
            raise WorkerError(
                f"worker '{alias.name}' (already serving this checkout "
                "under its pre-canonical name) is busy being reconciled — "
                "retry shortly"
            )
        current = _read_record(alias.name)
        if current is None or not _record_alive(current):
            raise WorkerError(
                f"worker '{alias.name}' died while being adopted for this "
                "checkout — retry shortly"
            )
        if not _unit_identity_matches(current):
            raise WorkerError(
                f"worker '{alias.name}' serves this checkout but its unit "
                f"{current.unit or '-'} identity cannot be positively "
                "proven — not adopting and not stopping it; inspect with "
                f"`systemctl --user status {current.unit or ''}`"
            )
        usable = _validate_for_handout(current)  # may raise (retry)
        if usable is None:
            raise WorkerError(
                f"worker '{alias.name}' serves this checkout but lost its "
                "backend registration — stop it (cursor_stop) or wait for "
                "the idle reaper, then re-send"
            )
        _update_record_locked(
            alias.name, usable.generation, last_active_at=time.time(),
        )
        usable = _read_record(alias.name) or usable
        logger.info(
            "worker %s adopted for canonical worktree %s (pre-canonical "
            "record name kept for routing)", usable.name, usable.repo_path,
        )
        return _install_lease_locked(usable, lease_id, lease_session)


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

    Runs one lazy :func:`reconcile` pass first (skipping this worker),
    so idle leaseless generations are reaped BEFORE capacity is
    evaluated. Serialized cross-process by the per-worker flock;
    capacity + port reservation + spawn + the authoritative record
    write additionally hold the short machine-global admission lock.
    Reuse first (the common case, and the second-worker trap avoider):
    a live managed worker for this worktree is handed out when READY —
    a live generation that never proved readiness is re-proven before
    reuse, never trusted. A live worker recorded under a PRE-canonical
    name (subdirectory naming) is adopted as-is; MULTIPLE live
    candidates for one canonical worktree fail closed as an explicit
    conflict. A dead record is cleaned and replaced by a fresh
    generation whose record is only written once its process exists, so
    a losing spawner can never poison the winner's record. Raises
    :class:`WorkerError` when the spawn fails or never reports ready.
    """
    real = canonical_repo_path(repo_path)
    name = worker_name_for(real)
    # Lazy cleanup: the dispatch itself reaps idle leaseless workers so
    # capacity evaluation below never counts reapable generations.
    reconcile(skip=(name,))

    with _worker_lock(name):
        record = _read_record(name)
        if record is not None and record.state == STATE_CONFLICT:
            # Conflicted ownership resolves ONLY on positive proof (all
            # other candidates dead / the foreign unit gone) — raises an
            # actionable error otherwise; never stops a candidate.
            record = _resolve_conflict_or_raise(name, record)
        aliases = _live_canonical_aliases(name, real)
        if aliases:
            if record is not None and _record_alive(record):
                raise WorkerError(
                    f"multiple live workers claim this checkout ({real}): "
                    f"'{name}' and "
                    f"{', '.join(repr(a.name) for a in aliases)} — "
                    "refusing to pick a winner, stop any of them, or "
                    "spawn another (only one worker per checkout receives "
                    "assignments); stop the stale one manually, then "
                    "re-send"
                )
            if len(aliases) > 1:
                raise WorkerError(
                    f"multiple live workers claim this checkout ({real}): "
                    f"{', '.join(repr(a.name) for a in aliases)} — "
                    "refusing to pick a winner, stop any of them, or "
                    "spawn another; stop the stale one manually, then "
                    "re-send"
                )
            return _adopt_alias_locked(aliases[0], lease_id, lease_session)
        if record is not None:
            usable: Optional[WorkerRecord] = None
            if _record_alive(record) and record.state not in (
                STATE_FAILED, STATE_DRAINING, STATE_CONFLICT,
            ):
                if not _unit_identity_matches(record):
                    # The deterministic unit is live under a DIFFERENT
                    # (or unprovable) identity — it may belong to
                    # another generation/controller. Never adopt it,
                    # never stop it: fail closed for operator/reconciler
                    # resolution (self-heals once the unit is gone).
                    _update_record_locked(
                        name, record.generation,
                        state=STATE_CONFLICT,
                        last_error=(
                            f"unit {record.unit} live identity does not "
                            f"match generation {record.generation}"
                        ),
                    )
                    raise WorkerError(
                        f"worker '{name}' is in identity conflict: unit "
                        f"{record.unit} is running with an identity that "
                        "does not match the persisted generation — not "
                        "adopting and not stopping a possibly-foreign "
                        "unit; inspect it with `systemctl --user status "
                        f"{record.unit}`"
                    )
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


def _resolve_conflict_or_raise(
    name: str, record: WorkerRecord
) -> Optional[WorkerRecord]:
    """Resolve a CONFLICT record on positive proof only (caller holds
    the flock).

    Migration collisions (candidate evidence on file): while MORE than
    one candidate is alive, raise — no winner, nothing adopted/stopped/
    deleted. Exactly one live candidate = every other is positively
    dead: adopt the survivor. Zero live candidates: verified teardown of
    remnants, then clear (returns None so the caller may spawn fresh).

    Unit-identity conflicts (no candidate file): resolve only when our
    recorded process is gone AND the unit's teardown can be verified
    (the stop path itself refuses live units with unproven ownership).
    """
    candidates = _read_conflict_candidates(name)
    if candidates is None:
        if _record_alive(record) or not _stop_generation(record):
            raise WorkerError(
                f"worker '{name}' is in identity conflict: its unit "
                f"{record.unit or '-'} cannot be positively proven ours "
                "— not adopting and not stopping it; inspect with "
                f"`systemctl --user status {record.unit or ''}`"
            )
        _cleanup_generation(record)
        return None

    parsed: List[WorkerRecord] = []
    for candidate in candidates:
        try:
            parsed.append(_parse_record(dict(candidate)))
        except Exception:
            continue  # unparseable evidence is not a live claim
    alive = [c for c in parsed if _record_alive(c)]
    if len(alive) > 1:
        raise WorkerError(
            f"worker '{name}' has CONFLICTING ownership: "
            f"{len(alive)} live candidates from different profiles "
            f"(pids {sorted(c.pid for c in alive)}) — refusing to "
            "adopt, stop, or replace any of them; stop the stale one "
            "manually (see the .conflict.json evidence in the worker "
            "state dir), then re-send"
        )
    if len(alive) == 1:
        survivor = alive[0]
        _write_record(survivor)
        _conflict_path(state_dir(), name).unlink(missing_ok=True)
        logger.warning(
            "worker %s: ownership conflict RESOLVED — every other "
            "candidate is dead; adopted the surviving pid %d",
            name, survivor.pid,
        )
        return survivor
    # Zero live candidates: all positively dead — verified teardown of
    # whatever remnants the marker still points at, then a clean slate.
    if not _stop_generation(record):
        raise WorkerError(
            f"worker '{name}': all conflict candidates are dead but "
            "remnant teardown could not be verified — retry shortly"
        )
    _cleanup_generation(record)
    _conflict_path(state_dir(), name).unlink(missing_ok=True)
    return None


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
    # Secret handoff: the API key crosses through a 0600 env file the
    # unit reads by PATH (EnvironmentFile) — never through argv.
    env_file = _write_auth_env(name, env)
    _systemd_start(unit, argv, env, cwd=real, log_path=log_path, env_file=env_file)
    deadline = time.monotonic() + _UNIT_PID_WAIT_S
    while True:
        show = _systemd_show(unit)
        pid = int(show.get("MainPID") or 0)
        invocation = str(show.get("InvocationID") or "")
        if pid > 0 and invocation:
            # InvocationID is MANDATORY: it is the generation fence every
            # later adoption/stop decision verifies against — a unit we
            # cannot fence is a unit we cannot own.
            return pid, invocation
        if show.get("ActiveState") in ("failed", "inactive") or (
            time.monotonic() >= deadline
        ):
            # Our own just-started unit — stopping it is safe (no other
            # controller can have claimed this name inside our locks).
            _systemd_stop(unit)
            raise WorkerError(
                f"unit {unit} started but exposed no usable identity "
                f"(MainPID={pid}, InvocationID={invocation!r}, "
                f"ActiveState={show.get('ActiveState')!r}) — log tail:\n"
                f"{_log_tail(log_path) or '(empty log)'}"
            )
        time.sleep(0.1)


def _pgid_empty(pgid: int) -> bool:
    """Whether a detached generation's process GROUP has no members left
    (a dead leader's tmux/MCP descendants keep the group alive)."""
    try:
        os.killpg(int(pgid), 0)
        return False
    except ProcessLookupError:
        return True
    except Exception:
        # Permission/odd platform: fall back to the leader probe.
        return not _pid_alive(int(pgid))


def _stop_generation(record: WorkerRecord) -> bool:
    """VERIFIED bounded teardown of a generation's whole process tree
    (caller holds the flock). Returns False when the teardown could not
    be confirmed — callers fail closed (record retained as draining).

    systemd records stop through the unit — even when the leader pid is
    already dead, the unit's cgroup may still hold descendants (the
    observed leaked-tmux-child scope) — EXCEPT when the unit's live
    identity proves it belongs to a FOREIGN generation: that unit is
    never stopped (fail closed; it is not ours to kill). Detached
    records get a bounded TERM→KILL of the whole process group,
    verified against the group (not just the leader — a dead leader's
    descendants keep the pgid alive).
    """
    if record.supervision == "systemd" and record.unit:
        show = _systemd_show(record.unit)
        if not show:
            return False  # UNKNOWN unit state — never stop blind
        if show.get("ActiveState") in ("active", "activating", "deactivating"):
            # Stopping a LIVE unit requires positively proven ownership:
            # the live InvocationID must exist AND match our generation.
            # A missing id is UNKNOWN ownership — exactly like a
            # mismatch, the unit may belong to another generation or
            # controller and is never ours to kill.
            invocation = str(show.get("InvocationID") or "")
            if not (
                invocation
                and record.generation
                and invocation == record.generation
            ):
                logger.warning(
                    "unit %s is live with unproven ownership (live "
                    "invocation %r vs our generation %r) — refusing to "
                    "stop it",
                    record.unit, invocation, record.generation,
                )
                return False
        return _systemd_stop(record.unit)
    _kill_detached(record)
    return _pgid_empty(record.pid)


def _kill_detached(record: WorkerRecord) -> None:
    """Bounded TERM→KILL of a detached generation's whole process group
    (paced on the GROUP emptying — a dead leader's descendants must not
    short-circuit the escalation)."""
    import signal

    for sig, wait_s in ((signal.SIGTERM, 5.0), (signal.SIGKILL, 2.0)):
        try:
            os.killpg(int(record.pid), sig)
        except ProcessLookupError:
            return  # group already empty
        except (PermissionError, OSError):
            try:
                os.kill(int(record.pid), sig)
            except Exception:
                return
        deadline = time.monotonic() + wait_s
        while time.monotonic() < deadline:
            if _pgid_empty(record.pid):
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
# binding happens within one create round-trip.
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
    quota). Robust to junk/non-finite values (inf/nan/strings)."""
    import math

    try:
        raw = float(_plugin_config("max_workers"))
        if math.isfinite(raw) and int(raw) >= 1:
            return int(raw)
    except (TypeError, ValueError, OverflowError):
        pass
    return 10


def _idle_ttl_s() -> float:
    """The idle reap TTL: ``plugins.ghost_cursor.worker_idle_ttl_s`` in
    config.yaml, else :data:`IDLE_TTL_S`. Robust to junk/non-finite
    values (inf/nan/strings — inf would disable reaping forever)."""
    import math

    try:
        value = float(_plugin_config("worker_idle_ttl_s"))
        if math.isfinite(value) and value > 0:
            return value
    except (TypeError, ValueError, OverflowError):
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
    an OBSERVED remote terminal releases it (the live executor's GET
    settle authority) — never a timer, and holder death is irrelevant
    (the run lives server-side; a restarted gateway must find the
    worker still protected). An UNBOUND dispatch lease is transitional:
    it expires ``UNBOUND_LEASE_MAX_AGE_S`` after acquisition even with
    a live holder (a create that never bound within that window failed
    or was lost), and ``LEASE_STALE_GRACE_S`` after a dead holder.
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


def bind_lease(name: str, lease_id: str, agent_id: str, run_id: str) -> bool:
    """Bind a dispatch lease to its real remote identity (called the
    instant the create/follow-up returned).

    From here the lease protects the worker until an OBSERVED remote
    terminal — across gateway restarts and stream failures. Returns the
    AUTHORITATIVE outcome: False means the binding is NOT durable
    (record gone, lease gone, generation changed, or the write failed)
    and the caller must not proceed as if the worker were protected.
    Never raises (a failure reads as False)."""
    try:
        with _worker_lock(str(name)):
            record = _read_record(str(name))
            if record is None or str(lease_id) not in (record.leases or {}):
                return False
            leases = dict(record.leases)
            leases[str(lease_id)] = {
                **leases[str(lease_id)],
                "agent_id": str(agent_id or ""),
                "run_id": str(run_id or ""),
            }
            return _update_record_locked(
                str(name), record.generation,
                leases=leases, last_active_at=time.time(),
            )
    except Exception:
        logger.warning("bind_lease(%s, %s) failed", name, lease_id, exc_info=True)
        return False


def release_lease(name: str, lease_id: str, agent_id: str = "") -> None:
    """Release one run's lease (idempotent, never raises). Bumps the
    idle clock so a just-finished worker gets a full TTL.

    Called ONLY by whoever just OBSERVED the run's remote terminal
    state (cloud_runner's GET settle authority). When ``agent_id`` is
    given, sibling leases on the SAME worker bound to the SAME agent
    whose holder process died are settled too: runs on one agent are
    sequential, so terminal proof for a later run is terminal proof for
    every earlier run of that agent (this shrinks the bound-lease leak
    a crashed gateway leaves behind once the session is re-dispatched).
    """
    try:
        name, lease_id = str(name), str(lease_id)
        if not name or not lease_id:
            return
        with _worker_lock(name):
            record = _read_record(name)
            if record is None:
                return
            leases = dict(record.leases or {})
            changed = leases.pop(lease_id, None) is not None
            if agent_id:
                for key, lease in list(leases.items()):
                    if (
                        str((lease or {}).get("agent_id") or "") == str(agent_id)
                        and not _holder_alive(lease or {})
                    ):
                        logger.info(
                            "worker %s: settling stale lease %s (same agent "
                            "%s, dead holder) on this run's terminal proof",
                            name, key, agent_id,
                        )
                        leases.pop(key)
                        changed = True
            if changed:
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


def reconcile(
    now: Optional[float] = None, skip: Iterable[str] = (),
) -> List[str]:
    """One LAZY reaper pass (no timer framework): retire dead
    generations and reap IDLE (leaseless past :data:`IDLE_TTL_S`)
    workers with a bounded full-tree stop. Runs at plugin init and at
    the head of every ensure. A leased worker is never touched;
    ``skip`` exempts names the caller is about to hand out. Returns the
    reaped names. Never raises (a broken worker must not break the
    caller's dispatch)."""
    reaped: List[str] = []
    now = time.time() if now is None else now
    skip = set(skip)
    directory = state_dir()
    if not directory.is_dir():
        return reaped
    for path in sorted(directory.glob("*.json")):
        if path.name.endswith(".conflict.json") or path.stem in skip:
            continue
        try:
            with _try_worker_lock(path.stem) as acquired:
                if not acquired:
                    continue  # someone is actively touching it — next pass
                record = _read_record(path.stem)
                if record is None:
                    continue
                if record.state == STATE_CONFLICT:
                    # Conflicts resolve on positive proof only — never
                    # via eviction (that could stop a live candidate).
                    try:
                        _resolve_conflict_or_raise(record.name, record)
                    except WorkerError:
                        pass  # still conflicted — next pass
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
    """Non-blocking flock variant for cleanup/eviction: a contended
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
    fits. Only positively-owned generations are ever reclaimed: the
    stop path refuses to touch a live unit whose ownership it cannot
    prove. Never evicts a leased worker, never queues, never silently
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
            # Legacy workers may serve runs this controller cannot see,
            # and CONFLICTED workers must never have a candidate stopped
            # — neither is ever a reclaim candidate.
            if r.supervision != "legacy"
            and r.state != STATE_CONFLICT
            and not _fresh_leases(r, now)
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
