"""Gateway-side Codex client: reach the controller, start it, follow it.

* :func:`request` — one op over the controller's Unix socket.
* :func:`ensure_controller` — start the controller when it is not running:
  ``systemd-run --user`` when user systemd answers (survives gateway
  restarts; the unit is the controller's lifetime), else a detached child
  reported as ``"detached"`` (weaker: nothing restarts it if it dies).
* :class:`Follower` — one thread that pulls each Codex session's events
  from the controller from a stable cursor (``codex_last_seq`` on the
  handle entry), appends them to the shared JSONL event log, keeps the
  handle status truthful, sends progress digests per subscriber, and fans
  out completion messages onto ``process_registry.completion_queue`` before
  acking the controller's completion intent.

Delivery guarantee at this boundary: at-least-once up to the enqueue. A
gateway crash between enqueue and ack re-enqueues the same delegation id
(the gateway dedupes by ``(delegation_id, type)``). Nothing here claims
end-to-end exactly-once.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from . import eventlog as _eventlog
from . import handles as _handles
from . import progress as _progress
from . import render as _render
from . import supervisor as _supervisor

logger = logging.getLogger(__name__)

BACKEND = "codex"
CODEX_BIN_ENV = "GHOST_CURSOR_CODEX_BIN"
CONTROLLER_START_WAIT_S = 12.0
_UNIT_NAME = "ghost-cursor-codex"
_SOCKET_TIMEOUT_S = 90.0
FOLLOW_INTERVAL_S = 1.0
_DIGEST_MAX_EVENTS = 25


class ControllerUnavailable(RuntimeError):
    """The controller socket is absent or not answering."""


def state_dir() -> Path:
    """Machine-global Codex control dir (0700): socket, sessions, events, claims."""
    base = os.environ.get("XDG_STATE_HOME", "").strip() or str(Path.home() / ".local" / "state")
    path = Path(base) / "ghost_cursor" / "codex"
    path.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(path, 0o700)
    except Exception:
        pass
    return path


def claims_dir() -> str:
    return str(state_dir() / "claims")


def _plugin_config(key: str) -> Any:
    try:
        from hermes_cli.config import cfg_get, read_raw_config

        return cfg_get(read_raw_config(), "plugins", "ghost_cursor", key)
    except Exception:
        return None


def codex_bin() -> Optional[str]:
    """The codex executable: env > config > PATH. None when absent."""
    for candidate in (os.environ.get(CODEX_BIN_ENV), _plugin_config("codex_bin")):
        if candidate and os.access(str(candidate), os.X_OK):
            return str(candidate)
    return shutil.which("codex")


def configured_model() -> Optional[str]:
    val = _plugin_config("codex_model")
    return str(val).strip() or None if val else None


def configured_effort() -> Optional[str]:
    val = _plugin_config("codex_effort")
    return str(val).strip() or None if val else None


def available() -> bool:
    """Tool gate: a codex binary is reachable. Never requires CURSOR_API_KEY."""
    try:
        return codex_bin() is not None
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Socket requests
# ---------------------------------------------------------------------------

def request(op: str, timeout: float = _SOCKET_TIMEOUT_S, **fields: Any) -> Dict[str, Any]:
    sock_path = state_dir() / "control.sock"
    if not sock_path.exists():
        raise ControllerUnavailable("codex controller is not running (no control socket)")
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        try:
            s.connect(str(sock_path))
        except OSError as exc:
            raise ControllerUnavailable(f"codex controller is not answering: {exc}") from exc
        s.sendall((json.dumps({"op": op, **fields}, ensure_ascii=False) + "\n").encode("utf-8"))
        buf = b""
        while not buf.endswith(b"\n"):
            chunk = s.recv(65536)
            if not chunk:
                break
            buf += chunk
    except socket.timeout as exc:
        raise ControllerUnavailable(f"codex controller timed out on {op!r}") from exc
    finally:
        s.close()
    if not buf:
        raise ControllerUnavailable(f"codex controller closed the connection on {op!r}")
    resp = json.loads(buf.decode("utf-8"))
    return resp if isinstance(resp, dict) else {"ok": False, "error": "malformed controller reply"}


def ping() -> Optional[Dict[str, Any]]:
    try:
        resp = request("ping", timeout=5.0)
        return resp if resp.get("ok") else None
    except (ControllerUnavailable, ValueError):
        return None


def controller_info() -> Dict[str, Any]:
    try:
        return json.loads((state_dir() / "controller.json").read_text("utf-8"))
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Controller start
# ---------------------------------------------------------------------------

def _controller_argv(bin_path: str) -> List[str]:
    script = str(Path(__file__).resolve().parent / "codex_controller.py")
    argv = [sys.executable, script, "--state-dir", str(state_dir()), "--codex-bin", bin_path]
    codex_home = os.environ.get("CODEX_HOME") or _plugin_config("codex_home")
    if codex_home:
        argv += ["--codex-home", str(codex_home)]
    allow = _plugin_config("codex_env_allow")
    if isinstance(allow, list) and allow:
        argv += ["--env-allow", ",".join(str(a) for a in allow)]
    return argv


def _systemd_available() -> bool:
    try:
        proc = subprocess.run(["systemctl", "--user", "is-system-running"], capture_output=True, text=True, timeout=10)
    except Exception:
        return False
    return proc.returncode == 0 or (proc.stdout or "").strip() in ("running", "degraded")


def _start_systemd(argv: List[str]) -> bool:
    log_path = state_dir() / "controller.log"
    unit = f"{_UNIT_NAME}-{uuid.uuid4().hex[:8]}"
    cmd = [
        "systemd-run", "--user", f"--unit={unit}", "--collect", "--service-type=exec",
        "--property=KillMode=control-group", "--property=TimeoutStopSec=20", "--property=Restart=no",
        f"--property=StandardOutput=append:{log_path}", f"--property=StandardError=append:{log_path}",
        f"--working-directory={state_dir()}",
    ]
    for key in ("PATH", "HOME", "XDG_STATE_HOME", "CODEX_HOME"):
        if os.environ.get(key):
            cmd.append(f"--setenv={key}={os.environ[key]}")
    try:
        proc = subprocess.run(cmd + ["--", *argv], capture_output=True, text=True, timeout=30)
    except Exception:
        logger.warning("systemd-run for the codex controller failed", exc_info=True)
        return False
    if proc.returncode != 0:
        logger.warning("systemd-run rc %s: %s", proc.returncode, (proc.stderr or proc.stdout or "")[:300])
        return False
    return True


def _start_detached(argv: List[str]) -> None:
    log_path = state_dir() / "controller.log"
    with open(log_path, "ab") as log:
        subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True,
                         cwd=str(state_dir()), close_fds=True)


def ensure_controller() -> str:
    """Make sure a controller answers. Returns "systemd" | "detached" | "running".

    Raises :class:`ControllerUnavailable` with the codex-bin problem or the
    start failure. Never installs or logs in to anything.
    """
    if ping():
        return str(controller_info().get("supervision") or "running")
    bin_path = codex_bin()
    if not bin_path:
        raise ControllerUnavailable(
            f"codex is not installed or not on PATH — set {CODEX_BIN_ENV} or plugins.ghost_cursor.codex_bin"
        )
    argv = _controller_argv(bin_path)
    supervision = "systemd" if _systemd_available() and _start_systemd(argv) else "detached"
    if supervision == "detached":
        _start_detached(argv)
    deadline = time.monotonic() + CONTROLLER_START_WAIT_S
    while time.monotonic() < deadline:
        if ping():
            _handles_note_supervision(supervision)
            return supervision
        time.sleep(0.15)
    raise ControllerUnavailable("codex controller did not answer after start; see " + str(state_dir() / "controller.log"))


def _handles_note_supervision(kind: str) -> None:
    try:
        path = state_dir() / "controller.json"
        data = json.loads(path.read_text("utf-8"))
        data["supervision"] = kind
        path.write_text(json.dumps(data), "utf-8")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Follower: controller events -> shared event log, handles, digests, delivery
# ---------------------------------------------------------------------------

def _enqueue(evt: Dict[str, Any]) -> bool:
    try:
        from tools.process_registry import process_registry

        process_registry.completion_queue.put(evt)
        return True
    except Exception as exc:
        logger.error("codex follower: completion enqueue failed: %s", exc)
        return False


def completion_delegation_id(session: str, turn_id: str) -> str:
    """Stable per-turn identity: backend, session and turn."""
    return f"{session}#codex-turn-{turn_id}"


class Follower:
    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._ingest_lock = threading.RLock()
        self._digest_due: Dict[str, Dict[str, float]] = {}
        self._digest_n: Dict[str, Dict[str, int]] = {}
        self._pending_tools: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._files: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._plan: Dict[str, List[Dict[str, str]]] = {}

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, name="ghost-cursor-codex-follower", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.sync_once()
            except Exception:
                logger.debug("codex follower pass failed", exc_info=True)
            self._stop.wait(FOLLOW_INTERVAL_S)

    # -- one pass -----------------------------------------------------------------

    def sync_once(self) -> None:
        if not (state_dir() / "control.sock").exists():
            return
        try:
            live = request("sessions", timeout=10.0).get("sessions") or []
        except (ControllerUnavailable, ValueError):
            return
        for row in live:
            name = str(row.get("session") or "")
            entry = _handles.get(name)
            if entry is None or _handles.backend_of(entry) != BACKEND:
                continue
            self.ingest(name, entry)
            entry = _handles.get(name) or entry
            if row.get("active_turn_id"):
                if str(entry.get("status") or "") != "running":
                    _handles.record(name, status="running", codex_turn_id=row.get("active_turn_id"))
                self._maybe_digest(name, entry)
        try:
            pending = request("completions", timeout=10.0).get("completions") or []
        except (ControllerUnavailable, ValueError):
            return
        for comp in pending:
            self.deliver(comp)

    def ingest(self, name: str, entry: Dict[str, Any]) -> int:
        """Append the controller's new events for ``name`` to the shared log.

        Serialized: the follower thread and tool calls (status/events/stop)
        all ingest, and the cursor read-advance must not interleave.
        """
        with self._ingest_lock:
            entry = _handles.get(name) or entry
            return self._ingest_locked(name, entry)

    def _ingest_locked(self, name: str, entry: Dict[str, Any]) -> int:
        since = int(entry.get("codex_last_seq") or 0)
        appended = 0
        while True:
            try:
                page = request("events", timeout=30.0, session=name, since=since, limit=500)
            except (ControllerUnavailable, ValueError):
                break
            events = page.get("events") or []
            if not events:
                break
            for rec in events:
                env = {k: v for k, v in rec.items() if k not in ("seq", "ts")}
                env["codex_seq"] = rec.get("seq")
                env["backend"] = BACKEND
                _eventlog.append(name, env)
                self._fold(name, env)
                appended += 1
            since = int(page.get("next") or (events[-1]["seq"] + 1))
            _handles.record(name, codex_last_seq=since)
            if len(events) < 500:
                break
        return appended

    def _fold(self, name: str, env: Dict[str, Any]) -> None:
        kind = env.get("kind")
        if kind == "tool_use":
            pend = self._pending_tools.setdefault(name, {})
            cid = str(env.get("id") or "tool")
            prior = pend.get(cid)
            title = str(env.get("title") or env.get("command") or "").strip()
            pend[cid] = {"tool": env.get("tool"), "title": f"{env.get('tool')} — {title}" if title else str(env.get("tool")),
                         "since": (prior or {}).get("since") or time.time()}
            if env.get("tool") == "plan" and env.get("plan_items"):
                self._plan[name] = list(env.get("plan_items") or [])
        elif kind == "tool_result":
            self._pending_tools.setdefault(name, {}).pop(str(env.get("id") or "tool"), None)
        elif kind == "file_diff":
            files = self._files.setdefault(name, {})
            f = files.setdefault(env.get("path"), {"path": env.get("path"), "added": 0, "removed": 0})
            f["added"] += int(env.get("added") or 0)
            f["removed"] += int(env.get("removed") or 0)
            f["status"] = env.get("status")
        elif kind == "lifecycle" and env.get("event") in ("run.completed", "run.failed"):
            self._pending_tools.pop(name, None)

    def _maybe_digest(self, name: str, entry: Dict[str, Any]) -> None:
        subs = _handles.subscribers_of(entry)
        due_map = self._digest_due.setdefault(name, {})
        now = time.monotonic()
        for key, interval in subs.items():
            due = due_map.get(key)
            if due is None:
                due_map[key] = now + float(interval)
                continue
            if now < due:
                continue
            due_map[key] = now + float(interval)
            self._deliver_digest(name, entry, key, float(interval))

    def _deliver_digest(self, name: str, entry: Dict[str, Any], key: str, interval: float) -> None:
        sup = _handles.supervision_of(entry)
        cursor = sup["last_seq_delivered"].get(key, 0)
        stats = _eventlog.stats(name) or {}
        total = int(stats.get("total_events") or 0)
        new_count = max(total - cursor, 0)
        events: List[Dict[str, Any]] = []
        if new_count:
            page = _eventlog.read_events(name, offset=-1, limit=_DIGEST_MAX_EVENTS)
            events = [e for e in ((page or {}).get("events") or []) if int(e.get("seq") or 0) >= cursor]
        n = self._digest_n.setdefault(name, {}).get(key, 0) + 1
        now = time.time()
        pending = sorted(
            ({"call_id": cid, "tool": p.get("tool"), "title": p.get("title"),
              "pending_s": round(now - p["since"], 1) if p.get("since") else None}
             for cid, p in self._pending_tools.get(name, {}).items()),
            key=lambda p: -(p["pending_s"] or 0),
        )
        text = _render.digest_text(
            name=name, n=n, status="running", elapsed_s=None, last_activity_s=None,
            files=sorted(self._files.get(name, {}).values(), key=lambda f: str(f.get("path"))),
            pending_tools=pending, plan=list(self._plan.get(name, [])), events=events, new_count=new_count,
            next_update_s=interval,
        )
        evt = {
            "type": "async_delegation",
            "delegation_id": f"{name}#codex-progress-{n}@{_progress.subscriber_suffix(key)}@{entry.get('codex_turn_id') or ''}",
            "session_key": key,
            "goal": f"codex progress update {n} for session '{name}' (turn still active — NOT the final result)",
            "context": None, "toolsets": None, "role": "codex", "model": str(entry.get("model") or "codex"),
            "status": "running", "summary": text, "error": None, "api_calls": 0, "duration_seconds": 0.0,
            "dispatched_at": None, "completed_at": time.time(), "cursor_progress_update": n,
        }
        if _enqueue(evt):
            self._digest_n[name][key] = n
            _handles.advance_delivery_cursor(name, key, total)

    def deliver(self, comp: Dict[str, Any]) -> bool:
        """Fan out one completion intent, then ack it. True when acked."""
        name = str(comp.get("session") or "")
        turn_id = str(comp.get("turn_id") or "")
        entry = _handles.get(name)
        if entry is None or _handles.backend_of(entry) != BACKEND:
            # Not ours (or unknown here): ack so the controller does not spin on it.
            try:
                request("ack", session=name, turn_id=turn_id)
            except (ControllerUnavailable, ValueError):
                pass
            return False
        self.ingest(name, entry)
        entry = _handles.get(name) or entry
        status = str(comp.get("status") or "failed")
        if status == "unknown":
            status = "failed"
        files = [dict(f) for f in (comp.get("files") or []) if isinstance(f, dict)]
        error = str(comp.get("error") or "")
        started = comp.get("started_at")
        finished = comp.get("finished_at")
        elapsed = (float(finished) - float(started)) if started and finished else None
        _handles.record(
            name, status=status, files_changed_count=len(files) or None, codex_turn_id=turn_id,
            duration_s=round(elapsed, 1) if elapsed is not None else None,
            **({"status_note": error} if error else {}),
        )
        _eventlog.append(name, _supervisor.stamp(
            {"source": "ghost", "kind": "lifecycle", "event": "session.settled", "status": status, "turn_id": turn_id,
             "backend": BACKEND}, ""))
        if str(comp.get("status")) == "unknown":
            error = error or "codex app-server died mid-turn; outcome unknown"
        stats = _eventlog.stats(name) or {}
        summary_text = _render.completion_text(
            name=name, status=status, elapsed_s=elapsed, repo=str(entry.get("repo") or ""),
            summary=str(comp.get("summary") or ""), files=files, error=error,
            total_events=stats.get("total_events", 0), last_prompt_seq=_handles.last_prompt_seq(entry),
        )
        result = {
            "success": status == "completed", "status": status, "repo": str(entry.get("repo") or ""),
            "summary": str(comp.get("summary") or ""), "files_changed": files, "files_changed_count": len(files),
            "session": name, "backend": BACKEND, "codex_thread_id": str(entry.get("codex_thread_id") or ""),
            "codex_turn_id": turn_id, "model": str(comp.get("model") or entry.get("model") or ""),
            **({"error": error} if error else {}),
        }
        subscribers = _handles.subscribers_of(entry)
        dispatcher = str(entry.get("session_key") or "")
        recipients = sorted({str(k or "") for k in subscribers} | {dispatcher})
        base_id = completion_delegation_id(name, turn_id)
        base_evt = {
            "type": "async_delegation",
            "goal": f"codex: {str(entry.get('task') or name)[:200]}",
            "context": None, "toolsets": None, "role": "codex", "model": result["model"] or "codex",
            "status": status,
            "summary": f"{summary_text}\n\nfollow up in this session: codex_send_message('{name}', ...)",
            "error": error or None, "api_calls": 0,
            "duration_seconds": round(elapsed, 2) if elapsed is not None else 0.0,
            "dispatched_at": started, "completed_at": finished or time.time(), "result": result,
            "codex_turn_id": turn_id,
        }
        all_ok = True
        for key in recipients:
            all_ok &= _enqueue({
                **base_evt, "session_key": key,
                "delegation_id": base_id if key == dispatcher else f"{base_id}@{_progress.subscriber_suffix(key)}",
            })
        if not all_ok:
            return False  # leave the intent pending; the next pass retries
        try:
            request("ack", session=name, turn_id=turn_id)
        except (ControllerUnavailable, ValueError):
            return False
        self._digest_due.pop(name, None)
        self._files.pop(name, None)
        self._plan.pop(name, None)
        return True


follower = Follower()


def _reset_for_tests() -> None:
    global follower
    follower.stop()
    follower = Follower()
