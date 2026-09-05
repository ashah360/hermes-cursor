"""Independent Codex controller process.

Owns the ``codex app-server`` child (stdio), the durable per-session state,
the per-session event log, active-turn worktree claims, and pending
completion intents. Started by ``codex_client`` under user systemd when
available; runs standalone::

    python codex_controller.py --state-dir DIR --codex-bin /path/to/codex

The gateway talks to it over ``DIR/control.sock`` (Unix socket, mode 0600,
DIR is 0700). One request per connection, newline-delimited JSON::

    {"op": "start", "session": ..., "cwd": ..., "model": ..., "effort": ...,
     "prompt": ..., "intent_id": ...}      -> {"ok": true, "thread_id", "turn_id"}
    {"op": "steer", "session", "text", "expected_turn_id"} -> {"ok": true, "turn_id"}
    {"op": "interrupt", "session"}          -> {"ok": true, "turn_id"}
    {"op": "status", "session"}             -> {"ok": true, "state": {...}}
    {"op": "events", "session", "since", "limit"} -> {"ok": true, "events": [...], "next": N}
    {"op": "completions"}                   -> {"ok": true, "completions": [...]}
    {"op": "ack", "session", "turn_id"}     -> {"ok": true}
    {"op": "catalog"} / {"op": "ping"} / {"op": "sessions"} / {"op": "shutdown"}

Lifecycle authority is the observed ``turn/started`` / ``turn/completed``
state, never a client connection. Every turn's intent is persisted BEFORE
``turn/start`` is sent, so a crash between the two leaves a visible
``pending_intent`` instead of an ambiguous re-send.

Stdlib only: nothing here imports the plugin package.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import logging
import os
import queue
import re
import signal
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
import codex_protocol as proto  # noqa: E402

logger = logging.getLogger("codex_controller")

SOCKET_NAME = "control.sock"
LOCK_NAME = "controller.lock"
INFO_NAME = "controller.json"
_SAFE = re.compile(r"[^A-Za-z0-9._-]")

TERMINAL = ("completed", "failed", "cancelled")

# thread/start takes the config-style sandbox mode (verified against codex-cli
# 0.153.4: "read-only" | "workspace-write" | "danger-full-access"); turn/start's
# sandboxPolicy.type uses the camelCase variant.
DEFAULT_SANDBOX = "workspace-write"
SANDBOX_POLICY_TYPE = {"read-only": "readOnly", "workspace-write": "workspaceWrite",
                       "danger-full-access": "dangerFullAccess"}
DEFAULT_APPROVAL = "never"


def _pid_birth(pid: int) -> Optional[int]:
    try:
        with open(f"/proc/{pid}/stat", "r") as fh:
            return int(fh.read().rsplit(")", 1)[1].split()[19])
    except Exception:
        return None


def claim_key(cwd: str) -> str:
    return hashlib.sha1(os.path.realpath(cwd).encode("utf-8")).hexdigest()[:16]


class _flocked:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._fh = None

    def __enter__(self):
        self._fh = open(self._path, "a+")
        fcntl.flock(self._fh, fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc):
        try:
            fcntl.flock(self._fh, fcntl.LOCK_UN)
        finally:
            self._fh.close()


def _now() -> float:
    return round(time.time(), 3)


class Controller:
    def __init__(self, state_dir: Path, codex_bin: str, codex_home: Optional[str], env_allow: List[str],
                 idle_exit_s: float, turn_start_timeout_s: float = 60.0) -> None:
        self.state_dir = state_dir
        self.turn_start_timeout_s = turn_start_timeout_s
        self.codex_bin = codex_bin
        self.codex_home = codex_home
        self.env_allow = env_allow
        self.idle_exit_s = idle_exit_s
        self.sessions_dir = state_dir / "sessions"
        self.events_dir = state_dir / "events"
        self.claims_dir = state_dir / "claims"
        for d in (state_dir, self.sessions_dir, self.events_dir, self.claims_dir):
            d.mkdir(parents=True, exist_ok=True)
            os.chmod(d, 0o700)
        self._lock = threading.RLock()
        self._server: Optional[proto.AppServer] = None
        self._server_info: Dict[str, Any] = {}
        self._catalog: Optional[List[Dict[str, Any]]] = None
        self._normalizers: Dict[str, proto.Normalizer] = {}
        self._thread_to_session: Dict[str, str] = {}
        self._inbox: "queue.Queue[Any]" = queue.Queue()
        self._stop = threading.Event()
        self._last_activity = time.monotonic()
        self.containment = "unknown"

    # -- persistence -----------------------------------------------------------

    def _safe(self, session: str) -> str:
        return _SAFE.sub("_", str(session))[:120]

    def _session_path(self, session: str) -> Path:
        return self.sessions_dir / f"{self._safe(session)}.json"

    def _events_path(self, session: str) -> Path:
        return self.events_dir / f"{self._safe(session)}.jsonl"

    def load(self, session: str) -> Optional[Dict[str, Any]]:
        path = self._session_path(session)
        if not path.is_file():
            return None
        try:
            return json.loads(path.read_text("utf-8"))
        except Exception:
            return None

    def save(self, state: Dict[str, Any]) -> None:
        path = self._session_path(state["session"])
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, ensure_ascii=False), "utf-8")
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)

    def append_event(self, session: str, envelope: Dict[str, Any], state: Optional[Dict[str, Any]] = None) -> int:
        """Append one event with the next per-session seq.

        ``state`` (when the caller holds the session record) is mutated in
        place and NOT saved here — the caller saves once; otherwise the
        record is loaded and saved. Both paths run under ``self._lock`` so
        seq assignment is single-writer.
        """
        own = state is None
        with self._lock:
            if own:
                state = self.load(session) or {"session": session, "next_seq": 0}
            seq = int(state.get("next_seq") or 0)
            record = {"seq": seq, "ts": _now(), **envelope}
            with self._events_path(session).open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
            state["next_seq"] = seq + 1
            if own:
                self.save(state)
        return seq

    def read_events(self, session: str, since: int, limit: int) -> List[Dict[str, Any]]:
        path = self._events_path(session)
        if not path.is_file():
            return []
        out: List[Dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                if isinstance(rec, dict) and int(rec.get("seq", -1)) >= since:
                    out.append(rec)
                    if len(out) >= limit:
                        break
        return out

    # -- claims (cross-backend writer exclusion, by canonical worktree) --------
    # Same file + flock as the gateway's writer_guard: one critical section
    # for every writer on this machine.

    def _claim_path(self, cwd: str) -> Path:
        return self.claims_dir / f"{claim_key(cwd)}.json"

    def _claim_locked(self, cwd: str):
        return _flocked(self.claims_dir / f"{claim_key(cwd)}.lock")

    def write_claim(self, cwd: str, session: str, turn_id: str, owner: str = "") -> None:
        """Assert (or refresh) the claim for ``owner`` (the dispatch intent id).
        Caller holds the flock. Only the same owner is ever overwritten."""
        pid = os.getpid()
        prior = self._read_claim(cwd) or {}
        data = {"cwd": os.path.realpath(cwd), "session": session, "turn_id": turn_id, "backend": "codex",
                "owner": owner or prior.get("owner") or "", "holder_pid": pid, "holder_birth": _pid_birth(pid),
                "claimed_at": prior.get("claimed_at") if prior.get("owner") == (owner or prior.get("owner")) else _now()}
        path = self._claim_path(cwd)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data), "utf-8")
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)

    def _read_claim(self, cwd: str) -> Optional[Dict[str, Any]]:
        try:
            data = json.loads(self._claim_path(cwd).read_text("utf-8"))
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    def clear_claim(self, cwd: str, session: str, owner: str = "") -> bool:
        """Remove the claim only when it belongs to ``session`` AND ``owner``
        (when given): a losing request can never remove the winner's claim."""
        with self._claim_locked(cwd):
            data = self._read_claim(cwd)
            if data is None or data.get("session") != session:
                return False
            if owner and str(data.get("owner") or "") not in ("", owner):
                return False
            self._claim_path(cwd).unlink(missing_ok=True)
            return True

    @staticmethod
    def _claim_live(data: Dict[str, Any]) -> bool:
        pid = int(data.get("holder_pid") or 0)
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
        return data.get("holder_birth") in (None, _pid_birth(pid))

    def other_claim(self, cwd: str, owner: str) -> Optional[Dict[str, Any]]:
        """A LIVE claim by a different owner (any backend, any session — a
        second dispatch on the SAME session is also another owner). Caller
        holds the flock."""
        data = self._read_claim(cwd)
        if data is None or str(data.get("owner") or "") == owner:
            return None
        if not self._claim_live(data):
            return None
        return data

    # -- app-server ------------------------------------------------------------

    def _spawn_env(self) -> Dict[str, str]:
        env = {k: v for k, v in os.environ.items()
               if k in ("PATH", "HOME", "USER", "LANG", "LC_ALL", "TERM", "TMPDIR", "XDG_RUNTIME_DIR",
                        "SSL_CERT_FILE", "SSL_CERT_DIR", "HTTPS_PROXY", "HTTP_PROXY", "NO_PROXY")
               or k in self.env_allow}
        if self.codex_home:
            env["CODEX_HOME"] = self.codex_home
        return env

    def server(self) -> proto.AppServer:
        with self._lock:
            if self._server is not None and self._server.alive():
                return self._server
            srv_ref: List[Any] = [None]
            # Containment: a controller-owned, uniquely named user-systemd
            # scope when user systemd answers; otherwise the child runs
            # uncontained and every cleanup proof fails closed.
            unit = proto.new_unit_name() if proto.systemd_user_available() else None
            if unit:
                self._record_unit(unit)
            srv = proto.AppServer(
                self.codex_bin, env=self._spawn_env(), unit=unit,
                on_notification=lambda m: self._inbox.put(("notify", m)),
                on_request=lambda m: self._inbox.put(("request", m)),
                on_exit=lambda code: self._inbox.put(("exit", (code, srv_ref[0]))),
            )
            srv_ref[0] = srv
            try:
                self._server_info = srv.initialize()
            except Exception:
                srv.close(grace_s=1.0)
                raise
            self._server = srv
            self._catalog = None
            self._thread_to_session.clear()
            self.containment = "systemd-scope" if unit else "none"
            return srv

    def _units_file(self) -> Path:
        return self.state_dir / "appserver_units.json"

    def _record_unit(self, unit: str) -> None:
        """Remember every owned scope this state dir ever started, until its
        cgroup is proven empty. Only unit NAMES are persisted — never pids or
        pgids, which can be reused by unrelated processes after a restart."""
        units = self._known_units()
        if unit not in units:
            units.append(unit)
        self._units_file().write_text(json.dumps(units), "utf-8")

    def _known_units(self) -> List[Any]:
        try:
            data = json.loads(self._units_file().read_text("utf-8"))
            return list(data) if isinstance(data, list) else []
        except Exception:
            return []

    def _forget_unit(self, unit: str) -> None:
        units = [u for u in self._known_units() if u != unit]
        self._units_file().write_text(json.dumps(units), "utf-8")

    def _stop_all_known_units(self, grace_s: float = 5.0) -> List[Any]:
        """Stop every recorded owned scope; return the entries that could NOT
        be proven empty (they stay recorded). Entries that are not
        controller-minted unit names (e.g. a legacy naked pgid) are never
        signalled and always count as unproven."""
        survivors: List[Any] = []
        for unit in self._known_units():
            if not isinstance(unit, str) or not unit.startswith(proto.UNIT_PREFIX):
                survivors.append(unit)
                continue
            if proto.stop_unit(unit, grace_s):
                self._forget_unit(unit)
            else:
                survivors.append(unit)
        return survivors

    def catalog(self) -> List[Dict[str, Any]]:
        srv = self.server()
        with self._lock:
            if self._catalog is None:
                self._catalog = proto.list_models(srv)
            return list(self._catalog)

    # -- ops ---------------------------------------------------------------------

    def op_start(self, req: Dict[str, Any]) -> Dict[str, Any]:
        session = str(req.get("session") or "").strip()
        cwd = str(req.get("cwd") or "").strip()
        prompt = str(req.get("prompt") or "")
        intent_id = str(req.get("intent_id") or "")
        if not session or not cwd or not prompt or not intent_id:
            return {"ok": False, "error": "session, cwd, prompt and intent_id are required"}
        if not os.path.isdir(cwd):
            return {"ok": False, "error": f"cwd does not exist: {cwd}"}
        with self._lock:
            state = self.load(session) or {
                "session": session, "cwd": os.path.realpath(cwd), "next_seq": 0, "turns": [], "completions": [],
                "created_at": _now(),
            }
            if state.get("active_turn_id"):
                return {"ok": False, "error": "turn_active", "turn_id": state["active_turn_id"]}
            pending = state.get("pending_intent")
            if pending:
                # An earlier dispatch's outcome is unknown (transport lost the
                # turn/start reply, or a crash between intent and start).
                # Never stack a second turn on it; codex_stop clears it.
                return {"ok": False, "error": "duplicate_intent" if pending.get("intent_id") == intent_id else "pending_intent",
                        "detail": "an earlier dispatch on this session was persisted but its turn/start outcome is unknown; "
                                  "inspect codex_status, then codex_stop to clear it before sending again",
                        "pending_intent": pending}
            model = str(req.get("model") or state.get("model") or "").strip()
            effort = req.get("effort") if req.get("effort") is not None else state.get("effort")
            if not model:
                return {"ok": False, "error": "model is required (no default model substitution)"}
            try:
                resolved = proto.resolve_model(self.catalog(), model, effort)
            except proto.ProtocolError as exc:
                return {"ok": False, "error": str(exc)}
            state.update({
                "cwd": os.path.realpath(cwd), "model": resolved["model"], "effort": resolved["effort"],
                "sandbox": str(req.get("sandbox") or state.get("sandbox") or DEFAULT_SANDBOX),
                "approval_policy": str(req.get("approval_policy") or state.get("approval_policy") or DEFAULT_APPROVAL),
            })
            # Reserve the worktree and persist the dispatch intent BEFORE any
            # wire call. Both stay in place until the outcome is known.
            with self._claim_locked(state["cwd"]):
                claim = self.other_claim(state["cwd"], intent_id)
                if claim is not None:
                    return {"ok": False, "error": "worktree_busy", "claim": claim}
                self.write_claim(state["cwd"], session, "pending", owner=intent_id)
            state["pending_intent"] = {"intent_id": intent_id, "prompt_sha1": hashlib.sha1(prompt.encode()).hexdigest(),
                                       "prompt_head": prompt[:200], "at": _now()}
            self.save(state)
            try:
                srv = self.server()
                thread_id = str(state.get("thread_id") or "")
                if thread_id:
                    try:
                        srv.request("thread/resume", {"threadId": thread_id}, timeout=60)
                    except proto.RpcError as exc:
                        return self._fail_intent(state, f"thread/resume failed for {thread_id}: {exc.message}")
                else:
                    res = srv.request("thread/start", {
                        "model": state["model"], "cwd": state["cwd"], "approvalPolicy": state["approval_policy"],
                        "sandbox": state["sandbox"], "serviceName": proto.CLIENT_INFO["name"],
                    }, timeout=60)
                    thread_id = str(((res.get("thread") or {}).get("id")) or "")
                    if not thread_id:
                        return self._fail_intent(state, "thread/start returned no thread id")
                    state["thread_id"] = thread_id
                    self.save(state)
                self._thread_to_session[thread_id] = session
                self._normalizers[thread_id] = proto.Normalizer()
            except proto.RpcError as exc:
                return self._fail_intent(state, str(exc))
            except (proto.ProtocolError, OSError) as exc:
                # No turn/start was sent yet: a lost thread/start reply can at
                # most leave an unused thread behind. Definitive failure.
                return self._fail_intent(state, str(exc))
            params: Dict[str, Any] = {
                "threadId": thread_id, "input": [{"type": "text", "text": prompt}], "cwd": state["cwd"],
                "approvalPolicy": state["approval_policy"],
                "sandboxPolicy": {"type": SANDBOX_POLICY_TYPE.get(state["sandbox"], "workspaceWrite"),
                                  "writableRoots": [state["cwd"]], "networkAccess": True},
                "model": state["model"],
            }
            if state.get("effort"):
                params["effort"] = state["effort"]
            try:
                res = srv.request("turn/start", params, timeout=self.turn_start_timeout_s)
            except proto.RpcError as exc:
                # The server answered: it rejected the turn. Definitive.
                return self._fail_intent(state, str(exc))
            except proto.ProtocolError as exc:
                # Timeout / closed pipe AFTER turn/start went out: the provider
                # may have accepted the turn. Keep the claim and the intent;
                # a turn/started notification for this thread adopts the turn
                # (_on_notify); codex_stop reconciles an intent nothing
                # arrives for.
                return self._mark_ambiguous(state, intent_id, thread_id, str(exc))
            turn = res.get("turn") or {}
            turn_id = str(turn.get("id") or "")
            if not turn_id:
                # A success reply without a turn id: the provider accepted
                # SOMETHING we cannot name. Ambiguous, not a rejection.
                return self._mark_ambiguous(state, intent_id, thread_id, "turn/start succeeded without a turn id")
            if state.get("active_turn_id") == turn_id:
                # turn/started already adopted it (reply raced the notification).
                return {"ok": True, "thread_id": thread_id, "turn_id": turn_id, "model": state["model"],
                        "effort": state.get("effort"), "resumed": len(state["turns"]) > 1}
            self._activate_turn(state, turn_id, intent_id, prompt)
            return {"ok": True, "thread_id": thread_id, "turn_id": turn_id, "model": state["model"],
                    "effort": state.get("effort"), "resumed": len(state["turns"]) > 1}

    def _fail_intent(self, state: Dict[str, Any], error: str) -> Dict[str, Any]:
        """Definitive rejection (the provider answered, or nothing was sent):
        drop the intent and this owner's worktree claim."""
        pending = state.get("pending_intent") or {}
        state["pending_intent"] = None
        state["last_error"] = error
        state["status"] = state.get("status") if state.get("turns") else "failed"
        self.save(state)
        self.clear_claim(state["cwd"], state["session"], owner=str(pending.get("intent_id") or ""))
        self.append_event(state["session"], proto.lifecycle("dispatch.failed", error=error,
                                                            intent_id=pending.get("intent_id")))
        return {"ok": False, "error": error, "definitive": True}

    def _mark_ambiguous(self, state: Dict[str, Any], intent_id: str, thread_id: str, error: str) -> Dict[str, Any]:
        state["pending_intent"]["ambiguous"] = True
        state["pending_intent"]["error"] = error
        state["last_error"] = error
        self.save(state)
        self.append_event(state["session"], proto.lifecycle("dispatch.ambiguous", intent_id=intent_id, error=error,
                                                            thread_id=thread_id))
        return {"ok": False, "error": "ambiguous_dispatch", "detail": error, "thread_id": thread_id,
                "pending_intent": state["pending_intent"]}

    def _activate_turn(self, state: Dict[str, Any], turn_id: str, intent_id: str, prompt: str) -> None:
        """The turn exists on the provider: record it, clear the intent, bind the claim."""
        session = state["session"]
        state["active_turn_id"] = turn_id
        state["status"] = "running"
        state["pending_intent"] = None
        state.setdefault("turns", []).append({"turn_id": turn_id, "intent_id": intent_id, "status": "running",
                                              "started_at": _now(), "prompt_head": prompt[:200]})
        state["last_activity_at"] = _now()
        self.save(state)
        with self._claim_locked(state["cwd"]):
            self.write_claim(state["cwd"], session, turn_id, owner=intent_id)
        self._last_activity = time.monotonic()
        self.append_event(session, proto.lifecycle("dispatch.accepted", turn_id=turn_id, thread_id=state.get("thread_id"),
                                                   model=state.get("model"), effort=state.get("effort"), intent_id=intent_id))

    def op_steer(self, req: Dict[str, Any]) -> Dict[str, Any]:
        session = str(req.get("session") or "")
        text = str(req.get("text") or "")
        expected = str(req.get("expected_turn_id") or "")
        with self._lock:
            state = self.load(session)
            if state is None:
                return {"ok": False, "error": "unknown session"}
            active = str(state.get("active_turn_id") or "")
            if not active:
                return {"ok": False, "error": "no_active_turn"}
            if expected and expected != active:
                return {"ok": False, "error": "stale_turn", "turn_id": active}
            try:
                res = self.server().request("turn/steer", {
                    "threadId": state["thread_id"], "input": [{"type": "text", "text": text}], "expectedTurnId": active,
                }, timeout=30)
            except proto.ProtocolError as exc:
                return {"ok": False, "error": str(exc), "turn_id": active}
            self.append_event(session, proto.lifecycle("steer.accepted", turn_id=active, text_head=text[:200]))
            return {"ok": True, "turn_id": str(res.get("turnId") or active)}

    def op_interrupt(self, req: Dict[str, Any]) -> Dict[str, Any]:
        session = str(req.get("session") or "")
        with self._lock:
            state = self.load(session)
            if state is None:
                return {"ok": False, "error": "unknown session"}
            active = str(state.get("active_turn_id") or "")
            if not active:
                pending = state.get("pending_intent")
                if pending:
                    return self._reconcile_pending(state, pending)
                return {"ok": True, "turn_id": "", "status": state.get("status") or "idle"}
            try:
                self.server().request("turn/interrupt", {"threadId": state["thread_id"], "turnId": active}, timeout=30)
            except proto.ProtocolError as exc:
                return {"ok": False, "error": str(exc), "turn_id": active}
            self.append_event(session, proto.lifecycle("interrupt_requested", turn_id=active))
            return {"ok": True, "turn_id": active, "status": "running"}

    def _reconcile_pending(self, state: Dict[str, Any], pending: Dict[str, Any]) -> Dict[str, Any]:
        """Explicit stop on an unresolved dispatch intent.

        "No observed turn" is not proof that nothing runs. The claim and the
        intent are released only once the controller can PROVE quiescence:
        the app-server child it owns is dead (never started or already gone),
        or it is the only session on that child and the child is torn down
        here (bounded, full process group). If another session has an active
        turn on the shared child, nothing is killed and the intent stays
        unresolved — reported honestly.
        """
        session = state["session"]
        others = [s for s in self._active_sessions() if s != session]
        if others and self._server is not None:
            return {"ok": True, "turn_id": "", "status": "unresolved", "pending_intent": pending,
                    "reason": ("cannot prove the lost dispatch is not executing: the shared codex app-server "
                               f"still serves active turns for {len(others)} other session(s); the intent and the "
                               "worktree claim stay in place")}
        # Proof is every owned scope's CGROUP being empty — including one
        # whose leader exited cleanly and left tool descendants (setsid or
        # not) behind. The leader's exit (or AppServer.alive()) is never
        # accepted as evidence. An uncontained child cannot be proven clean.
        with self._lock:
            app, self._server = self._server, None
        uncontained = app is not None and not app.contained()
        if app is not None:
            app.close(grace_s=5.0)
        survivors = self._stop_all_known_units(grace_s=5.0)
        if uncontained or survivors:
            reason = ("the app-server ran uncontained (no user systemd: detached fallback) — descendant cleanup "
                      "cannot be proven" if uncontained else
                      f"owned scope(s) {survivors} still have processes in their cgroup (or are not verifiable)")
            return {"ok": True, "turn_id": "", "status": "unresolved", "pending_intent": pending,
                    "reason": reason + "; intent and claim stay in place", "surviving_units": survivors,
                    "containment": "none" if uncontained else "systemd-scope"}
        proof = "every owned app-server scope cgroup is empty (verified via systemd + cgroup.procs)"
        state["pending_intent"] = None
        state["status"] = "unknown"
        self.save(state)
        self.clear_claim(state["cwd"], session, owner=str(pending.get("intent_id") or ""))
        self.append_event(session, proto.lifecycle(
            "intent.cleared", intent_id=pending.get("intent_id"), proof=proof,
            note="unresolved dispatch intent cleared by an explicit stop after proving quiescence"))
        return {"ok": True, "turn_id": "", "status": "unknown", "intent_cleared": pending.get("intent_id"), "proof": proof}

    def op_status(self, req: Dict[str, Any]) -> Dict[str, Any]:
        state = self.load(str(req.get("session") or ""))
        if state is None:
            return {"ok": False, "error": "unknown session"}
        srv_alive = self._server is not None and self._server.alive()
        return {"ok": True, "state": state, "app_server_alive": srv_alive, "controller_pid": os.getpid(),
                "containment": self.containment}

    def op_events(self, req: Dict[str, Any]) -> Dict[str, Any]:
        session = str(req.get("session") or "")
        since = max(int(req.get("since") or 0), 0)
        limit = min(max(int(req.get("limit") or 200), 1), 2000)
        events = self.read_events(session, since, limit)
        return {"ok": True, "events": events, "next": (events[-1]["seq"] + 1) if events else since}

    def op_completions(self, req: Dict[str, Any]) -> Dict[str, Any]:
        out = []
        for path in sorted(self.sessions_dir.glob("*.json")):
            try:
                state = json.loads(path.read_text("utf-8"))
            except Exception:
                continue
            for c in state.get("completions") or []:
                if not c.get("delivered"):
                    out.append({"session": state["session"], **c})
        return {"ok": True, "completions": out}

    def op_ack(self, req: Dict[str, Any]) -> Dict[str, Any]:
        session = str(req.get("session") or "")
        turn_id = str(req.get("turn_id") or "")
        with self._lock:
            state = self.load(session)
            if state is None:
                return {"ok": False, "error": "unknown session"}
            for c in state.get("completions") or []:
                if c.get("turn_id") == turn_id:
                    c["delivered"] = True
                    c["delivered_at"] = _now()
            self.save(state)
        return {"ok": True}

    def op_sessions(self, req: Dict[str, Any]) -> Dict[str, Any]:
        out = []
        for path in sorted(self.sessions_dir.glob("*.json")):
            try:
                st = json.loads(path.read_text("utf-8"))
            except Exception:
                continue
            out.append({k: st.get(k) for k in ("session", "cwd", "model", "effort", "thread_id", "active_turn_id",
                                                "status", "pending_intent", "last_activity_at")})
        return {"ok": True, "sessions": out}

    def op_catalog(self, req: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return {"ok": True, "models": self.catalog(), "server": self._server_info}
        except proto.ProtocolError as exc:
            return {"ok": False, "error": str(exc), "stderr": (self._server.stderr_tail if self._server else [])}
        except OSError as exc:
            return {"ok": False, "error": f"cannot start codex app-server ({self.codex_bin}): {exc}"}

    def handle(self, req: Dict[str, Any]) -> Dict[str, Any]:
        op = str(req.get("op") or "")
        if op == "ping":
            return {"ok": True, "pid": os.getpid(), "app_server_alive": bool(self._server and self._server.alive()),
                    "codex_bin": self.codex_bin, "containment": self.containment}
        if op == "shutdown":
            logger.info("shutdown requested by a client")
            self._stop.set()
            return {"ok": True}
        fn = getattr(self, f"op_{op}", None)
        if fn is None:
            return {"ok": False, "error": f"unknown op {op!r}"}
        try:
            return fn(req)
        except OSError as exc:
            return {"ok": False, "error": f"cannot start codex app-server ({self.codex_bin}): {exc}"}
        except Exception as exc:  # never crash the controller on one request
            logger.exception("op %s failed", op)
            return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

    # -- inbox: notifications / server requests / exit ------------------------------

    def _fold_loop(self) -> None:
        while not self._stop.is_set():
            try:
                kind, msg = self._inbox.get(timeout=0.2)
            except queue.Empty:
                self._idle_check()
                continue
            try:
                if kind == "notify":
                    self._on_notify(msg)
                elif kind == "request":
                    self._on_request(msg)
                elif kind == "exit":
                    self._on_exit(msg)
            except Exception:
                logger.exception("fold failed for %s", kind)

    def _session_for(self, params: Dict[str, Any]) -> Optional[str]:
        thread_id = str(params.get("threadId") or ((params.get("turn") or {}).get("threadId")) or "")
        if thread_id and thread_id in self._thread_to_session:
            return self._thread_to_session[thread_id]
        if not thread_id:
            # Some turn notifications omit threadId; fall back to the single active session.
            active = [s for s, st in self._active_sessions().items() if st.get("active_turn_id")]
            return active[0] if len(active) == 1 else None
        for path in self.sessions_dir.glob("*.json"):
            try:
                st = json.loads(path.read_text("utf-8"))
            except Exception:
                continue
            if st.get("thread_id") == thread_id:
                self._thread_to_session[thread_id] = st["session"]
                return st["session"]
        return None

    def _active_sessions(self) -> Dict[str, Dict[str, Any]]:
        out = {}
        for path in self.sessions_dir.glob("*.json"):
            try:
                st = json.loads(path.read_text("utf-8"))
            except Exception:
                continue
            if st.get("active_turn_id"):
                out[st["session"]] = st
        return out

    def _on_notify(self, msg: Dict[str, Any]) -> None:
        params = msg.get("params") if isinstance(msg.get("params"), dict) else {}
        session = self._session_for(params)
        if session is None:
            return
        self._last_activity = time.monotonic()
        with self._lock:
            state = self.load(session)
            if state is None:
                return
            thread_id = str(state.get("thread_id") or "")
            norm = self._normalizers.setdefault(thread_id, proto.Normalizer())
            envelopes = norm.normalize(msg)
            started = str(msg.get("method") or "") == "turn/started"
            if started and not state.get("active_turn_id"):
                # A turn we did not record as active: the turn/start reply was
                # lost (ambiguous dispatch) — adopt it instead of doubling.
                turn_id = str(((params.get("turn") or {}).get("id")) or "")
                pending = state.get("pending_intent") or {}
                if turn_id:
                    self._activate_turn(state, turn_id, str(pending.get("intent_id") or "adopted"),
                                        str(pending.get("prompt_head") or ""))
                    state = self.load(session) or state
                    self.append_event(session, proto.lifecycle("dispatch.adopted", turn_id=turn_id,
                                                               intent_id=pending.get("intent_id")), state)
            turn_state = next((t for t in reversed(state.get("turns") or []) if t.get("turn_id") == state.get("active_turn_id")), None)
            for env in envelopes:
                self.append_event(session, env, state)
                kind = env.get("kind")
                if turn_state is not None:
                    if kind == "file_diff":
                        files = turn_state.setdefault("files", {})
                        f = files.setdefault(env["path"], {"path": env["path"], "added": 0, "removed": 0})
                        f["added"] += int(env.get("added") or 0)
                        f["removed"] += int(env.get("removed") or 0)
                        f["status"] = env.get("status")
                        f["diff"] = str(env.get("diff") or "")[:20_000]
                        turn_state["segment_open"] = False
                    elif kind == "content":
                        delta = str(env.get("delta") or "")
                        segs = turn_state.setdefault("segments", [])
                        if delta:
                            if turn_state.get("segment_open") and segs:
                                segs[-1] += delta
                            else:
                                segs.append(delta)
                                turn_state["segment_open"] = True
                        if env.get("done"):
                            turn_state["segment_open"] = False
                            turn_state["ended_on_content"] = True
                    elif kind in ("tool_use", "tool_result"):
                        turn_state["segment_open"] = False
                        turn_state["ended_on_content"] = False
                        pend = turn_state.setdefault("pending_tools", {})
                        if kind == "tool_use":
                            pend[str(env.get("id"))] = {"tool": env.get("tool"), "title": env.get("title"), "since": _now()}
                            if env.get("tool") == proto.TOOL_PLAN and env.get("plan_items"):
                                turn_state["plan"] = list(env.get("plan_items") or [])
                                pend.pop(str(env.get("id")), None)
                        else:
                            pend.pop(str(env.get("id")), None)
                    elif kind == "lifecycle" and env.get("event") == "thread.status":
                        state["thread_status"] = {"type": env.get("status"), "flags": env.get("flags")}
                if kind == "lifecycle" and env.get("event") in ("run.completed", "run.failed"):
                    self._settle_turn(state, turn_state, env)
            state["last_activity_at"] = _now()
            self.save(state)

    def _settle_turn(self, state: Dict[str, Any], turn_state: Optional[Dict[str, Any]], env: Dict[str, Any],
                     release_claim: bool = True) -> None:
        turn_id = str(env.get("turn_id") or state.get("active_turn_id") or "")
        if env.get("event") == "run.completed":
            status = "completed"
        elif env.get("cancelled"):
            status = "cancelled"
        else:
            status = "failed"
        segs = (turn_state or {}).get("segments") or []
        blocks = [b.strip() for b in segs if b.strip()]
        summary = (blocks[-1] if (turn_state or {}).get("ended_on_content") and blocks else "\n\n".join(blocks))
        files = sorted(((turn_state or {}).get("files") or {}).values(), key=lambda f: f["path"])
        if turn_state is not None:
            turn_state["status"] = status
            turn_state["finished_at"] = _now()
            turn_state["pending_tools"] = {}
        state["active_turn_id"] = None
        state["status"] = status
        state.setdefault("completions", []).append({
            "turn_id": turn_id, "status": status, "error": env.get("error"), "summary": summary[:20_000],
            "files": files, "started_at": (turn_state or {}).get("started_at"), "finished_at": _now(),
            "delivered": False, "model": state.get("model"),
        })
        if release_claim:
            self.clear_claim(state["cwd"], state["session"], owner=str((turn_state or {}).get("intent_id") or ""))

    def _on_request(self, msg: Dict[str, Any]) -> None:
        method = str(msg.get("method") or "")
        params = msg.get("params") if isinstance(msg.get("params"), dict) else {}
        session = self._session_for(params)
        srv = self._server
        if srv is not None:
            try:
                srv.respond(msg.get("id"), proto.decline_result(method))
            except Exception:
                logger.debug("respond failed", exc_info=True)
        if session:
            self.append_event(session, proto.lifecycle(
                "approval.declined", method=method, item_id=params.get("itemId"), turn_id=params.get("turnId"),
                reason=params.get("reason"),
                note="codex asked for interactive approval/input; v1 declines instead of hanging",
            ))

    def _on_exit(self, payload: Any) -> None:
        code, srv = payload if isinstance(payload, tuple) else (payload, None)
        with self._lock:
            if srv is not None and self._server is not None and self._server is not srv:
                return  # a stale exit from an app-server already replaced
            # The leader exited; its tool descendants may not have. Stop the
            # whole group and only release worktree claims when it is proven
            # empty — otherwise the claims stay (a later stop/boot retries).
            group_ok = True
            if srv is not None:
                group_ok = bool(srv.unit) and proto.stop_unit(srv.unit, 5.0)
                if group_ok:
                    self._forget_unit(srv.unit)
            for session, state in self._active_sessions().items():
                turn_id = state.get("active_turn_id")
                env = proto.lifecycle("run.failed", turn_id=turn_id, thread_id=state.get("thread_id"),
                                      error=f"codex app-server exited (code {code}) while the turn was active; "
                                            "outcome unknown — inspect the worktree before re-sending",
                                      app_server_exit=True, descendants_stopped=group_ok)
                self.append_event(session, env, state)
                turn_state = next((t for t in reversed(state.get("turns") or []) if t.get("turn_id") == turn_id), None)
                self._settle_turn(state, turn_state, env, release_claim=group_ok)
                state["status"] = "unknown"
                if turn_state is not None:
                    turn_state["status"] = "unknown"
                state["completions"][-1]["status"] = "unknown"
                if not group_ok:
                    state["claim_retained"] = ("descendant cleanup not proven (uncontained child, or the owned "
                                               "scope cgroup is not empty); worktree claim kept")
                self.save(state)
            self._server = None

    def reconcile_on_boot(self) -> None:
        """A previous controller died: its app-server child died with it, so
        any session still recorded with an active turn has an UNKNOWN
        outcome. Settle it honestly (never re-run the prompt) and drop its
        claim so the worktree is not held by a ghost."""
        with self._lock:
            # The previous controller's app-server scopes may have orphaned
            # tool descendants: stop every recorded owned scope (by unique
            # unit name — never a persisted pid) and release claims only when
            # all of them are proven empty.
            survivors = self._stop_all_known_units(grace_s=5.0)
            for session, state in self._active_sessions().items():
                turn_id = state.get("active_turn_id")
                env = proto.lifecycle(
                    "run.failed", turn_id=turn_id, thread_id=state.get("thread_id"),
                    error="codex controller restarted while the turn was active; outcome unknown — inspect the "
                          "worktree before re-sending", controller_restart=True, descendants_stopped=not survivors,
                )
                self.append_event(session, env, state)
                turn_state = next((t for t in reversed(state.get("turns") or []) if t.get("turn_id") == turn_id), None)
                self._settle_turn(state, turn_state, env, release_claim=not survivors)
                if survivors:
                    state["claim_retained"] = (f"previous app-server scope(s) {survivors} could not be proven empty; "
                                               "worktree claim kept")
                state["status"] = "unknown"
                if turn_state is not None:
                    turn_state["status"] = "unknown"
                state["completions"][-1]["status"] = "unknown"
                self.save(state)

    def _idle_check(self) -> None:
        if self.idle_exit_s <= 0 or self._server is None:
            return
        if self._active_sessions():
            return
        if time.monotonic() - self._last_activity > self.idle_exit_s:
            with self._lock:
                srv, self._server = self._server, None
            if srv is not None:
                srv.close()

    # -- socket server ------------------------------------------------------------

    def serve(self) -> None:
        sock_path = self.state_dir / SOCKET_NAME
        if sock_path.exists():
            sock_path.unlink()
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(str(sock_path))
        os.chmod(sock_path, 0o600)
        srv.listen(16)
        srv.settimeout(0.5)
        info = self.state_dir / INFO_NAME
        info.write_text(json.dumps({"pid": os.getpid(), "birth": _pid_birth(os.getpid()), "started_at": _now(),
                                    "codex_bin": self.codex_bin, "socket": str(sock_path)}), "utf-8")
        os.chmod(info, 0o600)
        self.reconcile_on_boot()
        threading.Thread(target=self._fold_loop, name="codex-fold", daemon=True).start()
        try:
            while not self._stop.is_set():
                try:
                    conn, _ = srv.accept()
                except socket.timeout:
                    continue
                threading.Thread(target=self._serve_conn, args=(conn,), daemon=True).start()
        finally:
            # Order matters for observers: the app-server child is torn down
            # first, then the socket and info file vanish, then the process
            # exits — so "socket gone" implies "children gone".
            srv.close()
            with self._lock:
                app, self._server = self._server, None
            if app is not None:
                app.close()
            sock_path.unlink(missing_ok=True)
            info.unlink(missing_ok=True)

    def _serve_conn(self, conn: socket.socket) -> None:
        try:
            conn.settimeout(120)
            buf = b""
            while b"\n" not in buf:
                chunk = conn.recv(65536)
                if not chunk:
                    break
                buf += chunk
            line = buf.split(b"\n", 1)[0]
            if not line:
                return
            try:
                req = json.loads(line.decode("utf-8"))
            except Exception:
                resp = {"ok": False, "error": "malformed request (expected one JSON object per line)"}
            else:
                resp = self.handle(req) if isinstance(req, dict) else {"ok": False, "error": "request must be an object"}
            conn.sendall((json.dumps(resp, ensure_ascii=False, default=str) + "\n").encode("utf-8"))
        except Exception:
            logger.debug("connection failed", exc_info=True)
        finally:
            try:
                conn.close()
            except Exception:
                pass


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state-dir", required=True)
    ap.add_argument("--codex-bin", required=True)
    ap.add_argument("--codex-home", default=None)
    ap.add_argument("--env-allow", default="", help="comma-separated env var names passed to app-server")
    ap.add_argument("--idle-exit-s", type=float, default=1800.0)
    ap.add_argument("--turn-start-timeout-s", type=float, default=60.0)
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    state_dir = Path(args.state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(state_dir, 0o700)
    lock_fh = open(state_dir / LOCK_NAME, "a+")
    try:
        fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("another controller owns this state dir", file=sys.stderr)
        return 3
    ctl = Controller(state_dir, args.codex_bin, args.codex_home,
                     [e for e in args.env_allow.split(",") if e], args.idle_exit_s,
                     turn_start_timeout_s=args.turn_start_timeout_s)
    def _on_signal(signum, _frame):
        logger.info("signal %s received; shutting down", signum)
        ctl._stop.set()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)
    ctl.serve()
    return 0


if __name__ == "__main__":
    sys.exit(main())
