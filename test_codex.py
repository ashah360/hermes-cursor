"""Codex backend tests: real controller process + scripted app-server.

Every test here runs the REAL ``codex_controller.py`` as a separate OS
process (started the way the gateway starts it, detached) talking to the
fake app-server in ``fixtures/``. Nothing is mocked in-process.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

import plugins.ghost_cursor as plugin
from plugins.ghost_cursor import codex_backend as cb
from plugins.ghost_cursor import codex_client as cc
from plugins.ghost_cursor import eventlog as _eventlog
from plugins.ghost_cursor import handles as _handles
from plugins.ghost_cursor import jobs as _jobs
from plugins.ghost_cursor import render as _render
from plugins.ghost_cursor import writer_guard as guard

HERE = Path(__file__).resolve().parent
FAKE = HERE / "fixtures" / "fake_codex_app_server.py"


def _drain():
    from tools.process_registry import process_registry

    out = []
    while not process_registry.completion_queue.empty():
        out.append(process_registry.completion_queue.get_nowait())
    return out


def _git_repo(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-q", str(path)], check=True)
    return path


def _dead(pid):
    """Gone, or a zombie our own process has not reaped yet."""
    try:
        with open(f"/proc/{pid}/stat") as fh:
            return fh.read().rsplit(")", 1)[1].split()[0] == "Z"
    except FileNotFoundError:
        return True


def _shutdown_controller(timeout=15.0):
    """Ask the controller to exit and wait for OBSERVED exit (pid dead or
    reaped), bounded. Returns True when it exited in time."""
    pid = int(cc.controller_info().get("pid") or 0)
    try:
        cc.request("shutdown", timeout=5.0)
    except Exception:
        return not pid or _dead(pid)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if (not pid or _dead(pid)) and not (cc.state_dir() / "control.sock").exists():
            return True
        time.sleep(0.05)
    return False


@pytest.fixture
def codex_env(monkeypatch, tmp_path):
    monkeypatch.setenv(cc.CODEX_BIN_ENV, str(FAKE))
    monkeypatch.setattr(cc, "_systemd_available", lambda: False)  # detached path: no user units from tests
    monkeypatch.setattr(cc, "CONTROLLER_START_WAIT_S", 20.0)
    monkeypatch.setattr(cb, "STOP_WAIT_S", 10.0)
    monkeypatch.setattr(_handles, "_table", {})
    monkeypatch.setattr(_handles, "_loaded", False)
    _eventlog._reset_for_tests()
    cc._reset_for_tests()
    _drain()
    repo = _git_repo(tmp_path / "repo")
    yield repo
    cc.follower.stop()
    assert _shutdown_controller(), "controller did not exit within the bound after shutdown"
    _drain()


def _wait(pred, timeout=15.0, step=0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(step)
    return False


def _controller_state(name):
    return (cc.request("status", session=cc.session_ref(name)).get("state") or {})


def _settled(name):
    return lambda: not _controller_state(name).get("active_turn_id")


def _gw_log(name):
    """The gateway-side event log after pulling the controller's new events."""
    cc.follower.ingest(name, _handles.get(name) or {})
    return (_eventlog.read_events(name, offset=0, limit=500) or {}).get("events") or []


def _deliveries():
    """Run follower passes until the controller has no undelivered completions."""
    for _ in range(50):
        cc.follower.sync_once()
        if not cc.request("completions").get("completions"):
            break
        time.sleep(0.1)
    return _drain()


# ---------------------------------------------------------------------------
# Gate 2: integrated fake-protocol workflow
# ---------------------------------------------------------------------------

def test_workflow_start_stream_followup_steer_stop_decline(codex_env):
    repo = codex_env
    ack = cb.codex_create_session(title="Codex calc work", repo=str(repo), model="test-model", effort="high")
    assert "session: Codex calc work" in ack and "codex:local" in ack
    assert _handles.backend_of(_handles.get("Codex calc work")) == "codex"
    # Cursor tools never resolve a codex handle as one of theirs and vice versa.
    assert cb.codex_create_session(title="Codex calc work", repo=str(repo), model="x").startswith("cannot create session: the title")
    assert "model is required" in cb.codex_create_session(title="No model", repo=str(repo))

    # First send: thread/start + turn/start; model/effort verified against model/list.
    out = cb.codex_send_message("Codex calc work", "add mul to calc.py")
    assert "codex turn turn_1 on thread thr_1 (new thread)" in out and "model: test-model · effort: high" in out
    entry = _handles.get("Codex calc work")
    assert entry["status"] == "running" and entry["codex_thread_id"] == "thr_1" and entry["codex_turn_id"] == "turn_1"
    assert _wait(_settled("Codex calc work"))
    events = _deliveries()
    assert [e["delegation_id"] for e in events] == ["Codex calc work#codex-turn-turn_1"]
    assert events[0]["status"] == "completed" and events[0]["result"]["files_changed"][0]["path"] == "calc.py"
    assert "Added mul() to calc.py" in events[0]["result"]["summary"]
    assert "steered" not in events[0]["result"]["summary"]
    assert _handles.get("Codex calc work")["status"] == "completed"
    log = _eventlog.read_events("Codex calc work", offset=0, limit=100)["events"]
    kinds = [(e["kind"], e.get("event") or e.get("tool") or e.get("path")) for e in log]
    assert ("tool_use", "shell") in kinds and ("tool_result", "shell") in kinds
    assert ("file_diff", "calc.py") in kinds and ("lifecycle", "run.completed") in kinds
    shell_result = next(e for e in log if e["kind"] == "tool_result" and e["tool"] == "shell")
    assert shell_result["exit_code"] == 0 and "M calc.py" in shell_result["output"] and shell_result["codex_item_id"]
    # Streamed deltas were kept; the completed agentMessage text was NOT re-emitted (no duplicate prose).
    prose = "".join(e.get("delta") or "" for e in log if e["kind"] == "content")
    assert prose.count("Added mul() to calc.py") == 1
    assert sum(1 for e in log if e["kind"] == "file_diff") == 1
    assert "status: completed" in cb.codex_status("Codex calc work")

    # Follow-up: same thread, native steer while active, then explicit stop.
    out = cb.codex_send_message("Codex calc work", "now slow follow-up")
    assert "turn turn_2 on thread thr_1 (resumed thread)" in out
    steer = cb.codex_send_message("Codex calc work", "also add tests")
    assert steer.startswith("steered the ACTIVE turn turn_2") and "expectedTurnId=turn_2" in steer
    assert "status: running" in cb.codex_status("Codex calc work")
    stop = cb.codex_stop("Codex calc work")
    assert "status: cancelled" in stop
    events = _deliveries()
    assert [e["delegation_id"] for e in events] == ["Codex calc work#codex-turn-turn_2"]
    assert events[0]["status"] == "cancelled"
    log = _eventlog.read_events("Codex calc work", offset=0, limit=200)["events"]
    assert any(e.get("event") == "steer.accepted" and e.get("turn_id") == "turn_2" for e in log)
    assert any(e.get("event") == "interrupt_requested" for e in log)
    # Second explicit stop on an idle session is idempotent.
    assert "already" in cb.codex_stop("Codex calc work") or "status:" in cb.codex_stop("Codex calc work")

    # Interactive approval: declined, recorded, turn fails clearly (no hang).
    out = cb.codex_send_message("Codex calc work", "please approve this")
    assert "turn turn_3" in out
    assert _wait(_settled("Codex calc work"))
    events = _deliveries()
    assert events[0]["status"] == "failed" and "approval declined" in (events[0]["error"] or "")
    log = _eventlog.read_events("Codex calc work", offset=0, limit=300)["events"]
    declined = [e for e in log if e.get("event") == "approval.declined"]
    assert declined and declined[0]["method"] == "item/commandExecution/requestApproval"

    # Malformed socket request: error reply, controller still alive.
    import socket

    s = socket.socket(socket.AF_UNIX)
    s.connect(str(cc.state_dir() / "control.sock"))
    s.sendall(b"not json\n")
    reply = json.loads(s.recv(4096))
    s.close()
    assert reply["ok"] is False and "malformed" in reply["error"]
    assert cc.ping() is not None

    # Catalog verification: unknown model / unsupported effort fail with the exact catalog ids.
    _handles.record("Codex calc work", model="not-a-model")
    out = cb.codex_send_message("Codex calc work", "x")
    assert "not in this Codex install's catalog" in out and "test-model" in out
    _handles.record("Codex calc work", model="test-model", effort="ultra")
    assert "effort 'ultra' is not supported" in cb.codex_send_message("Codex calc work", "x")
    assert "not tracked" not in cb.codex_list("all") and "Codex calc work" in cb.codex_list("all")


# ---------------------------------------------------------------------------
# Gate 3a: gateway-side client killed; controller continues
# ---------------------------------------------------------------------------

_GATEWAY_SCRIPT = r"""
import os, sys, json, time
sys.path.insert(0, os.environ["PLUGIN_DIR"])
import conftest  # builds the plugins.ghost_cursor shim
from plugins.ghost_cursor import codex_backend as cb, codex_client as cc
cc._systemd_available = lambda: False
cc.CONTROLLER_START_WAIT_S = 20.0
print(cb.codex_create_session(title=os.environ["SESSION"], repo=os.environ["REPO"], model="test-model"))
print(cb.codex_send_message(os.environ["SESSION"], "first slow task"))
sys.stdout.flush()
time.sleep(1.0)  # one live pass so pre-disconnect events exist gateway-side
cc.follower.ingest(os.environ["SESSION"], __import__("plugins.ghost_cursor.handles", fromlist=["x"]).get(os.environ["SESSION"]))
print("INGESTED", json.dumps(__import__("plugins.ghost_cursor.handles", fromlist=["x"]).get(os.environ["SESSION"]).get("codex_last_seq")))
sys.stdout.flush()
os.kill(os.getpid(), 9)  # the gateway dies hard, mid-turn
"""


def test_gateway_process_dies_controller_continues(codex_env, tmp_path):
    repo = codex_env
    name = "Survive gateway death"
    # The child must import the same plugin package: the Hermes tree root (or, standalone, the repo's parent)
    # goes on PYTHONPATH and conftest's shim resolves the rest.
    hermes_root = str(Path(plugin.__file__).resolve().parents[2])
    env = {**os.environ, "PLUGIN_DIR": str(HERE), "SESSION": name, "REPO": str(repo),
           "PYTHONPATH": os.pathsep.join(p for p in (hermes_root, os.environ.get("PYTHONPATH", "")) if p)}
    proc = subprocess.run([sys.executable, "-c", _GATEWAY_SCRIPT], env=env, capture_output=True, text=True, timeout=60)
    assert proc.returncode == -9, proc.stderr
    assert "codex turn turn_1 on thread thr_1" in proc.stdout
    pre_seq = int(proc.stdout.split("INGESTED", 1)[1].split()[0])
    assert pre_seq >= 3  # dispatch.accepted, run.started, tool_use at least

    # This process is a NEW gateway: it reads the handle table fresh from disk.
    _handles._table.clear()
    _handles._loaded = False
    # The controller (a third process) must still be alive.
    info = cc.controller_info()
    ctl_pid = int(info["pid"])
    os.kill(ctl_pid, 0)
    assert ctl_pid != os.getpid()
    entry = _handles.get(name)
    assert entry["status"] == "running" and entry["codex_thread_id"] == "thr_1" and entry["codex_last_seq"] == pre_seq
    state = _controller_state(name)
    assert state["thread_id"] == "thr_1" and state["active_turn_id"] == "turn_1"
    # Completion while the gateway was absent, then delivery on reconnect.
    assert _wait(_settled(name))
    before = (_eventlog.stats(name) or {}).get("total_events", 0)
    events = _deliveries()
    after = (_eventlog.stats(name) or {}).get("total_events", 0)
    assert after > before  # fresh post-reconnect events were ingested from the stable cursor
    assert [e["delegation_id"] for e in events] == [f"{name}#codex-turn-turn_1"]
    assert events[0]["status"] == "completed"
    log = _eventlog.read_events(name, offset=0, limit=200)["events"]
    codex_seqs = [e["codex_seq"] for e in log if "codex_seq" in e]
    assert codex_seqs == sorted(set(codex_seqs))  # no duplicate ingestion across the restart
    # A second turn from the new gateway yields a second, distinct completion id.
    out = cb.codex_send_message(name, "second task")
    assert "turn turn_2 on thread thr_1 (resumed thread)" in out
    assert _wait(_settled(name))
    events = _deliveries()
    assert [e["delegation_id"] for e in events] == [f"{name}#codex-turn-turn_2"]
    assert not cc.request("completions")["completions"]
    assert int(cc.controller_info()["pid"]) == ctl_pid


# ---------------------------------------------------------------------------
# Gate 3b: controller / app-server death -> honest failure, no duplicate turn
# ---------------------------------------------------------------------------

def test_app_server_crash_and_controller_kill_are_honest(codex_env):
    repo = codex_env
    name = "Crash handling"
    cb.codex_create_session(title=name, repo=str(repo), model="test-model")
    out = cb.codex_send_message(name, "crash now")
    assert "turn turn_1" in out
    assert _wait(_settled(name))
    events = _deliveries()
    assert events[0]["status"] == "failed" and "app-server exited" in events[0]["error"]
    assert _handles.get(name)["status"] == "failed"
    # The controller restarts app-server lazily; the thread resumes, no automatic re-send of "crash now".
    out = cb.codex_send_message(name, "after the restart")
    assert "turn turn_1 on thread thr_1 (resumed thread)" in out  # fresh fake app-server: turn numbering restarts
    assert _wait(_settled(name))
    events = _deliveries()
    assert events[0]["status"] == "completed" and "after the restart" in events[0]["result"]["summary"]
    assert "crash now" not in events[0]["result"]["summary"]

    # Controller killed mid-turn: status is honest, the next controller settles the turn as unknown.
    cb.codex_send_message(name, "slow before kill")
    ctl_pid = int(cc.controller_info()["pid"])
    os.kill(ctl_pid, signal.SIGKILL)
    assert _wait(lambda: _dead(ctl_pid), timeout=5)
    (cc.state_dir() / "control.sock").unlink(missing_ok=True)
    status = cb.codex_status(name)
    assert "controller not reachable" in status and "status: running" in status
    out = cb.codex_send_message(name, "after controller death")
    # Boot reconciliation settled the orphaned turn as unknown BEFORE this start was accepted.
    assert "turn turn_1 on thread thr_1 (resumed thread)" in out
    events = _deliveries()
    statuses = sorted((e["delegation_id"], e["status"]) for e in events)
    assert any("controller restarted" in (e["error"] or "") for e in events)
    assert len({d for d, _ in statuses}) == len(statuses)  # distinct ids per turn
    assert _wait(_settled(name))
    _deliveries()

    # A persisted-but-unresolved dispatch intent blocks a re-send instead of doubling the turn.
    state_path = cc.state_dir() / "sessions" / (cc.session_ref(name).replace(" ", "_").replace(":", "_") + ".json")
    st = json.loads(state_path.read_text())
    st["pending_intent"] = {"intent_id": "intent-ghost", "prompt_head": "unknown outcome", "at": time.time()}
    state_path.write_text(json.dumps(st))
    out = cb.codex_send_message(name, "would double")
    assert "cannot dispatch" in out and "intent-ghost" in out
    assert not _controller_state(name).get("active_turn_id")


# ---------------------------------------------------------------------------
# Gate 4: cross-backend writer exclusion by canonical worktree
# ---------------------------------------------------------------------------

def test_writer_guard_primitive_and_sibling_worktrees(codex_env, tmp_path, monkeypatch):
    repo = codex_env
    sibling = _git_repo(tmp_path / "sibling")
    claims = Path(cc.claims_dir())
    claims.mkdir(parents=True, exist_ok=True)
    sub = repo / "pkg"
    sub.mkdir()

    # Live-holder claim (this pid) on the worktree root blocks a Cursor local dispatch from a SUBDIR.
    def birth(pid):
        with open(f"/proc/{pid}/stat") as fh:
            return int(fh.read().rsplit(")", 1)[1].split()[19])

    claim = {"cwd": str(repo), "session": "Codex holder", "turn_id": "turn_9", "backend": "codex",
             "holder_pid": os.getpid(), "holder_birth": birth(os.getpid())}
    (claims / f"{guard.claim_key(str(repo))}.json").write_text(json.dumps(claim))
    assert guard.current(str(sub), str(claims))["session"] == "Codex holder"
    assert guard.current(str(sibling), str(claims)) is None
    _handles.record("Cursor same tree", repo=str(repo), status="created", runtime="local")
    result = plugin._send_to_session("Cursor same tree", _handles.get("Cursor same tree"), "hi", None, None)
    assert result["status"] == "rejected" and result["session"] == "Codex holder (codex backend)"
    assert "codex writer" in result["reason"]

    # Dead holder (a reaped child pid) is a stale claim: ignored.
    child = subprocess.Popen([sys.executable, "-c", "pass"])
    child.wait()
    claim["holder_pid"] = child.pid
    claim["holder_birth"] = 1
    (claims / f"{guard.claim_key(str(repo))}.json").write_text(json.dumps(claim))
    assert guard.current(str(repo), str(claims)) is None

    # A running Cursor LOCAL handle on the same tree refuses a Codex send; a cloud handle never does.
    _handles.record("Cursor same tree", status="running", runtime="local")
    assert guard.cursor_writer(str(sub)) == "Cursor same tree"
    cb.codex_create_session(title="Codex same tree", repo=str(sub), model="test-model")
    assert _handles.get("Codex same tree")["repo"] == os.path.realpath(str(repo))
    out = cb.codex_send_message("Codex same tree", "x")
    assert "already running" in out and "Cursor same tree" in out
    _handles.record("Cursor same tree", runtime="cloud")
    assert guard.cursor_writer(str(repo)) is None
    _handles.record("Cursor same tree", status="completed")

    # Sibling worktrees run concurrently on ONE controller; the live claim from a real turn blocks a
    # second Codex writer on the same tree, and the claim disappears when the turn ends.
    cb.codex_create_session(title="Codex sibling", repo=str(sibling), model="test-model")
    assert "turn turn_1" in cb.codex_send_message("Codex same tree", "slow one")
    assert "turn turn_2" in cb.codex_send_message("Codex sibling", "slow two")
    cb.codex_create_session(title="Codex second writer", repo=str(repo), model="test-model")
    out = cb.codex_send_message("Codex second writer", "x")
    assert "already running" in out and "Codex same tree" in out
    live_claim = json.loads((claims / f"{guard.claim_key(str(repo))}.json").read_text())
    assert live_claim["holder_pid"] == int(cc.controller_info()["pid"]) and live_claim["session"] == cc.session_ref("Codex same tree")
    assert _wait(_settled("Codex same tree")) and _wait(_settled("Codex sibling"))
    assert not (claims / f"{guard.claim_key(str(repo))}.json").exists()
    events = _deliveries()
    assert sorted(e["delegation_id"] for e in events) == ["Codex same tree#codex-turn-turn_1", "Codex sibling#codex-turn-turn_2"]
    # Final release: the fake app-server child is a descendant of the controller; shutdown reaps it.
    ctl_pid = int(cc.controller_info()["pid"])
    children = subprocess.run(["pgrep", "-P", str(ctl_pid)], capture_output=True, text=True).stdout.split()
    assert children, "app-server child expected under the controller"
    assert _shutdown_controller(timeout=15.0)
    assert all(_dead(int(c)) for c in children)
    assert _dead(ctl_pid)


def test_cursor_only_install_unaffected(monkeypatch):
    monkeypatch.delenv(cc.CODEX_BIN_ENV, raising=False)
    monkeypatch.setattr(cc, "_plugin_config", lambda key: None)
    monkeypatch.setattr(cc.shutil, "which", lambda name: None)
    assert cc.available() is False
    registered = []

    class Ctx:
        def register_tool(self, **kw):
            registered.append(kw)

    plugin.register(Ctx())
    names = [r["name"] for r in registered]
    assert names[:7] == [plugin.CREATE_TOOL_NAME, plugin.SEND_TOOL_NAME, plugin.STATUS_TOOL_NAME, plugin.STOP_TOOL_NAME,
                         plugin.EVENTS_TOOL_NAME, plugin.LIST_TOOL_NAME, plugin.SUBSCRIBE_TOOL_NAME]
    assert set(names[7:]) == {cb.CREATE, cb.SEND, cb.STATUS, cb.STOP, cb.EVENTS, cb.LIST, cb.SUBSCRIBE}
    codex_checks = {r["check_fn"]() for r in registered[7:]}
    assert codex_checks == {False}
    # Old handles without a backend field are Cursor records.
    _handles.record("Old cursor handle", repo="/tmp", status="completed", runtime="local")
    assert _handles.backend_of(_handles.get("Old cursor handle")) == "cursor"
    assert "unknown session" in cb.codex_status("Old cursor handle") or "Old cursor handle" not in cb.codex_list("all")
    _jobs.registry._reset_for_tests()


# ---------------------------------------------------------------------------
# Review finding 1: admission must be one atomic reservation, not check-then-act
# ---------------------------------------------------------------------------

def test_concurrent_cross_backend_admission_admits_exactly_one(codex_env, monkeypatch):
    import threading

    repo = codex_env
    # Cursor dispatch itself is stubbed (no cursor run): the reservation in
    # _send_to_session is what is under test. The stub reports "running".
    dispatched = []

    def fake_dispatch(**kw):
        dispatched.append(kw["session_name"])
        return {"success": True, "status": "running", "session": kw["session_name"]}

    monkeypatch.setattr(plugin, "_dispatch_run", fake_dispatch)
    _handles.record("Cursor racer", repo=str(repo), status="created", runtime="local")
    cb.codex_create_session(title="Codex racer", repo=str(repo), model="test-model")
    # Warm the controller so both racers hit the reservation at the same time.
    assert cc.ensure_controller() in ("detached", "systemd", "running")

    barrier = threading.Barrier(2)
    results = {}

    def cursor_side():
        barrier.wait()
        results["cursor"] = plugin._send_to_session("Cursor racer", _handles.get("Cursor racer"), "go slow", None, None)

    def codex_side():
        barrier.wait()
        results["codex"] = cb.codex_send_message("Codex racer", "go slow")

    threads = [threading.Thread(target=cursor_side), threading.Thread(target=codex_side)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=60)
    cursor_won = results["cursor"].get("status") == "running"
    codex_won = "codex turn" in results["codex"]
    assert cursor_won != codex_won, results  # exactly one writer admitted
    if cursor_won:
        assert "already running" in results["codex"] and "Cursor racer" in results["codex"]
        assert dispatched == ["Cursor racer"]
        claim = guard.current(str(repo), cc.claims_dir())
        assert claim["backend"] == "cursor" and claim["session"] == "Cursor racer"
        # Cursor's reservation is released by the job on terminal; released here by the stubbed path.
        plugin._release_worktree(str(repo), "Cursor racer", claim["owner"])
    else:
        assert results["cursor"]["status"] == "rejected" and "codex writer" in results["cursor"]["reason"]
        assert dispatched == []
        claim = guard.current(str(repo), cc.claims_dir())
        assert claim["backend"] == "codex" and claim["session"] == cc.session_ref("Codex racer")
        assert _wait(_settled("Codex racer"))
        _deliveries()
    assert guard.current(str(repo), cc.claims_dir()) is None


def test_cursor_reservation_is_released_by_job_finalize(codex_env, monkeypatch):
    """The Cursor job holds the worktree claim until _finalize (observed terminal)."""
    repo = codex_env
    monkeypatch.setattr(plugin._cloud, "derive_repo_ref", lambda p: ("https://github.com/x/y", "main"))
    _handles.record("Cursor holder", repo=str(repo), status="created", runtime="local")
    released = []

    def runner(job):
        assert guard.current(str(repo), cc.claims_dir())["session"] == "Cursor holder"
        return {"success": True, "status": "completed", "repo": job.repo, "summary": "ok", "files_changed": [],
                "files_changed_count": 0, "duration_ms": 1, "session": job.session_name}

    monkeypatch.setattr(plugin, "_execute_cursor_run", runner)
    monkeypatch.setattr(plugin, "_HANDLE_WAIT_S", 5.0)
    result = plugin._send_to_session("Cursor holder", _handles.get("Cursor holder"), "hi", None, None)
    job = _jobs.registry.get_by_name("Cursor holder")
    assert job is not None and job.done_event.wait(10)
    assert result["status"] in ("completed", "running")
    assert guard.current(str(repo), cc.claims_dir()) is None  # released on terminal
    _jobs.registry._reset_for_tests()


# ---------------------------------------------------------------------------
# Review finding 2: lost turn/start reply is ambiguous, never a second turn
# ---------------------------------------------------------------------------

def test_lost_turn_start_reply_is_ambiguous_and_adopted_not_duplicated(codex_env, monkeypatch, tmp_path):
    repo = codex_env
    inbound = tmp_path / "fake_inbound.jsonl"
    monkeypatch.setenv("FAKE_CODEX_EVENT_LOG", str(inbound))
    # The controller passes only allowlisted env to app-server (plugins.ghost_cursor.codex_env_allow).
    monkeypatch.setattr(cc, "_plugin_config", lambda key: ["FAKE_CODEX_EVENT_LOG"] if key == "codex_env_allow" else None)
    monkeypatch.setenv(cc.TURN_START_TIMEOUT_ENV, "0.5")
    name = "Lost reply"
    cb.codex_create_session(title=name, repo=str(repo), model="test-model")
    out = cb.codex_send_message(name, "lostresponse slow task")
    assert out.startswith(f"dispatch to {name} is AMBIGUOUS") and "worktree stays reserved" in out
    # Intent and claim are retained while the outcome is unknown.
    state = _controller_state(name)
    claim = guard.current(str(repo), cc.claims_dir())
    assert claim is not None and claim["session"] == cc.session_ref(name)
    assert state["pending_intent"] and state["pending_intent"]["ambiguous"] is True or state.get("active_turn_id")
    # A Cursor writer is rejected during the ambiguity; a re-send never starts a second turn.
    _handles.record("Cursor while ambiguous", repo=str(repo), status="created", runtime="local")
    res = plugin._send_to_session("Cursor while ambiguous", _handles.get("Cursor while ambiguous"), "x", None, None)
    assert res["status"] == "rejected"
    again = cb.codex_send_message(name, "second attempt")
    assert "unresolved dispatch intent" in again or again.startswith("steered the ACTIVE turn")
    # The provider DID accept: turn/started adopts the turn, the turn completes and delivers once.
    assert _wait(lambda: _controller_state(name).get("active_turn_id") or _controller_state(name).get("completions"), timeout=10)
    assert _wait(_settled(name), timeout=20)
    events = _deliveries()
    assert [e["delegation_id"] for e in events] == [f"{name}#codex-turn-turn_1"]
    assert events[0]["status"] == "completed"
    starts = [json.loads(l) for l in inbound.read_text().splitlines() if '"turn/start"' in l]
    assert len(starts) == 1, "exactly one turn/start reached the provider"
    log = _eventlog.read_events(name, offset=0, limit=300)["events"]
    assert any(e.get("event") == "dispatch.ambiguous" for e in log)
    assert any(e.get("event") == "dispatch.adopted" and e.get("turn_id") == "turn_1" for e in log)
    assert guard.current(str(repo), cc.claims_dir()) is None
    assert not _controller_state(name).get("pending_intent")


@pytest.mark.skipif(
    not __import__("plugins.ghost_cursor.codex_protocol", fromlist=["x"]).systemd_user_available(),
    reason="user systemd not available: containment cannot be tested here",
)
def test_lost_reply_with_no_turn_releases_only_after_proven_quiescence(codex_env, monkeypatch, tmp_path):
    repo = codex_env
    monkeypatch.setenv(cc.TURN_START_TIMEOUT_ENV, "0.5")
    name = "Silent provider"
    cb.codex_create_session(title=name, repo=str(repo), model="test-model")
    # Another session keeps an active turn on the SAME app-server child.
    other_repo = _git_repo(tmp_path / "other")
    cb.codex_create_session(title="Busy neighbour", repo=str(other_repo), model="test-model")
    assert "turn turn_1" in cb.codex_send_message("Busy neighbour", "hang here")
    out = cb.codex_send_message(name, "lostresponse silent")
    assert "AMBIGUOUS" in out
    assert "unresolved dispatch intent" in cb.codex_send_message(name, "again")
    status = cb.codex_status(name)
    assert "dispatch pending (outcome unknown)" in status and "codex_stop" in status
    claim_before = guard.current(str(repo), cc.claims_dir())
    assert claim_before["session"] == cc.session_ref(name)
    # Stop cannot prove the lost dispatch is idle while the shared child serves another turn:
    # intent and claim MUST stay; nothing is killed.
    stop = cb.codex_stop(name)
    assert "could NOT be proven idle" in stop and "other session" in stop
    assert guard.current(str(repo), cc.claims_dir()) == claim_before
    assert _controller_state(name)["pending_intent"]
    assert _controller_state("Busy neighbour")["active_turn_id"] == "turn_1"  # neighbour untouched
    # Free the neighbour; now the controller can tear down its own child and prove quiescence.
    assert "status: cancelled" in cb.codex_stop("Busy neighbour")
    stop = cb.codex_stop(name)
    assert "cleared the unresolved dispatch intent" in stop and "scope cgroup is empty" in stop
    assert guard.current(str(repo), cc.claims_dir()) is None
    log = _eventlog.read_events(name, offset=0, limit=100)["events"]
    cleared = [e for e in log if e.get("event") == "intent.cleared"]
    assert cleared and "cgroup is empty" in cleared[0]["proof"]
    assert json.loads((cc.state_dir() / "appserver_units.json").read_text()) == []
    assert "turn turn_" in cb.codex_send_message(name, "after clearing")  # fresh child, fresh turn
    assert _wait(_settled(name))
    _deliveries()


@pytest.mark.skipif(
    not __import__("plugins.ghost_cursor.codex_protocol", fromlist=["x"]).systemd_user_available(),
    reason="user systemd not available: containment cannot be tested here",
)
def test_app_server_death_before_turn_start_reply_is_ambiguous(codex_env, monkeypatch):
    """A pipe closed mid-request is a synthesized transport error, not a provider
    rejection: intent + claim stay until the dead child proves quiescence."""
    repo = codex_env
    name = "Died on start"
    cb.codex_create_session(title=name, repo=str(repo), model="test-model")
    out = cb.codex_send_message(name, "dieonstart")
    assert "AMBIGUOUS" in out and "app-server" in out
    assert guard.current(str(repo), cc.claims_dir())["session"] == cc.session_ref(name)
    assert _controller_state(name)["pending_intent"]["ambiguous"] is True
    log = _gw_log(name)
    assert any(e.get("event") == "dispatch.ambiguous" for e in log)
    assert not any(e.get("event") == "dispatch.failed" for e in log)
    stop = cb.codex_stop(name)
    assert "cleared the unresolved dispatch intent" in stop and "scope cgroup is empty" in stop
    assert guard.current(str(repo), cc.claims_dir()) is None


def test_definitive_provider_rejections_release_intent_and_claim(codex_env, monkeypatch):
    repo = codex_env
    monkeypatch.setenv("FAKE_CODEX_STRICT_RESUME", "1")
    monkeypatch.setattr(cc, "_plugin_config", lambda key: ["FAKE_CODEX_STRICT_RESUME"] if key == "codex_env_allow" else None)
    name = "Rejected"
    cb.codex_create_session(title=name, repo=str(repo), model="test-model")
    # turn/start answered with a JSON-RPC error: definitive.
    out = cb.codex_send_message(name, "rejectturn please")
    assert "codex dispatch failed" in out and "turn rejected by provider" in out
    assert guard.current(str(repo), cc.claims_dir()) is None
    state = _controller_state(name)
    assert not state.get("pending_intent") and not state.get("active_turn_id")
    log = _gw_log(name)
    assert any(e.get("event") == "dispatch.failed" and "rejected" in e.get("error", "") for e in log)
    # thread/resume of a thread the provider no longer knows: definitive, no turn sent.
    state_path = cc.state_dir() / "sessions" / (cc.session_ref(name).replace(" ", "_").replace(":", "_") + ".json")
    st = json.loads(state_path.read_text())
    st["thread_id"] = "thr_missing"
    state_path.write_text(json.dumps(st))
    out = cb.codex_send_message(name, "resume me")
    assert "thread/resume failed for thr_missing" in out
    assert guard.current(str(repo), cc.claims_dir()) is None
    assert not _controller_state(name).get("pending_intent")
    # The session stays usable once the recorded thread is dropped.
    st = json.loads(state_path.read_text())
    st.pop("thread_id")
    state_path.write_text(json.dumps(st))
    assert "on thread thr_2 (new thread)" in cb.codex_send_message(name, "fresh")
    assert _wait(_settled(name))
    _deliveries()


def test_steer_after_turn_finished_never_starts_a_new_turn(codex_env, monkeypatch):
    repo = codex_env
    name = "Stale steer"
    cb.codex_create_session(title=name, repo=str(repo), model="test-model")
    assert "turn turn_1" in cb.codex_send_message(name, "quick one")
    # Make the send observe an ACTIVE turn while the controller has already finished it.
    real_request = cc.request

    def stale_status(op, timeout=90.0, **fields):
        resp = real_request(op, timeout=timeout, **fields)
        if op == "status" and resp.get("ok"):
            resp["state"] = {**resp["state"], "active_turn_id": "turn_1", "pending_intent": None}
        return resp

    assert _wait(_settled(name))
    monkeypatch.setattr(cc, "request", stale_status)
    out = cb.codex_send_message(name, "follow-up")
    assert "finished before this message could be appended" in out and "NOT re-sending" in out
    monkeypatch.setattr(cc, "request", real_request)
    assert [t["turn_id"] for t in _controller_state(name)["turns"]] == ["turn_1"]
    _deliveries()


def test_same_session_concurrent_sends_admit_one_and_loser_cannot_release(codex_env):
    import threading

    repo = codex_env
    name = "Same session race"
    cb.codex_create_session(title=name, repo=str(repo), model="test-model")
    assert cc.ensure_controller() in ("detached", "systemd", "running")
    barrier = threading.Barrier(2)
    results = [None, None]

    def send(i):
        barrier.wait()
        results[i] = cb.codex_send_message(name, "slow same session")

    threads = [threading.Thread(target=send, args=(i,)) for i in range(2)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=60)
    winners = [r for r in results if "codex turn turn_1" in r]
    losers = [r for r in results if r not in winners]
    assert len(winners) == 1 and len(losers) == 1, results
    assert "already running" in losers[0] or "steered the ACTIVE turn" in losers[0]
    claim = guard.current(str(repo), cc.claims_dir())
    assert claim is not None and claim["session"] == cc.session_ref(name)
    assert claim["owner"] == _controller_state(name)["turns"][0]["intent_id"]
    # The loser's release attempt (its own intent id) cannot remove the winner's claim.
    assert guard.release(str(repo), cc.session_ref(name), cc.claims_dir(), "intent-loser") is False
    assert guard.current(str(repo), cc.claims_dir()) == claim
    assert [t["turn_id"] for t in _controller_state(name)["turns"]] == ["turn_1"]
    assert _wait(_settled(name))
    _deliveries()
    assert guard.current(str(repo), cc.claims_dir()) is None


def test_same_title_cursor_claims_are_profile_scoped(codex_env, monkeypatch, tmp_path):
    repo = codex_env
    claims = cc.claims_dir()
    claim_a, busy = guard.reserve(str(repo), "cursor", "Fix login", claims, "cursor:a:Fix login:1")
    assert claim_a is not None and busy is None and claim_a["profile"] == cc.profile_id()
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "home_b"))
    (tmp_path / "home_b" / "state").mkdir(parents=True)
    assert cc.profile_id() != claim_a["profile"]
    owner_b = f"cursor:{cc.profile_id()}:Fix login:2"
    claim_b, busy = guard.reserve(str(repo), "cursor", "Fix login", claims, owner_b)
    assert claim_b is None and busy["owner"] == "cursor:a:Fix login:1"  # same title, other profile: refused
    assert guard.release(str(repo), "Fix login", claims, owner_b) is False  # and cannot release A's claim
    assert guard.current(str(repo), claims)["owner"] == "cursor:a:Fix login:1"
    assert guard.release(str(repo), "Fix login", claims, "cursor:a:Fix login:1") is True
    assert guard.current(str(repo), claims) is None


# ---------------------------------------------------------------------------
# Review finding 3: profiles share the controller; foreign completions untouched
# ---------------------------------------------------------------------------

def test_other_profile_follower_never_acks_foreign_completion(codex_env, monkeypatch, tmp_path):
    repo = codex_env
    home_a = os.environ["HERMES_HOME"]
    home_b = str(tmp_path / "home_b")
    os.makedirs(os.path.join(home_b, "state"), exist_ok=True)

    def switch_home(home):
        monkeypatch.setenv("HERMES_HOME", home)
        _handles._table.clear()
        _handles._loaded = False
        _eventlog._reset_for_tests()
        cc._reset_for_tests()

    name = "Same title"
    # Profile A dispatches and finishes a turn but does NOT run its follower yet.
    cb.codex_create_session(title=name, repo=str(repo), model="test-model")
    assert "turn turn_1" in cb.codex_send_message(name, "profile a work")
    cc.follower.stop()
    ref_a = cc.session_ref(name)
    assert _wait(_settled(name))
    pending = cc.request("completions")["completions"]
    assert [c["session"] for c in pending] == [ref_a]

    # Profile B (different HERMES_HOME, same machine-global controller) with the SAME title.
    switch_home(home_b)
    ref_b = cc.session_ref(name)
    assert ref_b != ref_a
    sibling = _git_repo(tmp_path / "sibling_b")
    cb.codex_create_session(title=name, repo=str(sibling), model="test-model")
    assert "turn turn_2" in cb.codex_send_message(name, "profile b work")
    cc.follower.stop()
    assert _wait(_settled(name))
    events_b = _deliveries()
    # B delivered only its own completion and left A's intent pending (no ack).
    assert [e["delegation_id"] for e in events_b] == [f"{name}#codex-turn-turn_2"]
    assert events_b[0]["result"]["repo"] == os.path.realpath(str(sibling))
    still = cc.request("completions")["completions"]
    assert [c["session"] for c in still] == [ref_a]
    assert "Same title" in cb.codex_list("all") and "profile a work" not in cb.codex_status(name)  # B's own view

    # Back in profile A: its follower delivers the completion B left alone.
    switch_home(home_a)
    events_a = _deliveries()
    assert [e["delegation_id"] for e in events_a] == [f"{name}#codex-turn-turn_1"]
    assert events_a[0]["result"]["repo"] == os.path.realpath(str(repo))
    assert not cc.request("completions")["completions"]


# ---------------------------------------------------------------------------
# Review finding 4: Cursor entry points and reconciler fence Codex handles
# ---------------------------------------------------------------------------

def test_cursor_entry_points_and_reconciler_fence_codex_handles(codex_env, monkeypatch):
    from plugins.ghost_cursor import supervisor as _supervisor

    repo = codex_env
    name = "Codex only"
    cb.codex_create_session(title=name, repo=str(repo), model="test-model")
    assert "turn turn_1" in cb.codex_send_message(name, "slow work")
    entry = _handles.get(name)
    assert entry["status"] == "running" and _handles.backend_of(entry) == "codex"

    dispatched = []
    monkeypatch.setattr(plugin, "_dispatch_run", lambda **kw: dispatched.append(kw) or {"status": "running"})
    for text in (
        plugin.cursor_send_message(name, "hijack"),
        plugin.cursor_status(name),
        plugin.cursor_stop(name),
        plugin.cursor_events(name),
        plugin.cursor_subscribe(name, 60),
    ):
        assert "is a codex session" in text and "codex_* tools" in text, text
    assert dispatched == []
    assert name not in plugin.cursor_list("all")
    assert plugin._resolve_session(name) is None
    # The Cursor reconciler never adopts or supervises a running Codex handle.
    monkeypatch.setattr(_supervisor._cloud, "make_client", lambda: (_ for _ in ()).throw(AssertionError("cursor client built for a codex handle")))
    assert _supervisor.reconcile_once() == []
    assert _supervisor.ensure_supervisor(name) is None
    assert _handles.supervision_of(_handles.get(name))["phase"] == ""
    assert _wait(_settled(name))
    _deliveries()


# ---------------------------------------------------------------------------
# Gateway restart: digests/status project from the controller's durable record
# ---------------------------------------------------------------------------

def test_digest_ids_survive_follower_restart(codex_env):
    name = "Digest restart"
    cb.codex_create_session(title=name, repo=str(codex_env), model="test-model")
    assert "turn turn_1" in cb.codex_send_message(name, "hang here")
    assert _wait(lambda: any(t.get("pending_tools") for t in _controller_state(name).get("turns") or []))
    old = cc.follower
    cc._reset_for_tests()
    old._thread.join(timeout=5)
    assert not old._thread.is_alive()
    cc.follower.ingest(name, _handles.get(name))
    total = _eventlog.stats(name)["total_events"]
    assert total > 0
    keys = ("", "gw:watch")
    _handles.record(name, subscribers={key: 60.0 for key in keys})
    _drain()

    for key in keys:
        cc.follower._deliver_digest(name, _handles.get(name), key, 60.0)
    first = _drain()
    assert len(first) == 2
    assert _handles.supervision_of(_handles.get(name))["last_seq_delivered"] == dict.fromkeys(keys, total)

    cc._reset_for_tests()
    assert cc.follower.ingest(name, _handles.get(name)) == 0
    assert _eventlog.stats(name)["total_events"] == total
    assert _controller_state(name)["active_turn_id"] == "turn_1"
    for key in keys:
        cc.follower._deliver_digest(name, _handles.get(name), key, 60.0)
    second = _drain()
    assert len(second) == 2
    assert _handles.supervision_of(_handles.get(name))["last_seq_delivered"] == dict.fromkeys(keys, total)
    assert all("codex is busy inside the calls above" in event["summary"] for event in second)
    # Both followers start at 1, with no new events to distinguish the heartbeat.
    events = first + second
    assert {event["cursor_progress_update"] for event in events} == {1}
    assert len({(event["delegation_id"], event["type"]) for event in events}) == 4
    for event in events:
        key = event["session_key"]
        assert key in keys
        assert event["delegation_id"].startswith(
            f"{name}#codex-progress-1@{cc._progress.subscriber_suffix(key)}@turn_1@"
        )
        assert event["summary"].startswith(f"codex session '{name}' — progress update 1")
        assert "cursor" not in event["summary"]
    assert "status: cancelled" in cb.codex_stop(name)
    _deliveries()


@pytest.mark.parametrize("backend", ["cursor", "codex"])
@pytest.mark.parametrize("record", [
    {"kind": "tool_use", "tool": "shell", "title": "x" * 400},
    {"kind": "tool_result", "output": "x" * 400},
    {"kind": "reasoning", "text": "x" * 400},
    {"kind": "content", "delta": "x" * 400},
    {"kind": "lifecycle", "event": "failed", "error": "x" * 400},
], ids=["tool", "result", "reasoning", "content", "lifecycle"])
def test_digest_text_uses_backend_tools(backend, record):
    text = _render.digest_text(
        name="Digest text", n=1, status="running", elapsed_s=30, last_activity_s=1,
        files=[], plan=[{"content": "p" * 400, "status": "in_progress"}],
        pending_tools=[{"title": "t" * 400, "pending_s": 10}],
        events=[{**record, "seq": i} for i in range(8)], new_count=8,
        **({"backend": backend} if backend == "codex" else {}),
    )
    assert text.startswith(f"{backend} session 'Digest text' — progress update 1")
    assert text.count("full text via " + backend + "_events") >= 3
    assert f"more — {backend}_events('Digest text')" in text
    other = "cursor" if backend == "codex" else "codex"
    assert other not in text


def test_restart_projection_comes_from_controller_record(codex_env, monkeypatch):
    repo = codex_env
    name = "Projection survives"
    cb.codex_create_session(title=name, repo=str(repo), model="test-model")
    assert "turn turn_1" in cb.codex_send_message(name, "hang here")
    # Let the tool_use land, then simulate a gateway restart: brand-new Follower, empty process state.
    assert _wait(lambda: any(t.get("pending_tools") for t in _controller_state(name).get("turns") or []), timeout=10)
    cc._reset_for_tests()
    proj = cc.follower.turn_projection(name)
    assert proj["pending_tools"] and proj["pending_tools"][0]["tool"] == "shell"
    _drain()
    cc.follower._deliver_digest(name, _handles.get(name), "", 60.0)
    digests = _drain()
    assert len(digests) == 1 and "git status --short" in digests[0]["summary"]
    assert "git status --short" in cb.codex_status(name)
    # Files from a finished turn also come from the record, not from this process.
    assert "status: cancelled" in cb.codex_stop(name)
    _deliveries()
    assert "turn turn_2" in cb.codex_send_message(name, "quick")
    assert _wait(_settled(name))
    cc._reset_for_tests()
    proj = cc.follower.turn_projection(name)
    assert [f["path"] for f in proj["files"]] == ["calc.py"]
    _deliveries()


# ---------------------------------------------------------------------------
# Review finding: a cleanly exiting app-server leaves tool descendants behind
# (including setsid'd ones) — cleanup must be an owned cgroup, else fail closed
# ---------------------------------------------------------------------------

_ORPHANING_PARENT = r"""
import os, subprocess, sys
from pathlib import Path
# Behaves like an app-server whose tool shell outlives it: fork a descendant
# that leaves the process group AND session (setsid), then exit 0 on stdin EOF.
# A separate pipe lets the test stop the child after it has been reparented.
# It also expires if the test process dies before sending the stop byte.
child = subprocess.Popen([sys.executable, "-c", '''
import os, select, sys
fd = os.open(sys.argv[1], os.O_RDONLY | os.O_NONBLOCK)
os.setsid()
select.select([fd], [], [], 120)
os.close(fd)
''', str(Path(__file__).with_suffix(".stop"))])
sys.stdout.write('{"method": "fake/child", "params": {"pid": %d}}\n' % child.pid)
sys.stdout.flush()
sys.stdin.read()
sys.exit(0)
"""


def _proc_state(pid):
    try:
        with open(f"/proc/{pid}/stat") as fh:
            return fh.read().rsplit(")", 1)[1].split()[0]
    except FileNotFoundError:
        return None


def _own_session(pid):
    try:
        with open(f"/proc/{pid}/stat") as fh:
            fields = fh.read().rsplit(")", 1)[1].split()
    except FileNotFoundError:
        return False
    return int(fields[2]) == pid and int(fields[3]) == pid


@pytest.fixture
def orphaning_bin(tmp_path):
    script = tmp_path / "orphaning_parent.py"
    script.write_text(_ORPHANING_PARENT)
    wrapper = tmp_path / "fake_codex"
    wrapper.write_text(f"#!/bin/sh\nexec {sys.executable} {script}\n")
    wrapper.chmod(0o755)
    pipe = script.with_suffix(".stop")
    os.mkfifo(pipe)
    fd = os.open(pipe, os.O_RDWR | os.O_NONBLOCK)
    try:
        yield str(wrapper), fd
    finally:
        os.write(fd, b"stop")
        os.close(fd)


def _wait_child_pid(seen, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for m in seen:
            if m.get("method") == "fake/child":
                return int(m["params"]["pid"])
        time.sleep(0.05)
    raise AssertionError("descendant never reported")


_needs_user_systemd = pytest.mark.skipif(
    not __import__("plugins.ghost_cursor.codex_protocol", fromlist=["x"]).systemd_user_available(),
    reason="user systemd not available: containment cannot be tested here",
)


@_needs_user_systemd
def test_owned_scope_stops_setsid_descendant_after_clean_parent_exit(orphaning_bin):
    import plugins.ghost_cursor.codex_protocol as proto

    seen = []
    unit = proto.new_unit_name()
    bin_path, _ = orphaning_bin
    srv = proto.AppServer(bin_path, on_notification=seen.append, unit=unit)
    try:
        child_pid = _wait_child_pid(seen)
        assert _proc_state(child_pid) in ("R", "S")
        assert _wait(lambda: _own_session(child_pid), timeout=5)  # setsid done: its own pgrp AND session
        assert child_pid in (proto.unit_procs(unit) or [])  # ...but still inside the owned cgroup
        proven = srv.close(grace_s=1.0)
        assert srv.returncode() == 0  # parent exited cleanly: not evidence
        assert proven is True
        assert proto.unit_procs(unit) == []
        assert _proc_state(child_pid) in (None, "Z")
        assert proto.unit_state(unit)["LoadState"] == "not-found"
    finally:
        proto.stop_unit(unit, 2.0)


def test_uncontained_child_close_fails_closed(orphaning_bin):
    """Without an owned scope, close() must report that cleanup is unproven —
    a setsid descendant really does survive the process-group kill."""
    import plugins.ghost_cursor.codex_protocol as proto

    seen = []
    bin_path, stop_fd = orphaning_bin
    srv = proto.AppServer(bin_path, on_notification=seen.append, unit=None)
    child_pid = None
    try:
        child_pid = _wait_child_pid(seen)
        assert _wait(lambda: _own_session(child_pid), timeout=5)
        proven = srv.close(grace_s=0.5)
        assert srv.returncode() == 0 and srv.alive() is False
        assert proven is False  # fail closed
        assert _proc_state(child_pid) in ("R", "S")  # the escaped descendant is still running
    finally:
        srv.close(grace_s=0.5)
        os.write(stop_fd, b"stop")
        if child_pid is not None:
            assert _wait(lambda: _dead(child_pid), timeout=5), "fixture child did not exit after stop"


def test_unproven_cleanup_retains_intent_and_claim(codex_env, monkeypatch, tmp_path):
    """Real controller paths, in-process: while the owned scope cannot be
    proven empty (or the child was uncontained), claims and intents stay."""
    from plugins.ghost_cursor import codex_controller as ctl_mod

    repo = codex_env
    ctl = ctl_mod.Controller(tmp_path / "ctl_state", str(FAKE), None, [], idle_exit_s=0)
    ref = cc.session_ref("Unproven")
    ctl.save({"session": ref, "cwd": os.path.realpath(str(repo)), "next_seq": 0, "turns": [], "completions": [],
              "pending_intent": {"intent_id": "intent-x", "ambiguous": True, "at": time.time()}})
    with ctl._claim_locked(str(repo)):
        ctl.write_claim(str(repo), ref, "pending", owner="intent-x")
    unit = ctl_mod.proto.UNIT_PREFIX + "unprovable0001.scope"
    ctl._record_unit(unit)

    class Stuck:
        pid = 1

        def __init__(self, unit):
            self.unit = unit

        def contained(self):
            return bool(self.unit)

        def close(self, grace_s=5.0):
            return False

    ctl._server = Stuck(unit)
    monkeypatch.setattr(ctl_mod.proto, "stop_unit", lambda u, g: False)
    resp = ctl._reconcile_pending(ctl.load(ref), ctl.load(ref)["pending_intent"])
    assert resp["status"] == "unresolved" and resp["surviving_units"] == [unit]
    assert ctl.load(ref)["pending_intent"]["intent_id"] == "intent-x"
    assert ctl._read_claim(str(repo))["owner"] == "intent-x"
    # Uncontained child (detached fallback): the PERSISTED marker, not the in-memory
    # server, keeps this unresolved — also with no server object at all (restart).
    ctl._units_file().write_text(json.dumps([ctl_mod.UNCONTAINED_MARK + "abc123"]))
    ctl._server = None
    resp = ctl._reconcile_pending(ctl.load(ref), ctl.load(ref)["pending_intent"])
    assert resp["status"] == "unresolved" and resp["containment"] == "none" and "cannot be proven" in resp["reason"]
    assert ctl._read_claim(str(repo))["owner"] == "intent-x"
    ctl._units_file().write_text("[]")
    # _on_exit with an unproven scope: the turn settles unknown but the claim is kept.
    ctl._record_unit(unit)
    ctl.save({**ctl.load(ref), "pending_intent": None, "active_turn_id": "turn_z",
              "turns": [{"turn_id": "turn_z", "intent_id": "intent-x", "status": "running", "started_at": time.time()}]})
    dead = Stuck(unit)
    ctl._server = dead
    ctl._on_exit((1, dead))
    st = ctl.load(ref)
    assert st["active_turn_id"] is None and st["completions"][-1]["status"] == "unknown"
    assert "worktree claim kept" in st["claim_retained"]
    assert ctl._read_claim(str(repo))["owner"] == "intent-x"
    # Once the scope is provably empty, the same reconciliation releases everything.
    monkeypatch.setattr(ctl_mod.proto, "stop_unit", lambda u, g: True)
    ctl.save({**ctl.load(ref), "pending_intent": {"intent_id": "intent-x", "ambiguous": True}})
    resp = ctl._reconcile_pending(ctl.load(ref), ctl.load(ref)["pending_intent"])
    assert resp.get("intent_cleared") == "intent-x" and "cgroup is empty" in resp["proof"]
    assert ctl._read_claim(str(repo)) is None and ctl._known_units() == []


def test_stale_recorded_ids_never_signal_unrelated_processes(codex_env, tmp_path):
    from plugins.ghost_cursor import codex_controller as ctl_mod

    ctl = ctl_mod.Controller(tmp_path / "ctl_state", str(FAKE), None, [], idle_exit_s=0)
    bystander = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], start_new_session=True)
    try:
        # A legacy naked pgid (the bystander's), a stale owned unit that no longer
        # exists, and a unit name this controller never minted.
        stale_unit = ctl_mod.proto.UNIT_PREFIX + "deadbeef0000.scope"
        ctl._units_file().write_text(json.dumps([bystander.pid, stale_unit, "not-ours.scope"]))
        survivors = ctl._stop_all_known_units(grace_s=1.0)
        assert survivors == [bystander.pid, "not-ours.scope"]  # unprovable entries fail closed
        assert bystander.poll() is None and _proc_state(bystander.pid) in ("R", "S")  # alive, never signalled
        assert stale_unit not in ctl._known_units()  # a gone owned unit is proven gone
        assert ctl_mod.proto.stop_unit("not-ours.scope", 0.2) is False
    finally:
        bystander.kill()
        bystander.wait(timeout=5)


# ---------------------------------------------------------------------------
# Review finding: controller death must not free the worktree while its owned
# app-server scope still executes; failed cleanup on restart keeps blocking.
# ---------------------------------------------------------------------------

@_needs_user_systemd
def test_controller_death_keeps_worktree_reserved_until_proven_cleanup(codex_env, monkeypatch, tmp_path):
    import plugins.ghost_cursor.codex_protocol as proto

    repo = codex_env
    name = "Survives controller death"
    cb.codex_create_session(title=name, repo=str(repo), model="test-model")
    assert "turn turn_1" in cb.codex_send_message(name, "hang here")
    units = json.loads((cc.state_dir() / "appserver_units.json").read_text())
    assert len(units) == 1 and units[0].startswith(proto.UNIT_PREFIX)
    unit = units[0]
    assert proto.unit_procs(unit), "owned app-server scope should be running"
    claim = guard.current(str(repo), cc.claims_dir())
    assert claim["durable"] is True and claim["session"] == cc.session_ref(name)
    owner = claim["owner"]
    try:
        # Kill the controller hard. Its scope (fake app-server + hanging turn) keeps running.
        ctl_pid = int(cc.controller_info()["pid"])
        os.kill(ctl_pid, signal.SIGKILL)
        assert _wait(lambda: _dead(ctl_pid), timeout=5)
        (cc.state_dir() / "control.sock").unlink(missing_ok=True)
        assert proto.unit_procs(unit), "live owned execution remains after controller death"
        # Admission BEFORE restart: the durable claim still blocks both backends.
        assert guard.current(str(repo), cc.claims_dir())["owner"] == owner
        _handles.record("Cursor after crash", repo=str(repo), status="created", runtime="local")
        res = plugin._send_to_session("Cursor after crash", _handles.get("Cursor after crash"), "x", None, None)
        assert res["status"] == "rejected" and "codex" in res["session"], res
        cb.codex_create_session(title="Codex after crash", repo=str(repo), model="test-model")
        # (codex send restarts the controller; poison the unit record first so boot cleanup cannot be proven)
        (cc.state_dir() / "appserver_units.json").write_text(json.dumps([unit, 4242]))
        out = cb.codex_send_message("Codex after crash", "x")
        assert "already running" in out and name in out, out
        # Boot reconciliation ran: the real owned scope was stopped and proven, the
        # non-minted entry could not be, so the turn settled unknown WITH the claim kept.
        assert proto.unit_procs(unit) == []
        st = _controller_state(name)
        assert st["active_turn_id"] is None and st["completions"][-1]["status"] == "unknown"
        assert "worktree claim kept" in st["claim_retained"]
        assert json.loads((cc.state_dir() / "appserver_units.json").read_text()) == [4242]
        assert guard.current(str(repo), cc.claims_dir())["owner"] == owner
        res = plugin._send_to_session("Cursor after crash", _handles.get("Cursor after crash"), "x", None, None)
        assert res["status"] == "rejected"
        # An explicit stop cannot prove cleanup either: still unresolved, still reserved.
        stop = cb.codex_stop(name)
        assert "could NOT be proven idle" in stop or "cannot be proven" in stop, stop
        assert guard.current(str(repo), cc.claims_dir())["owner"] == owner
        # Operator resolves the unprovable record; the same stop path now proves cleanup and releases.
        (cc.state_dir() / "appserver_units.json").write_text("[]")
        stop = cb.codex_stop(name)
        assert "is now released" in stop and "cgroup is empty" in stop, stop
        assert guard.current(str(repo), cc.claims_dir()) is None
        events = _deliveries()
        assert any(e["delegation_id"] == f"{name}#codex-turn-turn_1" and e["status"] == "failed" for e in events)
        # Admission after proven cleanup.
        res = plugin._send_to_session("Cursor after crash", _handles.get("Cursor after crash"), "x", None, None)
        assert res["status"] != "rejected"  # admitted (the cursor run itself fails fast here: no API key)
        job = _jobs.registry.get_by_name("Cursor after crash")
        if job is not None:
            job.done_event.wait(10)  # its finalize releases the cursor claim
        left = guard.current(str(repo), cc.claims_dir())
        if left is not None:
            plugin._release_worktree(str(repo), "Cursor after crash", left["owner"])
        assert "turn turn_" in cb.codex_send_message("Codex after crash", "quick")
        assert _wait(_settled("Codex after crash"))
        _deliveries()
    finally:
        proto.stop_unit(unit, 3.0)


# ---------------------------------------------------------------------------
# Review finding: retrying a retained claim must not stop siblings' work, and
# uncontained uncertainty must survive a controller restart.
# ---------------------------------------------------------------------------

@_needs_user_systemd
def test_retained_claim_retry_leaves_sibling_session_running(codex_env, tmp_path):
    import plugins.ghost_cursor.codex_protocol as proto

    repo = codex_env
    repo_b = _git_repo(tmp_path / "repo_b")
    cb.codex_create_session(title="Session A", repo=str(repo), model="test-model")
    cb.codex_create_session(title="Session B", repo=str(repo_b), model="test-model")
    assert "turn turn_1" in cb.codex_send_message("Session A", "hang here")
    unit_a = json.loads((cc.state_dir() / "appserver_units.json").read_text())[0]
    owner_a = guard.current(str(repo), cc.claims_dir())["owner"]
    try:
        ctl_pid = int(cc.controller_info()["pid"])
        os.kill(ctl_pid, signal.SIGKILL)
        assert _wait(lambda: _dead(ctl_pid), timeout=5)
        (cc.state_dir() / "control.sock").unlink(missing_ok=True)
        # Poison the record so boot cleanup cannot be proven -> A settles unknown, claim kept.
        (cc.state_dir() / "appserver_units.json").write_text(json.dumps([unit_a, 4242]))
        assert "turn turn_" in cb.codex_send_message("Session B", "hang here")  # restarts controller; B active
        st_a = _controller_state("Session A")
        assert st_a["active_turn_id"] is None and "worktree claim kept" in st_a["claim_retained"]
        unit_b = [u for u in json.loads((cc.state_dir() / "appserver_units.json").read_text()) if isinstance(u, str) and u != unit_a][0]
        assert proto.unit_procs(unit_b), "B's owned scope executes"
        # Stop A: nothing may be stopped while B is active; A stays reserved.
        stop = cb.codex_stop("Session A")
        assert "could NOT be proven idle" in stop and "other session" in stop, stop
        assert proto.unit_procs(unit_b), "B still executing after stop A"
        assert _controller_state("Session B")["active_turn_id"]
        assert guard.current(str(repo), cc.claims_dir())["owner"] == owner_a
        assert json.loads((cc.state_dir() / "appserver_units.json").read_text()) == [unit_b, 4242] or \
            set(json.loads((cc.state_dir() / "appserver_units.json").read_text())) == {unit_b, 4242}
        # B finishes; the poisoned entry is resolved; now A's retry proves and releases.
        assert "status: cancelled" in cb.codex_stop("Session B")
        (cc.state_dir() / "appserver_units.json").write_text(json.dumps([unit_b]))
        stop = cb.codex_stop("Session A")
        assert "is now released" in stop, stop
        assert guard.current(str(repo), cc.claims_dir()) is None
        _deliveries()
    finally:
        proto.stop_unit(unit_a, 3.0)


def test_uncontained_uncertainty_persists_across_controller_restart(codex_env, monkeypatch, tmp_path):
    """An app-server that ran without an owned scope leaves a persisted marker;
    a NEW controller (no in-memory server, no live units) must still refuse to
    release claims — an empty unit list is not proof."""
    from plugins.ghost_cursor import codex_controller as ctl_mod

    repo = codex_env
    state_dir = tmp_path / "ctl_state"
    monkeypatch.setattr(ctl_mod.proto, "systemd_user_available", lambda: False)
    first = ctl_mod.Controller(state_dir, str(FAKE), None, [], idle_exit_s=0)
    srv = first.server()  # real fake app-server, uncontained
    try:
        assert first.containment == "none"
        marks = [u for u in first._known_units() if str(u).startswith(ctl_mod.UNCONTAINED_MARK)]
        assert len(marks) == 1
        ref = cc.session_ref("Uncontained run")
        first.save({"session": ref, "cwd": os.path.realpath(str(repo)), "next_seq": 0, "completions": [],
                    "active_turn_id": "turn_u", "status": "running",
                    "turns": [{"turn_id": "turn_u", "intent_id": "intent-u", "status": "running", "started_at": time.time()}]})
        with first._claim_locked(str(repo)):
            first.write_claim(str(repo), ref, "turn_u", owner="intent-u")
    finally:
        srv.close(grace_s=1.0)  # returns False: uncontained; the marker stays recorded
    # "Controller death": a fresh Controller on the same state dir with no server object.
    second = ctl_mod.Controller(state_dir, str(FAKE), None, [], idle_exit_s=0)
    assert second._server is None
    second.reconcile_on_boot()
    st = second.load(ref)
    assert st["active_turn_id"] is None and st["completions"][-1]["status"] == "unknown"
    assert "worktree claim kept" in st["claim_retained"]
    assert second._read_claim(str(repo))["owner"] == "intent-u"
    assert guard.claim_live(second._read_claim(str(repo))) is True  # durable: blocks admission
    resp = second._retry_retained_claim(second.load(ref))
    assert resp["status"] == "unresolved" and "uncontained" in resp["reason"]
    assert second._read_claim(str(repo))["owner"] == "intent-u"
    assert second._known_units() == marks  # never dropped by the controller itself
    # Only a deliberate operator resolution of the marker lets the proof succeed.
    second._units_file().write_text("[]")
    resp = second._retry_retained_claim(second.load(ref))
    assert resp.get("claim_released") is True
    assert second._read_claim(str(repo)) is None
