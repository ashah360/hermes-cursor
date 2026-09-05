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


def _shutdown_controller():
    try:
        cc.request("shutdown", timeout=5.0)
    except Exception:
        return
    for _ in range(50):
        if not (cc.state_dir() / "control.sock").exists():
            return
        time.sleep(0.1)


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
    _shutdown_controller()
    _drain()


def _wait(pred, timeout=15.0, step=0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(step)
    return False


def _dead(pid):
    """Gone, or a zombie our own process has not reaped yet."""
    try:
        with open(f"/proc/{pid}/stat") as fh:
            return fh.read().rsplit(")", 1)[1].split()[0] == "Z"
    except FileNotFoundError:
        return True


def _controller_state(name):
    return (cc.request("status", session=name).get("state") or {})


def _settled(name):
    return lambda: not _controller_state(name).get("active_turn_id")


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
    state_path = cc.state_dir() / "sessions" / (name.replace(" ", "_") + ".json")
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
    assert guard.codex_writer(str(sub), str(claims))["session"] == "Codex holder"
    assert guard.codex_writer(str(sibling), str(claims)) is None
    _handles.record("Cursor same tree", repo=str(repo), status="created", runtime="local")
    result = plugin._send_to_session("Cursor same tree", _handles.get("Cursor same tree"), "hi", None, None)
    assert result["status"] == "rejected" and result["session"] == "Codex holder"
    assert "codex" in result["reason"]

    # Dead holder (a reaped child pid) is a stale claim: ignored.
    child = subprocess.Popen([sys.executable, "-c", "pass"])
    child.wait()
    claim["holder_pid"] = child.pid
    claim["holder_birth"] = 1
    (claims / f"{guard.claim_key(str(repo))}.json").write_text(json.dumps(claim))
    assert guard.codex_writer(str(repo), str(claims)) is None

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
    assert live_claim["holder_pid"] == int(cc.controller_info()["pid"]) and live_claim["session"] == "Codex same tree"
    assert _wait(_settled("Codex same tree")) and _wait(_settled("Codex sibling"))
    assert not (claims / f"{guard.claim_key(str(repo))}.json").exists()
    events = _deliveries()
    assert sorted(e["delegation_id"] for e in events) == ["Codex same tree#codex-turn-turn_1", "Codex sibling#codex-turn-turn_2"]
    # Final release: the fake app-server child is a descendant of the controller; shutdown reaps it.
    ctl_pid = int(cc.controller_info()["pid"])
    children = subprocess.run(["pgrep", "-P", str(ctl_pid)], capture_output=True, text=True).stdout.split()
    assert children, "app-server child expected under the controller"
    _shutdown_controller()
    assert _wait(lambda: all(_dead(int(c)) for c in children), timeout=10)
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
