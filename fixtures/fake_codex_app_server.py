#!/usr/bin/env python3
"""Scripted stand-in for ``codex app-server`` (tests only).

Speaks the documented newline JSON-RPC shapes: initialize/initialized,
model/list, thread/start, thread/resume, turn/start, turn/steer,
turn/interrupt, and streams item/* + turn/* notifications. Behavior is
keyed by words in the prompt:

* default  — one shell command, one file change, agent text, completed.
* ``slow`` — 0.4s between steps (leaves room for steer/interrupt).
* ``hang`` — starts a command and waits for turn/interrupt.
* ``approve`` — asks for a command approval; a decline fails the turn.
* ``crash`` — exits the process mid-turn (controller death test).
* ``lostresponse`` — the turn runs and streams normally but the turn/start
  REPLY is delayed 2s (client-side timeout => ambiguous outcome).
  With ``silent`` too: no reply and no turn at all.

Invoked as ``fake_codex_app_server.py app-server`` (the controller appends
the ``app-server`` subcommand). ``FAKE_CODEX_EVENT_LOG`` (optional) gets
every inbound request appended, for assertions.
"""

import json
import os
import sys
import threading
import time

_lock = threading.Lock()
_threads = {}
_turn_n = 0
_active = {}  # thread_id -> {"turn_id", "interrupt": Event, "steers": []}
_initialized = False
_pending_server_requests = {}
_next_server_id = 1000

CATALOG = [
    {"id": "test-model", "model": "test-model", "displayName": "Test Model", "hidden": False,
     "defaultReasoningEffort": "medium",
     "supportedReasoningEfforts": [{"reasoningEffort": e, "description": e} for e in ("low", "medium", "high")],
     "inputModalities": ["text"], "isDefault": True},
    {"id": "test-model-hidden", "model": "test-model-hidden", "displayName": "Hidden", "hidden": True,
     "supportedReasoningEfforts": [{"reasoningEffort": "low"}]},
]


def send(msg):
    with _lock:
        sys.stdout.write(json.dumps(msg) + "\n")
        sys.stdout.flush()


def notify(method, params):
    send({"method": method, "params": params})


def log_inbound(msg):
    path = os.environ.get("FAKE_CODEX_EVENT_LOG")
    if path:
        with open(path, "a") as fh:
            fh.write(json.dumps(msg) + "\n")


def run_turn(thread_id, turn_id, prompt, ctl):
    slow = "slow" in prompt
    pause = 0.4 if slow else 0.02

    def step():
        time.sleep(pause)
        return ctl["interrupt"].is_set()

    def finish(status, error=None):
        turn = {"id": turn_id, "status": status, "items": [], "error": error}
        notify("turn/completed", {"threadId": thread_id, "turn": turn})
        with _lock:
            _active.pop(thread_id, None)

    notify("turn/started", {"threadId": thread_id, "turn": {"id": turn_id, "status": "inProgress", "items": []}})
    if "crash" in prompt:
        time.sleep(0.1)
        os._exit(1)
    item_id = f"item_{turn_id}_cmd"
    cmd_item = {"id": item_id, "type": "commandExecution", "command": ["bash", "-lc", "git status --short"],
                "cwd": "/tmp", "status": "inProgress"}
    notify("item/started", {"threadId": thread_id, "turnId": turn_id, "item": cmd_item})
    if "approve" in prompt:
        global _next_server_id
        with _lock:
            rid = _next_server_id
            _next_server_id += 1
            _pending_server_requests[rid] = threading.Event()
        send({"id": rid, "method": "item/commandExecution/requestApproval",
              "params": {"itemId": item_id, "threadId": thread_id, "turnId": turn_id, "command": ["rm", "-rf", "x"],
                         "cwd": "/tmp", "reason": "destructive"}})
        _pending_server_requests[rid].wait(timeout=10)
        notify("item/completed", {"threadId": thread_id, "turnId": turn_id,
                                  "item": {**cmd_item, "status": "declined"}})
        finish("failed", {"message": "command approval declined", "codexErrorInfo": "Other"})
        return
    if "hang" in prompt:
        ctl["interrupt"].wait(timeout=60)
        notify("item/completed", {"threadId": thread_id, "turnId": turn_id, "item": {**cmd_item, "status": "failed", "exitCode": 130}})
        finish("interrupted")
        return
    if step():
        finish("interrupted"); return
    notify("item/commandExecution/outputDelta", {"threadId": thread_id, "turnId": turn_id, "itemId": item_id, "delta": " M calc.py\n"})
    if step():
        finish("interrupted"); return
    notify("item/completed", {"threadId": thread_id, "turnId": turn_id,
                              "item": {**cmd_item, "status": "completed", "aggregatedOutput": " M calc.py\n",
                                       "exitCode": 0, "durationMs": 12}})
    if step():
        finish("interrupted"); return
    diff = "--- a/calc.py\n+++ b/calc.py\n@@ -1,2 +1,4 @@\n def add(a, b):\n     return a + b\n+def mul(a, b):\n+    return a * b\n"
    fc = {"id": f"item_{turn_id}_fc", "type": "fileChange", "status": "inProgress",
          "changes": [{"path": "calc.py", "kind": "update", "diff": diff}]}
    notify("item/started", {"threadId": thread_id, "turnId": turn_id, "item": fc})
    notify("turn/diff/updated", {"threadId": thread_id, "turnId": turn_id, "diff": diff})
    if step():
        finish("interrupted"); return
    notify("item/completed", {"threadId": thread_id, "turnId": turn_id, "item": {**fc, "status": "completed"}})
    # Streamed agent reply (deltas) followed by the completed item carrying the full text.
    msg_id = f"item_{turn_id}_msg"
    text = f"Added mul() to calc.py. Prompt seen: {prompt[:40]}"
    for steer in list(ctl["steers"]):
        text += f" | steered: {steer}"
    for part in (text[:10], text[10:]):
        notify("item/agentMessage/delta", {"threadId": thread_id, "turnId": turn_id, "itemId": msg_id, "delta": part})
        if step():
            finish("interrupted"); return
    notify("item/completed", {"threadId": thread_id, "turnId": turn_id,
                              "item": {"id": msg_id, "type": "agentMessage", "text": text, "phase": "final_answer"}})
    finish("completed")


def handle(msg):
    global _initialized, _turn_n
    log_inbound(msg)
    rid = msg.get("id")
    method = msg.get("method")
    params = msg.get("params") or {}
    if method is None and rid is not None:
        # Response to a server-initiated request (approval decline).
        ev = _pending_server_requests.pop(rid, None)
        if ev is not None:
            ev.set()
        return
    if method == "initialize":
        _initialized = True
        send({"id": rid, "result": {"userAgent": "fake-codex/0.0", "platformFamily": "unix", "platformOs": "linux"}})
        return
    if method == "initialized":
        return
    if not _initialized:
        send({"id": rid, "error": {"code": -32002, "message": "Not initialized"}})
        return
    if method == "model/list":
        data = CATALOG if params.get("includeHidden") else [m for m in CATALOG if not m.get("hidden")]
        send({"id": rid, "result": {"data": data, "nextCursor": None}})
    elif method == "thread/start":
        tid = f"thr_{len(_threads) + 1}"
        _threads[tid] = {"cwd": params.get("cwd"), "model": params.get("model")}
        send({"id": rid, "result": {"thread": {"id": tid, "sessionId": tid, "preview": "", "ephemeral": False,
                                               "modelProvider": "openai", "createdAt": int(time.time())}}})
        notify("thread/started", {"thread": {"id": tid}})
    elif method == "thread/resume":
        tid = params.get("threadId")
        if tid not in _threads:
            # Threads persist on disk in real codex; a fresh fake accepts any id.
            _threads[tid] = {}
        send({"id": rid, "result": {"thread": {"id": tid, "ephemeral": False}}})
    elif method == "turn/start":
        tid = params.get("threadId")
        if tid not in _threads:
            send({"id": rid, "error": {"code": -32600, "message": "unknown thread"}}); return
        if tid in _active:
            send({"id": rid, "error": {"code": -32600, "message": "turn already active"}}); return
        with _lock:
            _turn_n += 1
            turn_id = f"turn_{_turn_n}"
            ctl = {"turn_id": turn_id, "interrupt": threading.Event(), "steers": []}
            _active[tid] = ctl
        prompt = " ".join(i.get("text", "") for i in params.get("input", []) if i.get("type") == "text")
        reply = {"id": rid, "result": {"turn": {"id": turn_id, "status": "inProgress", "items": [], "error": None}}}
        if "lostresponse" in prompt:
            if "silent" in prompt:
                with _lock:
                    _active.pop(tid, None)
                return  # provider never answers and never starts a turn
            threading.Thread(target=lambda: (time.sleep(2.0), send(reply)), daemon=True).start()
        else:
            send(reply)
        threading.Thread(target=run_turn, args=(tid, turn_id, prompt, ctl), daemon=True).start()
    elif method == "turn/steer":
        ctl = _active.get(params.get("threadId"))
        if ctl is None:
            send({"id": rid, "error": {"code": -32600, "message": "no active turn"}}); return
        if params.get("expectedTurnId") != ctl["turn_id"]:
            send({"id": rid, "error": {"code": -32600, "message": "expectedTurnId mismatch"}}); return
        ctl["steers"].append(" ".join(i.get("text", "") for i in params.get("input", [])))
        send({"id": rid, "result": {"turnId": ctl["turn_id"]}})
    elif method == "turn/interrupt":
        ctl = _active.get(params.get("threadId"))
        if ctl is None or ctl["turn_id"] != params.get("turnId"):
            send({"id": rid, "error": {"code": -32600, "message": "no such active turn"}}); return
        ctl["interrupt"].set()
        send({"id": rid, "result": {}})
    else:
        send({"id": rid, "error": {"code": -32601, "message": f"unknown method {method}"}})


def main():
    if os.environ.get("FAKE_CODEX_VERSION_ONLY"):
        print("fake-codex 0.0.0")
        return
    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue
        try:
            msg = json.loads(raw)
        except Exception:
            continue
        handle(msg)


if __name__ == "__main__":
    main()
