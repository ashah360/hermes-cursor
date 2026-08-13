"""Tests for the ghost_cursor plugin (v0.6: REST+SSE cloud transport).

Six tools: ``cursor_create_session`` / ``cursor_send_message`` /
``cursor_status`` / ``cursor_stop`` / ``cursor_events`` / ``cursor_list`` —
all keyed on caller-provided meaningful session titles (cursor agent ids
resolve as aliases). Every tool returns plain text (labeled headers, prose,
raw fenced diffs, TSV) — never JSON. Covered here:

* ``cloud_runner.run_cloud`` — the REST+SSE transport against a fully faked
  REST client (happy path, native REST cancel, inactivity watchdog with
  pending-tool-call suspension, max-wall + first-event ceilings, follow-up
  + fresh-agent fallback, Last-Event-ID stream re-attach with a bounded
  budget, terminal settle via the final GET runs/{id} authority,
  unroutable-worker detection, runtime=local|cloud, missing-key preflight).
* ``events.SdkNormalizer`` — cloud_runner message dicts → canonical envelope
  mapping, including defensive parsing of the (explicitly unstable)
  tool_call payload shapes, plus a full-run fixture replay
  (``fixtures/sdk_stream.jsonl``).
* The tool handlers — session lifecycle (create → lazy first send → status
  → stop/follow-up), the read-only guarantee of ``cursor_status``, the
  same-repo concurrency guard, title handles (duplicate-title rejection,
  over-long-title rejection, deterministic repo-basename fallback) +
  agent-id alias resolution, ``cursor_events`` paging (tail defaults, negative
  offsets, kind filter, 2KB inline clip, 20KB response cap), ``cursor_list``
  TSV + scoping, model threading, completion delivery on the shared
  async-delegation rail, rejection of legacy bridge-era handles, and
  actionable prose errors for bogus/expired handles. The runner layer is
  replayed with fast deterministic fakes (no live workers, no network).
* ``handles.py`` — the persistent handle table (explicit lookup only; the
  v0.2 auto-resume heuristic is gone by design).
* The legacy ``--print`` runner + ``normalize_harness`` mapping (kept as
  fallback/reference — must stay importable and correct).

No live cursor runs.
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import plugins.ghost_cursor as gc
from plugins.ghost_cursor import eventlog as gc_eventlog
from plugins.ghost_cursor import (
    CREATE_TOOL_NAME,
    CURSOR_CREATE_SCHEMA,
    CURSOR_EVENTS_SCHEMA,
    CURSOR_LIST_SCHEMA,
    CURSOR_SEND_SCHEMA,
    CURSOR_STATUS_SCHEMA,
    CURSOR_STOP_SCHEMA,
    CURSOR_SUBSCRIBE_SCHEMA,
    EVENTS_TOOL_NAME,
    LIST_TOOL_NAME,
    SEND_TOOL_NAME,
    STATUS_TOOL_NAME,
    STOP_TOOL_NAME,
    SUBSCRIBE_TOOL_NAME,
    TOOLSET,
    _handle_cursor_create_session,
    _handle_cursor_events,
    _handle_cursor_list,
    _handle_cursor_send_message,
    _handle_cursor_status,
    _handle_cursor_stop,
    _handle_cursor_subscribe,
    check_cursor_available,
    cursor_create_session,
    cursor_events,
    cursor_list,
    cursor_send_message,
    cursor_status,
    cursor_stop,
    cursor_subscribe,
    register,
)
from plugins.ghost_cursor import cloud_runner as gc_cloud
from plugins.ghost_cursor import events as gc_events
from plugins.ghost_cursor import handles as gc_handles
from plugins.ghost_cursor import rest_client as gc_rest
from plugins.ghost_cursor import workers as gc_workers
from plugins.ghost_cursor import jobs as gc_jobs
from plugins.ghost_cursor import progress as gc_progress
from plugins.ghost_cursor import render as gc_render
from plugins.ghost_cursor import runner as gc_runner
from plugins.ghost_cursor import supervisor as gc_supervisor

FIXTURE = Path(__file__).parent / "fixtures" / "cursor_stream.jsonl"
SDK_FIXTURE = Path(__file__).parent / "fixtures" / "sdk_stream.jsonl"
# Raw tool_call SDKMessages captured VERBATIM from a real cursor-sdk run
# (2026-07-03, model gpt-5.4-nano) that created + committed a file — the
# reproduction of the "completion said no files were changed" blind spot.
SDK_EDIT_FIXTURE = Path(__file__).parent / "fixtures" / "sdk_edit_tool_call.jsonl"


def _prepend_path_env(stub_dir):
    """subprocess_env replacement that puts a stub bin dir FIRST on PATH
    (keeping the rest, so /bin utilities inside stub scripts still work)."""
    import os

    def _env():
        env = dict(os.environ)
        env["PATH"] = f"{stub_dir}:{env.get('PATH', '')}"
        return env

    return _env


# ---------------------------------------------------------------------------
# SDK fixture replay helpers
# ---------------------------------------------------------------------------

def _sdk_fixture_messages():
    """The SDKMessage dicts from the fixture stream."""
    return [
        json.loads(line)
        for line in SDK_FIXTURE.read_text().splitlines()
        if line.strip()
    ]


def _sdk_replay_events():
    """A full run's worth of sdk_runner events built from the fixture."""
    events = [(
        "cloud.session",
        {"agentId": "agent-fixture", "cwd": "/tmp/sdk_probe/repo",
         "model": "fake-model", "resumed": False},
    )]
    events.extend(("cloud.message", msg) for msg in _sdk_fixture_messages())
    events.append(("cloud.result", {"status": "finished"}))
    return events


def _replay_sdk(*_args, **_kwargs):
    yield from _sdk_replay_events()


def _normalize_all(events):
    normalizer = gc_events.SdkNormalizer()
    envs = []
    for key, obj in events:
        envs.extend(normalizer.normalize(key, obj))
    return envs


# ---------------------------------------------------------------------------
# Tool-test plumbing — deterministic gated fakes for the SDK layer
# ---------------------------------------------------------------------------

def _drain_completion_queue():
    """Pop and return every event currently on the shared completion queue."""
    from tools.process_registry import process_registry

    events = []
    while not process_registry.completion_queue.empty():
        try:
            events.append(process_registry.completion_queue.get_nowait())
        except Exception:
            break
    return events


@pytest.fixture
def clean_state(monkeypatch):
    """Fresh job registry + handle table + event-log writer state + drained
    completion queue + no live progress tickers.

    Also drops the tool-boundary interval minimum (issue #14 clamps
    sub-15s requests UP) — the timing-based tests here rely on
    sub-second digest cadences. Validation-contract tests patch their
    own minimum back in.

    cursor_create_session eagerly validates that a local checkout has a
    GitHub origin (pure `git -C`, no network) — tmp_path repos have none,
    so the introspection is stubbed here; TestCloudRunner covers the real
    derive_repo_ref failure modes."""
    monkeypatch.setattr(
        gc_cloud, "derive_repo_ref",
        lambda path: ("https://github.com/example/repo", "main"),
    )
    monkeypatch.setattr(gc_progress, "MIN_UPDATE_INTERVAL_S", 0.0)
    gc_supervisor._reset_for_tests()
    gc_progress._reset_for_tests()
    gc_jobs.registry._reset_for_tests()
    _drain_completion_queue()
    monkeypatch.setattr(gc_handles, "_table", {})
    monkeypatch.setattr(gc_handles, "_loaded", False)
    gc_eventlog._reset_for_tests()
    yield gc_jobs.registry
    gc_supervisor._reset_for_tests()
    gc_progress._reset_for_tests()
    gc_jobs.registry._reset_for_tests()
    _drain_completion_queue()
    gc_eventlog._reset_for_tests()


def _wait_until(cond, timeout=10.0, interval=0.01):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(interval)
    return cond()


def _gated_replay_factory(release, sid="agent-run", early_edit=True, late_edit=True):
    """A cancel-aware replay held open on ``release``.

    Yields the cloud.session event (the handle), optionally one early file
    edit, then blocks until ``release`` is set — honoring ``cancel_check``
    the way the real ``run_sdk`` does (a cancel mid-run resolves with
    ``status: "cancelled"``). After release: an optional second edit, a
    summary message, and a clean finished result.
    """

    def replay(task, workdir, inactivity_timeout_s=0.0, max_wall_s=0.0,
               cancel_check=None, agent_id=None, model=None,
               first_event_timeout_s=None, **_kw):
        yield ("cloud.session", {
            "agentId": sid, "cwd": str(workdir),
            "model": model or "fake-model", "resumed": bool(agent_id),
        })
        if early_edit:
            yield ("cloud.message", {
                "type": "tool_call", "call_id": "t1", "name": "edit_file",
                "status": "running", "args": {"path": f"{workdir}/f1.py"},
            })
            yield ("cloud.message", {
                "type": "tool_call", "call_id": "t1", "name": "edit_file",
                "status": "completed",
                "result": {"path": f"{workdir}/f1.py",
                           "oldText": "a\n", "newText": "a\nb\n"},
            })
        while not release.is_set():
            if cancel_check and cancel_check():
                yield ("cloud.result", {"status": "cancelled"})
                return
            time.sleep(0.01)
        if late_edit:
            yield ("cloud.message", {
                "type": "tool_call", "call_id": "t2", "name": "edit_file",
                "status": "running", "args": {"path": f"{workdir}/f2.py"},
            })
            yield ("cloud.message", {
                "type": "tool_call", "call_id": "t2", "name": "edit_file",
                "status": "completed",
                "result": {"path": f"{workdir}/f2.py",
                           "oldText": "", "newText": "new\n"},
            })
        yield ("cloud.message", {
            "type": "assistant",
            "message": {"content": [{"type": "text", "text": "all done"}]},
        })
        yield ("cloud.result", {"status": "finished"})

    return replay


def _cancel_deaf_replay_factory(release, sid="agent-deaf"):
    """A replay whose run IGNORES the cancel signal (issue #22).

    Models the lost native run.cancel(): ``cancel_check`` is never
    consulted, so a stop mid-run leaves the run executing. It yields the
    session handle plus one early edit, keeps running until ``release``,
    then streams a wrap-up message and finishes normally.
    """

    def replay(task, workdir, inactivity_timeout_s=0.0, max_wall_s=0.0,
               cancel_check=None, agent_id=None, model=None,
               first_event_timeout_s=None, **_kw):
        yield ("cloud.session", {
            "agentId": sid, "cwd": str(workdir),
            "model": model or "fake-model", "resumed": bool(agent_id),
        })
        yield ("cloud.message", {
            "type": "tool_call", "call_id": "t1", "name": "edit_file",
            "status": "running", "args": {"path": f"{workdir}/f1.py"},
        })
        yield ("cloud.message", {
            "type": "tool_call", "call_id": "t1", "name": "edit_file",
            "status": "completed",
            "result": {"path": f"{workdir}/f1.py",
                       "oldText": "a\n", "newText": "a\nb\n"},
        })
        while not release.is_set():
            time.sleep(0.01)  # deaf: never checks cancel_check
        yield ("cloud.message", {
            "type": "assistant",
            "message": {"content": [{"type": "text", "text": "kept going"}]},
        })
        yield ("cloud.result", {"status": "finished"})

    return replay


class _SdkSequence:
    """Route successive run_cloud calls to successive replay factories,
    recording each call's kwargs for assertion."""

    def __init__(self, *factories):
        self._factories = list(factories)
        self.calls = []

    def __call__(self, task, workdir, inactivity_timeout_s=0.0, max_wall_s=0.0,
                 cancel_check=None, agent_id=None, model=None,
                 runtime="local", session_title=None,
                 first_event_timeout_s=None, **_kw):
        self.calls.append({
            "task": task, "workdir": str(workdir),
            "agent_id": agent_id, "model": model,
            "runtime": runtime,
            "inactivity_timeout_s": inactivity_timeout_s,
            "max_wall_s": max_wall_s,
            "first_event_timeout_s": first_event_timeout_s,
        })
        factory = self._factories.pop(0)
        return factory(task, workdir, inactivity_timeout_s=inactivity_timeout_s,
                       max_wall_s=max_wall_s, cancel_check=cancel_check,
                       agent_id=agent_id, model=model,
                       first_event_timeout_s=first_event_timeout_s)


def _resolved(tmp_path):
    """The repo key the tools use (resolved workdir)."""
    return str(gc_runner.resolve_repo(str(tmp_path)))


def _job_for(sid):
    job = gc_jobs.registry.get_by_session(sid)
    assert job is not None, f"no job for handle {sid!r}"
    return job


def _session_name(sid):
    """The v0.4 session NAME behind a cursor sid (alias or live job)."""
    name = gc_handles.resolve(sid)
    if name:
        return name
    return _job_for(sid).session_name


def _assert_running_ack(ack):
    assert "running in background" in ack, f"not a running ack: {ack!r}"
    return ack


def _created_name(ack):
    assert ack.startswith("session: "), f"not a create ack: {ack!r}"
    return ack.splitlines()[0].split("session: ", 1)[1]


def _start_run(task, repo, model=None):
    """Test setup shorthand: create a session + send the first message."""
    name = _created_name(cursor_create_session(repo=repo, model=model))
    return cursor_send_message(name, task)


# ---------------------------------------------------------------------------
# create + first send — new runs: ack out, running status, delivery on done
# ---------------------------------------------------------------------------

class TestFirstSendRun:
    """The create + first-send flow: background-run plumbing, session-name
    handles, plain-text acks."""

    def test_new_run_returns_running_ack_and_registers_named_handle(
        self, clean_state, monkeypatch, tmp_path
    ):
        release = threading.Event()
        monkeypatch.setattr(gc_cloud, "run_cloud", _gated_replay_factory(release, sid="s-new"))

        ack = _start_run("add multiply", repo=str(tmp_path))
        try:
            _assert_running_ack(ack)

            # Live job table is addressable by the cursor sid alias...
            job = _job_for("s-new")
            assert job.status == "running"
            assert job.deliver is True  # armed: outcome must arrive as a message
            # ...the ack names the minted session...
            name = job.session_name
            assert name and f"sent to {name}" in ack
            # ...and the persistent handle table has the entry immediately,
            # keyed by name, with the cursor sid recorded as the alias.
            entry = gc_handles.get(name)
            assert entry is not None
            assert entry["repo"] == _resolved(tmp_path)
            assert entry["status"] == "running"
            assert entry["cursor_session_id"] == "s-new"
            assert gc_handles.get("s-new") == entry  # UUID alias resolves
        finally:
            release.set()
        assert job.done_event.wait(10)

    def test_completion_is_delivered_with_full_result_and_session_id(
        self, clean_state, monkeypatch, tmp_path
    ):
        monkeypatch.setattr(gc, "_resolve_session_key", lambda: "gw:test:1")
        release = threading.Event()
        monkeypatch.setattr(gc_cloud, "run_cloud", _gated_replay_factory(release, sid="s-done"))

        _assert_running_ack(_start_run("t", repo=str(tmp_path)))
        job = _job_for("s-done")
        release.set()
        assert job.done_event.wait(10)

        events = _drain_completion_queue()
        assert len(events) == 1
        evt = events[0]
        assert evt["type"] == "async_delegation"
        assert evt["delegation_id"] == job.session_name
        assert evt["session_key"] == "gw:test:1"  # routes back to the caller
        assert evt["status"] == "completed"
        assert evt["cursor_session_id"] == "s-done"
        # Continuation survives the async boundary: full result in payload.
        assert evt["result"]["success"] is True
        assert evt["result"]["session"] == job.session_name
        assert evt["result"]["session_id"] == "s-done"
        assert evt["result"]["files_changed_count"] == 2
        # The delivered text is the v0.4 completion format.
        assert "status: completed" in evt["summary"]
        assert f"session: {job.session_name}" in evt["summary"]
        assert "```diff" in evt["summary"]
        # The shared formatter renders it (this is what re-enters the chat).
        from tools.process_registry import format_process_notification
        text = format_process_notification(evt)
        assert job.session_name in text
        assert "completed" in text
        # The handle table settled to the terminal status.
        assert gc_handles.get("s-done")["status"] == "completed"

    def test_fixture_replay_run_aggregates_files_and_summary(
        self, clean_state, monkeypatch, tmp_path
    ):
        """The fixture SDK stream flows through the job aggregation: both
        edits land in files_changed with diffs, and the summary is the
        FINAL content block of the turn (the wrap-up message), not every
        interstitial fragment fused together."""
        monkeypatch.setattr(gc_cloud, "run_cloud", _replay_sdk)
        _start_run("add multiply", repo=str(tmp_path))

        job = _job_for("agent-fixture")
        assert job.done_event.wait(10)
        result = job.result
        assert result["success"] is True
        assert result["status"] == "completed"
        assert result["session_id"] == "agent-fixture"
        assert result["resumed"] is False
        assert result["files_changed_count"] == 2
        by_path = {f["path"]: f for f in result["files_changed"]}
        calc = by_path["/tmp/sdk_probe/repo/calc.py"]
        assert calc["added"] == 8 and calc["removed"] == 0
        assert calc["status"] == "M"
        assert "multiply" in calc["diff"]
        assert by_path["/tmp/sdk_probe/repo/notes.txt"]["status"] == "A"
        # The capture's final message block is the notes.txt wrap-up.
        assert result["summary"].startswith("Both done")
        assert "not deleted or touched" in result["summary"]
        # Earlier narration blocks ("Added `subtract`…", "I'll run `ls -…")
        # are NOT concatenated into the summary.
        assert "subtract" not in result["summary"]
        assert "I'll run" not in result["summary"]

    def test_handshake_failure_reports_in_turn_and_never_delivers(
        self, clean_state, monkeypatch, tmp_path
    ):
        def failing(*_a, **_k):
            raise gc_cloud.CloudRunnerError(
                "cursor agent create failed (boom)"
            )

        monkeypatch.setattr(gc_cloud, "run_cloud", failing)
        out = _start_run("t", repo=str(tmp_path))
        assert "status: failed" in out
        assert "agent create failed" in out
        # Errors are prose sentences with a next step, not codes.
        assert "send another message" in out
        # Exactly-once: the tool result IS the report; nothing enqueued.
        assert _drain_completion_queue() == []

    def test_wedged_prehandshake_run_is_cancelled_and_reported(
        self, clean_state, monkeypatch, tmp_path
    ):
        """A run that never establishes an agent gets cancelled and
        the tool returns an actionable failure instead of blocking forever."""
        monkeypatch.setattr(gc, "_HANDLE_WAIT_S", 0.3)

        def never_session(task, workdir, inactivity_timeout_s=0.0, max_wall_s=0.0,
                          cancel_check=None, agent_id=None, model=None, **_kw):
            while not (cancel_check and cancel_check()):
                time.sleep(0.01)
            return
            yield  # pragma: no cover — make it a generator

        monkeypatch.setattr(gc_cloud, "run_cloud", never_session)
        out = _start_run("t", repo=str(tmp_path))
        assert "status: failed" in out
        assert "did not establish" in out
        job = gc_jobs.registry.list_jobs()[-1]
        assert job.cancel_event.is_set()
        assert job.done_event.wait(10)
        # Never armed → no delivery for the wedged attempt.
        assert _drain_completion_queue() == []


def _preset_event():
    evt = threading.Event()
    evt.set()
    return evt


# ---------------------------------------------------------------------------
# completion summary — the final content block, not fused narration
# ---------------------------------------------------------------------------

def _narration_chunk(text):
    return ("cloud.message", {
        "type": "assistant",
        "message": {"content": [{"type": "text", "text": text}]},
    })


def _tool_round(call_id):
    return [
        ("cloud.message", {
            "type": "tool_call", "call_id": call_id, "name": "read_file",
            "status": "running", "args": {"path": "/tmp/x"},
        }),
        ("cloud.message", {
            "type": "tool_call", "call_id": call_id, "name": "read_file",
            "status": "completed", "result": {},
        }),
    ]


class TestCompletionSummary:
    """Live repro: the delivered summary was every interstitial narration
    fragment glued with no separators — "…the leftover spike file.Now let
    me explore the relevant code…" — instead of the final wrap-up message."""

    def _run_replay(self, monkeypatch, tmp_path, sid, events):
        def replay(task, workdir, inactivity_timeout_s=0.0, max_wall_s=0.0,
                   cancel_check=None, agent_id=None, model=None, **_kw):
            yield ("cloud.session", {"agentId": sid, "cwd": str(workdir),
                                   "model": "m"})
            yield from events

        monkeypatch.setattr(gc_cloud, "run_cloud", replay)
        _start_run("t", repo=str(tmp_path))
        job = _job_for(sid)
        assert job.done_event.wait(10)
        return job

    def test_summary_is_the_final_content_block_not_fused_narration(
        self, clean_state, monkeypatch, tmp_path
    ):
        events = [
            _narration_chunk(
                "I'll start by reading the brief and the leftover spike file."
            ),
            *_tool_round("t1"),
            _narration_chunk("Now let me explore the relevant code..."),
            *_tool_round("t2"),
            _narration_chunk("Now let me read the actual services..."),
            *_tool_round("t3"),
            # The final wrap-up streams as multiple deltas of ONE message —
            # those must join raw, not with injected separators.
            _narration_chunk("Implemented the retry wrapper in servi"),
            _narration_chunk("ces/http.py and added regression tests."),
            ("cloud.result", {"status": "finished"}),
        ]
        job = self._run_replay(monkeypatch, tmp_path, "s-sum", events)

        summary = job.result["summary"]
        assert summary == (
            "Implemented the retry wrapper in services/http.py "
            "and added regression tests."
        )
        # None of the interstitial narration fragments leak in.
        assert "I'll start by reading" not in summary
        assert "Now let me explore" not in summary
        # And in particular nothing fuses sentence-to-sentence.
        assert "file.Now" not in summary

        # The delivered completion carries the same final-block summary.
        events_out = _drain_completion_queue()
        assert len(events_out) == 1
        delivered = events_out[0]["summary"]
        assert "cursor's summary:" in delivered
        assert "Implemented the retry wrapper" in delivered
        assert "file.Now" not in delivered

    def test_no_final_message_falls_back_to_separated_blocks(
        self, clean_state, monkeypatch, tmp_path
    ):
        """A turn that does not END on a content block has no wrap-up to
        prefer: the fallback keeps every block, joined with blank lines so
        sentences never fuse."""
        events = [
            _narration_chunk("Reading the brief."),
            *_tool_round("t1"),
            _narration_chunk("Exploring the services."),
            *_tool_round("t2"),  # turn ends right after tool activity
            ("cloud.result", {"status": "finished"}),
        ]
        job = self._run_replay(monkeypatch, tmp_path, "s-fall", events)

        summary = job.result["summary"]
        assert summary == "Reading the brief.\n\nExploring the services."
        assert "brief.Exploring" not in summary


# ---------------------------------------------------------------------------
# resume via cursor_send_message — explicit session/load, no heuristics
# ---------------------------------------------------------------------------

class TestSendMessageResume:
    def test_legacy_bridge_era_handle_is_rejected_with_actionable_prose(
        self, clean_state, monkeypatch, tmp_path
    ):
        """A pre-migration handle (no ``runtime`` field — a bridge-era
        session whose agent id is not a cloud agent) cannot be continued
        over the cloud transport: the send is REFUSED with a pointer to
        cursor_create_session, and nothing is dispatched."""
        gc_handles.record("s-prior", repo=_resolved(tmp_path), status="completed")
        monkeypatch.setattr(
            gc_cloud, "run_cloud",
            lambda *a, **k: pytest.fail("legacy handle must not dispatch"),
        )

        out = cursor_send_message("s-prior", "continue it")
        assert "legacy bridge-era session" in out
        assert CREATE_TOOL_NAME in out
        assert gc_jobs.registry.list_jobs() == []
        assert _drain_completion_queue() == []

    def test_legacy_bracket_model_record_is_sanitized_before_the_api(
        self, clean_state, monkeypatch, tmp_path
    ):
        """A handle can carry the bracket model string verbatim (still an
        accepted create-time form); a send whose follow-up falls back to a
        fresh create must translate it to base id + params before the API
        (passing it straight through was a live BadRequestError).
        End-to-end through cursor_send_message with the REAL run_cloud
        against the fake REST client."""
        legacy = "claude-fable-5[thinking=true,context=300k,effort=high]"
        gc_handles.record(
            "s-legacy", repo=_resolved(tmp_path), status="completed",
            model=legacy, runtime="local", cursor_session_id="bc-old",
        )
        client = _FakeRestClient(
            agent_id="bc-legacy",
            followup_error=gc_rest.RestApiError(
                "cursor api POST .../runs -> 404 not_found: unknown agent",
                status_code=404, code="not_found",
            ),
        )
        _install_fake_rest(monkeypatch, client)

        cursor_send_message("s-legacy", "continue the work")
        # The fallback minted a fresh agent, so the job is addressable by
        # the NEW agent id once cloud.session lands.
        assert _wait_until(
            lambda: gc_jobs.registry.get_by_session("bc-legacy") is not None
        )
        job = _job_for("bc-legacy")
        assert job.done_event.wait(10)

        # The fallback create carried the TRANSLATED model, never the
        # bracket string.
        call = client.create_calls[0]
        assert call["model_id"] == _FABLE_BRACKET_SELECTION["id"]
        assert call["model_params"] == _FABLE_BRACKET_SELECTION["params"]
        # The handle heals: cloud.session reports the base id, which is
        # what gets recorded for the next follow-up.
        assert gc_handles.get("s-legacy")["model"] == "claude-fable-5"

    def test_expired_handle_falls_back_to_fresh_session(
        self, clean_state, monkeypatch, tmp_path
    ):
        """The runner falls back to a fresh agent for an expired id; the
        session's alias is updated to the fresh sid — no crash, no hard
        failure."""

        def fallback_replay(task, workdir, inactivity_timeout_s=0.0, max_wall_s=0.0,
                            cancel_check=None, agent_id=None, model=None, **_kw):
            # Simulates cloud_runner's follow-up → fresh-agent fallback.
            yield ("cloud.session", {"agentId": "s-fresh", "cwd": str(workdir),
                                   "model": "m", "resumed": False})
            yield ("cloud.result", {"status": "finished"})

        gc_handles.record("s-expired", repo=_resolved(tmp_path),
                          status="completed", runtime="local")
        monkeypatch.setattr(gc_cloud, "run_cloud", fallback_replay)
        cursor_send_message("s-expired", "t")
        job = _job_for("s-fresh")
        assert job.done_event.wait(10)
        assert job.result["session_id"] == "s-fresh"
        assert job.result["resumed"] is False
        # The name still resolves, now aliased to the fresh cursor sid.
        assert gc_handles.get("s-expired")["cursor_session_id"] == "s-fresh"
        assert gc_handles.resolve("s-fresh") == "s-expired"


# ---------------------------------------------------------------------------
# Model override — explicit > config > cursor default
# ---------------------------------------------------------------------------

class TestModelParam:
    def _run_and_capture(self, monkeypatch, tmp_path, sid, **start_kwargs):
        seq = _SdkSequence(_gated_replay_factory(_preset_event(), sid=sid))
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)
        _start_run("t", repo=str(tmp_path), **start_kwargs)
        _job_for(sid).done_event.wait(10)
        return seq.calls[0]

    def test_explicit_model_reaches_the_runner(self, clean_state, monkeypatch, tmp_path):
        monkeypatch.setattr(gc, "_configured_model", lambda: None)
        call = self._run_and_capture(monkeypatch, tmp_path, "s-m1",
                                     model="composer-2.5")
        assert call["model"] == "composer-2.5"

    def test_configured_model_is_the_fallback(self, clean_state, monkeypatch, tmp_path):
        monkeypatch.setattr(gc, "_configured_model", lambda: "cfg-model")
        call = self._run_and_capture(monkeypatch, tmp_path, "s-m2")
        assert call["model"] == "cfg-model"

    def test_explicit_model_beats_config(self, clean_state, monkeypatch, tmp_path):
        monkeypatch.setattr(gc, "_configured_model", lambda: "cfg-model")
        call = self._run_and_capture(monkeypatch, tmp_path, "s-m3",
                                     model="explicit-model")
        assert call["model"] == "explicit-model"

    def test_no_model_anywhere_passes_none(self, clean_state, monkeypatch, tmp_path):
        monkeypatch.setattr(gc, "_configured_model", lambda: None)
        call = self._run_and_capture(monkeypatch, tmp_path, "s-m4")
        assert call["model"] is None

    def test_session_model_lands_on_the_job_and_the_handle(
        self, clean_state, monkeypatch, tmp_path
    ):
        release = threading.Event()
        monkeypatch.setattr(gc, "_configured_model", lambda: None)
        monkeypatch.setattr(gc_cloud, "run_cloud", _gated_replay_factory(release, sid="s-m5"))
        ack = _start_run("t", repo=str(tmp_path), model="composer-x")
        try:
            _assert_running_ack(ack)
            job = _job_for("s-m5")
            assert job.model == "composer-x"  # reported by sdk.session
            assert gc_handles.get("s-m5")["model"] == "composer-x"
        finally:
            release.set()
        assert _job_for("s-m5").done_event.wait(10)


# ---------------------------------------------------------------------------
# cursor_send — interrupt + re-prompt on the same handle
# ---------------------------------------------------------------------------

class TestCursorSendMessage:
    def test_send_midflight_interrupts_and_reprompts_same_session(
        self, clean_state, monkeypatch, tmp_path
    ):
        release1 = threading.Event()  # never set: run 1 ends only via cancel
        release2 = threading.Event()
        seq = _SdkSequence(
            _gated_replay_factory(release1, sid="s-send"),
            _gated_replay_factory(release2, sid="s-send", early_edit=False),
        )
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)

        _assert_running_ack(_start_run("task A", repo=str(tmp_path)))
        first_job = _job_for("s-send")
        name = first_job.session_name
        # Let the early edit land so the interrupted partial work is real.
        assert _wait_until(lambda: first_job.files)

        ack = cursor_send_message("s-send", "also add subtract")
        try:
            _assert_running_ack(ack)
            assert f"sent to {name}" in ack
            # The ack SAYS the live prompt was interrupted.
            assert "interrupted mid-run" in ack
            assert "continuing with context" in ack

            # The old run was settled via native cancel...
            assert first_job.status == "cancelled"
            assert first_job.result["files_changed_count"] == 1
            # ...its delivery suppressed (the send ack reported it) ...
            assert _drain_completion_queue() == []
            # ...and the re-prompt continued the SAME session with the message.
            assert seq.calls[1]["agent_id"] == "s-send"
            assert seq.calls[1]["task"] == "also add subtract"
        finally:
            release2.set()

        second_job = gc_jobs.registry.get_by_session("s-send")
        assert second_job is not first_job
        assert second_job.session_name == name  # same named session
        assert second_job.done_event.wait(10)
        # The re-prompted run delivers its own completion normally.
        events = _drain_completion_queue()
        assert len(events) == 1
        assert events[0]["result"]["session_id"] == "s-send"
        assert events[0]["delegation_id"] == name

    def test_send_after_run_settled_is_a_plain_followup(
        self, clean_state, monkeypatch, tmp_path
    ):
        release2 = threading.Event()
        seq = _SdkSequence(
            _gated_replay_factory(_preset_event(), sid="s-f"),
            _gated_replay_factory(release2, sid="s-f", early_edit=False),
        )
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)

        _start_run("task A", repo=str(tmp_path))
        first_job = _job_for("s-f")
        assert first_job.done_event.wait(10)
        _drain_completion_queue()

        ack = cursor_send_message("s-f", "refine it")
        try:
            _assert_running_ack(ack)
            # No interruption happened — the run had already settled.
            assert "interrupted mid-run" not in ack
            assert seq.calls[1]["agent_id"] == "s-f"
            assert seq.calls[1]["task"] == "refine it"
        finally:
            release2.set()
        assert gc_jobs.registry.get_by_session("s-f").done_event.wait(10)

    def test_first_message_on_a_fresh_session_is_the_task(
        self, clean_state, monkeypatch, tmp_path
    ):
        """cursor_create_session dispatches NOTHING; the first send creates
        the cursor agent with the message as the task."""
        seq = _SdkSequence(_gated_replay_factory(_preset_event(), sid="s-lazy"))
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)

        ack = cursor_create_session(repo=str(tmp_path))
        name = ack.splitlines()[0].split("session: ")[1]
        assert seq.calls == []  # lazy: nothing dispatched yet
        assert gc_jobs.registry.get_by_name(name) is None
        assert gc_handles.get(name)["status"] == "created"

        cursor_send_message(name, "build the thing")
        assert seq.calls[0]["task"] == "build the thing"
        assert seq.calls[0]["agent_id"] is None  # fresh, not a resume
        job = gc_jobs.registry.get_by_name(name)
        assert job is not None
        assert job.done_event.wait(10)
        assert gc_handles.get(name)["cursor_session_id"] == "s-lazy"

    def test_send_resolves_handle_from_persisted_table_after_restart(
        self, clean_state, monkeypatch, tmp_path
    ):
        """No live job (process restart) but the handle table knows the repo:
        send re-prompts the session instead of erroring."""
        gc_handles.record("s-old", repo=_resolved(tmp_path), status="cancelled",
                          model="recorded-model", runtime="local")
        release = threading.Event()
        seq = _SdkSequence(_gated_replay_factory(release, sid="s-old",
                                                 early_edit=False))
        monkeypatch.setattr(gc, "_configured_model", lambda: None)
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)

        ack = cursor_send_message("s-old", "pick it back up")
        try:
            _assert_running_ack(ack)
            assert "sent to s-old" in ack
            assert seq.calls[0]["agent_id"] == "s-old"
            # The recorded model is reused for the continuation.
            assert seq.calls[0]["model"] == "recorded-model"
        finally:
            release.set()
        assert _job_for("s-old").done_event.wait(10)

    def test_unknown_session_is_actionable_prose(self, clean_state):
        out = cursor_send_message("s-nope", "hello")
        assert "no session named 's-nope'" in out
        assert "cursor_create_session" in out

    def test_unknown_session_error_lists_scoped_sessions_as_tsv(
        self, clean_state, monkeypatch, tmp_path
    ):
        monkeypatch.setattr(gc, "_resolve_session_key", lambda: "gw:me")
        gc_handles.record("mine-alpha-fox", repo=str(tmp_path),
                          status="completed", session_key="gw:me")
        gc_handles.record("theirs-beta-owl", repo=str(tmp_path),
                          status="completed", session_key="gw:other")
        out = cursor_send_message("s-nope", "hello")
        assert "no session named 's-nope'" in out
        assert "mine-alpha-fox" in out
        assert "theirs-beta-owl" not in out  # scoped by default
        assert "scope='all'" in out

    def test_recorded_repo_gone_is_a_graceful_error(self, clean_state, tmp_path):
        gone = tmp_path / "was-here"
        gc_handles.record("s-gone", repo=str(gone), status="cancelled",
                          runtime="local")
        out = cursor_send_message("s-gone", "hello")
        assert "no longer exists" in out

    def test_missing_args_are_clean_errors(self, clean_state):
        assert "session is required" in cursor_send_message("", "hello")
        assert "message is required" in cursor_send_message("s-x", "   ")

    def test_handler_maps_args(self, clean_state):
        out = _handle_cursor_send_message({"session": "s-nope", "message": "m"})
        assert "no session named 's-nope'" in out


# ---------------------------------------------------------------------------
# cursor_status — STRICTLY read-only (the whole point of the tool)
# ---------------------------------------------------------------------------

class TestCursorStatusReadOnly:
    def test_polling_a_live_run_never_cancels_it(self, clean_state, monkeypatch, tmp_path):
        """THE critical property: asking "how's it going?" must not kill the
        run — no cancel, no mutation, and the run still completes normally."""
        release = threading.Event()
        monkeypatch.setattr(gc_cloud, "run_cloud", _gated_replay_factory(release, sid="s-ro"))

        _assert_running_ack(_start_run("long task", repo=str(tmp_path)))
        job = _job_for("s-ro")
        name = job.session_name
        assert _wait_until(lambda: job.files)  # first diff landed

        s1 = cursor_status("s-ro")
        assert s1.startswith("status: running")
        assert f"session: {name}" in s1
        assert "working on: long task" in s1
        # Files as path + counts ONLY — no inline diffs in status output.
        assert "files so far (1)" in s1
        assert "f1.py +1 −0" in s1
        assert "+b" not in s1
        assert "cursor_events" in s1  # pointer to the diffs
        assert "elapsed:" in s1 and "last activity:" in s1

        # Poll repeatedly — still running, still untouched.
        for _ in range(3):
            assert cursor_status("s-ro").startswith("status: running")
        assert not job.cancel_event.is_set()
        assert job.deliver is True  # delivery not suppressed either

        # Read-only proof: the run keeps going and produces its normal,
        # complete result with its delivery intact.
        release.set()
        assert job.done_event.wait(10)
        assert job.status == "completed"
        assert job.result["files_changed_count"] == 2
        assert job.result["session_id"] == "s-ro"
        events = _drain_completion_queue()
        assert len(events) == 1 and events[0]["status"] == "completed"

    def test_finished_job_status_shows_summary_peek_without_diffs(
        self, clean_state, monkeypatch, tmp_path
    ):
        monkeypatch.setattr(gc_cloud, "run_cloud", _gated_replay_factory(_preset_event(),
                                                                     sid="s-fin"))
        _start_run("t", repo=str(tmp_path))
        job = _job_for("s-fin")
        assert job.done_event.wait(10)

        status = cursor_status("s-fin")
        assert status.startswith("status: completed")
        assert "summary: all done" in status  # the ~200-char peek line
        assert "files so far (2)" in status
        assert "```diff" not in status  # never inline diffs in status
        assert "reasoning" not in status  # never thinking content either

    def test_status_reports_last_activity_seconds(
        self, clean_state, monkeypatch, tmp_path
    ):
        """The header carries last activity — seconds since the last stream
        event — so a caller can spot a silent run without touching it.
        Fresh while events stream; frozen at finished_at once terminal."""
        release = threading.Event()
        monkeypatch.setattr(gc_cloud, "run_cloud",
                            _gated_replay_factory(release, sid="s-act"))

        _start_run("t", repo=str(tmp_path))
        job = _job_for("s-act")
        assert _wait_until(lambda: job.last_event_at is not None)

        live = cursor_status("s-act")
        assert live.startswith("status: running")
        header = live.splitlines()[1]
        seconds = int(header.split("last activity: ")[1].split("s ago")[0])
        assert 0 <= seconds < 10  # events just streamed — fresh, not silent

        release.set()
        assert job.done_event.wait(10)
        done = cursor_status("s-act")
        assert done.startswith("status: completed")
        # Terminal runs freeze the clock at finished_at: repeated polls of a
        # finished run must not report ever-growing silence.
        time.sleep(0.3)
        done_line = done.splitlines()[1].split("last activity: ")[1]
        again_line = cursor_status("s-act").splitlines()[1].split("last activity: ")[1]
        assert again_line == done_line

    def test_status_addresses_a_dispatched_resume_before_its_session_event(
        self, clean_state, tmp_path
    ):
        """A just-dispatched continuation is addressable by the requested
        handle in the window before sdk.session fires."""
        release = threading.Event()

        def runner(job):
            release.wait(10)
            return {"success": True, "status": "completed"}

        job, existing = gc_jobs.registry.dispatch(
            runner=runner, task="t", repo=str(tmp_path), inactivity_timeout_s=60,
            requested_session_id="s-req",
        )
        assert existing is None
        try:
            snap = cursor_status("s-req")
            assert snap.startswith("status: running")
            assert not job.cancel_event.is_set()
        finally:
            release.set()
        assert job.done_event.wait(10)

    def test_persisted_handle_without_live_job_reports_its_record(
        self, clean_state, tmp_path
    ):
        gc_handles.record("s-hist", repo=str(tmp_path), status="completed",
                          task="old task", model="m")
        status = cursor_status("s-hist")
        assert status.startswith("status: completed")
        assert "working on: old task" in status
        assert "not tracked live" in status
        assert "cursor_send_message" in status

    def test_stale_running_record_is_reconciled_not_running(self, clean_state, tmp_path):
        # A dead process can't have left a live run behind (issue #13).
        gc_handles.record("s-stale", repo=str(tmp_path), status="running")
        status = cursor_status("s-stale")
        assert status.startswith("status: failed")
        assert "orphaned: plugin process restarted mid-run" in status

    def test_unknown_session_is_actionable_prose(self, clean_state):
        out = cursor_status("s-nope")
        assert "no session named 's-nope'" in out

    def test_empty_session_is_a_clean_error(self, clean_state):
        assert "session is required" in cursor_status("")

    def test_handler_maps_args(self, clean_state):
        out = _handle_cursor_status({"session": "s-nope"})
        assert "no session named 's-nope'" in out


# ---------------------------------------------------------------------------
# orphaned-handle reconciliation — issue #13: a persisted "running" record
# with no live job in this process (the plugin process restarted mid-run)
# must settle to a terminal state, never render plain "running" forever
# ---------------------------------------------------------------------------

class TestOrphanedHandleReconciliation:
    def _seed_orphan(self, tmp_path, name="s-orphan"):
        """Exactly the post-restart world: a persisted running handle
        (written by the dead process at cloud.session) and an empty job
        registry — clean_state guarantees the latter."""
        gc_handles.record(
            name, repo=_resolved(tmp_path), status="running",
            task="long task", cursor_session_id="agent-orphan",
            runtime="local",
        )
        return name

    def test_status_reconciles_orphaned_running_handle(self, clean_state, tmp_path):
        self._seed_orphan(tmp_path)
        out = cursor_status("s-orphan")
        assert out.startswith("status: failed")
        assert "orphaned: plugin process restarted mid-run" in out
        # The persisted record is repaired, not just the rendering.
        entry = gc_handles.get("s-orphan")
        assert entry["status"] == "failed"
        assert entry["status_note"] == "orphaned: plugin process restarted mid-run"

    def test_list_reconciles_orphaned_running_handle(self, clean_state, tmp_path):
        self._seed_orphan(tmp_path)
        out = cursor_list(scope="all")
        row = next(l for l in out.splitlines() if "s-orphan" in l)
        assert "failed (orphaned: plugin process restarted mid-run)" in row
        assert "running" not in row
        assert gc_handles.get("s-orphan")["status"] == "failed"

    def test_reconciliation_fires_no_completion_delivery(self, clean_state, tmp_path):
        """No consumer exists for a run whose process died — reconciliation
        only fixes the record, it never enqueues a completion/digest."""
        self._seed_orphan(tmp_path)
        cursor_status("s-orphan")
        cursor_list(scope="all")
        assert _drain_completion_queue() == []

    def test_reconciled_session_stays_continuable(
        self, clean_state, monkeypatch, tmp_path
    ):
        """Only the run died; the agent id is durable — a follow-up send
        still resumes the reconciled session via Agent.resume."""
        self._seed_orphan(tmp_path)
        assert cursor_status("s-orphan").startswith("status: failed")

        release = threading.Event()
        seq = _SdkSequence(_gated_replay_factory(release, sid="agent-orphan",
                                                 early_edit=False))
        monkeypatch.setattr(gc, "_configured_model", lambda: None)
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)

        ack = cursor_send_message("s-orphan", "pick it back up")
        try:
            _assert_running_ack(ack)
            assert seq.calls[0]["agent_id"] == "agent-orphan"  # Agent.resume
        finally:
            release.set()
        assert _job_for("agent-orphan").done_event.wait(10)

    def test_handle_with_live_job_is_untouched(
        self, clean_state, monkeypatch, tmp_path
    ):
        release = threading.Event()
        monkeypatch.setattr(gc_cloud, "run_cloud",
                            _gated_replay_factory(release, sid="s-live"))
        _assert_running_ack(_start_run("t", repo=str(tmp_path)))
        job = _job_for("s-live")
        name = job.session_name
        try:
            assert cursor_status(name).startswith("status: running")
            row = next(
                l for l in cursor_list(scope="all").splitlines() if name in l
            )
            assert "running" in row
            assert "orphaned" not in row
            assert gc_handles.get(name)["status"] == "running"
        finally:
            release.set()
        assert job.done_event.wait(10)


# ---------------------------------------------------------------------------
# cursor_stop — graceful native cancel; idempotent on finished runs
# ---------------------------------------------------------------------------

class TestCursorStop:
    def test_stop_cancels_a_running_job_and_reports_partials(
        self, clean_state, monkeypatch, tmp_path
    ):
        release = threading.Event()  # never set: only the cancel ends the run
        monkeypatch.setattr(gc_cloud, "run_cloud", _gated_replay_factory(release, sid="s-stop"))

        _assert_running_ack(_start_run("t", repo=str(tmp_path)))
        job = _job_for("s-stop")
        name = job.session_name
        assert _wait_until(lambda: job.files)  # partial edit landed

        out = cursor_stop("s-stop")
        assert out.startswith("status: cancelled")
        assert f"session: {name} · stopped after" in out
        assert "(native cancel)" in out
        # Partial work is surfaced as counts, diffs stay in the event log...
        assert "partial work: 1 file" in out
        assert "f1.py +1 −0" in out
        assert "diffs in event log" in out
        # ...the session stays continuable...
        assert "continuable" in out
        # ...and the outcome was reported in-turn: delivery suppressed.
        assert job.status == "cancelled"
        assert _drain_completion_queue() == []
        # Handle table settled too.
        assert gc_handles.get("s-stop")["status"] == "cancelled"

    def test_stop_on_cancel_deaf_run_is_honest_and_keeps_status_running(
        self, clean_state, monkeypatch, tmp_path
    ):
        """Issue #22: a run that never honors the cancel signal must NOT
        get the confident "stopped" ack — the caller is told the run is
        still executing, and no terminal status is persisted while the
        job is live."""
        release = threading.Event()
        monkeypatch.setattr(
            gc_cloud, "run_cloud", _cancel_deaf_replay_factory(release, sid="s-deaf")
        )
        monkeypatch.setattr(gc, "_STOP_WAIT_S", 0.3)

        _assert_running_ack(_start_run("t", repo=str(tmp_path)))
        job = _job_for("s-deaf")
        name = job.session_name
        try:
            out = cursor_stop(name)
            # Honest ack: cancel signalled, run NOT observed to stop.
            assert "still executing" in out
            assert "status stays running" in out
            assert "stopped after" not in out
            assert not out.startswith("status: cancelled")
            # The cancel WAS signalled...
            assert job.cancel_event.is_set()
            # ...but nothing terminal was recorded on faith, anywhere.
            assert job.status == "running"
            assert gc_handles.get(name)["status"] == "running"
            assert cursor_status(name).startswith("status: running")
        finally:
            release.set()
        assert job.done_event.wait(10)
        # Delivery was re-armed: the REAL outcome arrives as a delivered
        # message when the run actually settles, never silently swallowed.
        events = _drain_completion_queue()
        assert len(events) == 1
        assert events[0]["status"] == "completed"
        assert gc_handles.get(name)["status"] == "completed"

    def test_stop_on_finished_run_is_graceful_and_idempotent(
        self, clean_state, monkeypatch, tmp_path
    ):
        monkeypatch.setattr(gc_cloud, "run_cloud", _gated_replay_factory(_preset_event(),
                                                                     sid="s-idem"))
        _start_run("t", repo=str(tmp_path))
        job = _job_for("s-idem")
        assert job.done_event.wait(10)
        _drain_completion_queue()

        out = cursor_stop("s-idem")
        assert out.startswith("status: completed")
        assert "already finished" in out and "nothing to stop" in out
        assert "2 files" in out
        # Stopping a finished run must not re-deliver anything.
        assert _drain_completion_queue() == []
        # Truly idempotent: a second stop reports the same.
        assert "nothing to stop" in cursor_stop("s-idem")

    def test_stop_on_persisted_handle_without_live_job_is_graceful(
        self, clean_state, tmp_path
    ):
        gc_handles.record("s-past", repo=str(tmp_path), status="completed")
        out = cursor_stop("s-past")
        assert out.startswith("status: completed")
        assert "nothing to stop" in out

    def test_stop_on_stale_running_record_is_graceful(self, clean_state, tmp_path):
        gc_handles.record("s-ghost", repo=str(tmp_path), status="running")
        out = cursor_stop("s-ghost")
        assert out.startswith("status: not running (stale record)")

    def test_unknown_session_is_actionable_prose(self, clean_state):
        out = cursor_stop("s-nope")
        assert "no session named 's-nope'" in out

    def test_empty_session_is_a_clean_error(self, clean_state):
        assert "session is required" in cursor_stop("")

    def test_handler_maps_args(self, clean_state):
        out = _handle_cursor_stop({"session": "s-nope"})
        assert "no session named 's-nope'" in out


# ---------------------------------------------------------------------------
# Terminal-status repair invariant (issue #22 belt-and-braces)
# ---------------------------------------------------------------------------

class TestTerminalStatusRepairInvariant:
    """The persisted handle status must never be terminal while the run's
    events are still folding in this process — the fold path repairs the
    record back to running and logs the contradiction."""

    def test_events_folding_repair_a_persisted_terminal_status(
        self, clean_state, monkeypatch, tmp_path
    ):
        gate_event, gate_end = threading.Event(), threading.Event()

        def replay(task, workdir, inactivity_timeout_s=0.0, max_wall_s=0.0,
                   cancel_check=None, agent_id=None, model=None,
                   first_event_timeout_s=None, **_kw):
            yield ("cloud.session", {"agentId": "s-repair", "cwd": str(workdir),
                                   "model": "m", "resumed": False})
            while not gate_event.is_set():
                time.sleep(0.01)
            yield ("cloud.message", {
                "type": "assistant",
                "message": {"content": [{"type": "text", "text": "still here"}]},
            })
            while not gate_end.is_set():
                time.sleep(0.01)
            yield ("cloud.result", {"status": "finished"})

        monkeypatch.setattr(gc_cloud, "run_cloud", replay)
        _assert_running_ack(_start_run("t", repo=str(tmp_path)))
        job = _job_for("s-repair")
        name = job.session_name
        try:
            # Simulate the issue #22 corruption: a terminal status lands
            # on the persisted handle while the run is still live here.
            gc_handles.record(name, status="cancelled")
            assert gc_handles.get(name)["status"] == "cancelled"

            gate_event.set()  # one more event folds for the live run
            assert _wait_until(
                lambda: gc_handles.get(name)["status"] == "running"
            ), "a folding event must repair the persisted terminal status"

            # The contradiction is surfaced as a lifecycle event, not
            # silently papered over (the event append trails the status
            # write by a beat — poll for it).
            def _repaired_events():
                page = gc_eventlog.read_events(name, offset=0, limit=200)
                return [e for e in (page or {}).get("events", [])
                        if e.get("event") == "status.repaired"]

            assert _wait_until(lambda: _repaired_events())
            assert _repaired_events()[0].get("was") == "cancelled"
        finally:
            gate_event.set()
            gate_end.set()
        assert job.done_event.wait(10)
        # The normal lifecycle still owns the real terminal state.
        assert gc_handles.get(name)["status"] == "completed"


# ---------------------------------------------------------------------------
# Same-repo concurrency guard
# ---------------------------------------------------------------------------

class TestSameRepoConcurrency:
    """Two cursor agents on one working tree = corruption. Reject."""

    def test_second_start_on_same_repo_is_rejected_with_existing_handle(
        self, clean_state, monkeypatch, tmp_path
    ):
        release = threading.Event()
        monkeypatch.setattr(gc_cloud, "run_cloud", _gated_replay_factory(release, sid="s-a"))
        first = _start_run("task A", repo=str(tmp_path))
        try:
            _assert_running_ack(first)
            name = _job_for("s-a").session_name

            second = _start_run("task B", repo=str(tmp_path))
            # Actionable prose: names the ACTIVE session so the caller can
            # steer/inspect it instead.
            assert "cannot start" in second
            assert f"'{name}' is already running" in second
            assert "cursor_send_message" in second
            assert "cursor_stop" in second
        finally:
            release.set()

        job = _job_for("s-a")
        assert job.done_event.wait(10)
        # Only the first job ever ran and delivered.
        assert len(_drain_completion_queue()) == 1

    def test_different_repos_run_concurrently(self, clean_state, monkeypatch, tmp_path):
        repo_a = tmp_path / "a"
        repo_b = tmp_path / "b"
        repo_a.mkdir()
        repo_b.mkdir()
        release = threading.Event()
        seq = _SdkSequence(
            _gated_replay_factory(release, sid="s-ra", early_edit=False),
            _gated_replay_factory(release, sid="s-rb", early_edit=False),
        )
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)

        res_a = _start_run("a", repo=str(repo_a))
        res_b = _start_run("b", repo=str(repo_b))
        try:
            _assert_running_ack(res_a)
            _assert_running_ack(res_b)
            assert _job_for("s-ra").session_name != _job_for("s-rb").session_name
        finally:
            release.set()
        for sid in ("s-ra", "s-rb"):
            assert _job_for(sid).done_event.wait(10)

    def test_finished_run_releases_the_repo(self, clean_state, monkeypatch, tmp_path):
        seq = _SdkSequence(
            _gated_replay_factory(_preset_event(), sid="s-one"),
            _gated_replay_factory(_preset_event(), sid="s-two"),
        )
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)
        _start_run("t", repo=str(tmp_path))
        assert _job_for("s-one").done_event.wait(10)

        second = _start_run("t2", repo=str(tmp_path))
        assert "already running" not in second  # no rejection once settled
        assert _job_for("s-two").done_event.wait(10)


def _make_git_repo(path):
    """A real git repo with one commit (linked worktrees need one)."""
    path.mkdir(parents=True, exist_ok=True)
    for args in (
        ("init", "-q", "-b", "main"),
        ("add", "."),
        ("commit", "-q", "--allow-empty", "-m", "seed"),
    ):
        subprocess.run(
            ["git", "-c", "user.email=t@t", "-c", "user.name=t", *args],
            cwd=str(path), check=True, capture_output=True,
        )
    return path


class TestCanonicalWorktreeIdentity:
    """Canonical identity = realpath(git rev-parse --show-toplevel) at the
    tool boundary: /repo and /repo/subdir are ONE admission identity (and
    one persisted repo key); sibling linked worktrees stay distinct."""

    def test_create_session_records_the_worktree_toplevel(
        self, clean_state, tmp_path
    ):
        repo = _make_git_repo(tmp_path / "repo")
        sub = repo / "src" / "inner"
        sub.mkdir(parents=True)
        name = _created_name(cursor_create_session(repo=str(sub)))
        assert gc_handles.get(name)["repo"] == os.path.realpath(repo)

    def test_subdir_and_toplevel_share_one_admission_identity(
        self, clean_state, monkeypatch, tmp_path
    ):
        repo = _make_git_repo(tmp_path / "repo")
        sub = repo / "src"
        sub.mkdir()
        release = threading.Event()
        monkeypatch.setattr(
            gc_cloud, "run_cloud", _gated_replay_factory(release, sid="s-top")
        )
        first = _start_run("task at toplevel", repo=str(repo))
        try:
            _assert_running_ack(first)
            second = _start_run("task at subdir", repo=str(sub))
            assert "cannot start" in second
            assert "already running" in second
        finally:
            release.set()
        assert _job_for("s-top").done_event.wait(10)

    def test_sibling_worktrees_run_concurrently(
        self, clean_state, monkeypatch, tmp_path
    ):
        repo = _make_git_repo(tmp_path / "repo")
        wt = tmp_path / "wt-feature"
        subprocess.run(
            ["git", "worktree", "add", "-q", str(wt), "-b", "feature"],
            cwd=str(repo), check=True, capture_output=True,
        )
        release = threading.Event()
        seq = _SdkSequence(
            _gated_replay_factory(release, sid="s-main", early_edit=False),
            _gated_replay_factory(release, sid="s-wt", early_edit=False),
        )
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)
        res_main = _start_run("main task", repo=str(repo))
        res_wt = _start_run("worktree task", repo=str(wt))
        try:
            _assert_running_ack(res_main)
            _assert_running_ack(res_wt)
        finally:
            release.set()
        for sid in ("s-main", "s-wt"):
            assert _job_for(sid).done_event.wait(10)


# ---------------------------------------------------------------------------
# handles.py — persistent handle table (explicit lookup only)
# ---------------------------------------------------------------------------

@pytest.fixture
def clean_handles(monkeypatch):
    """Fresh in-memory handle table (HERMES_HOME is already a temp dir via
    the autouse _isolate_hermes_home fixture, so the JSON file is too)."""
    monkeypatch.setattr(gc_handles, "_table", {})
    monkeypatch.setattr(gc_handles, "_loaded", False)
    return gc_handles


class TestHandleTable:
    def test_record_and_get_roundtrip(self, clean_handles):
        gc_handles.record("s-1", repo="/w/repo", status="running", task="t", model="m")
        entry = gc_handles.get("s-1")
        assert entry["repo"] == "/w/repo"
        assert entry["status"] == "running"
        assert entry["model"] == "m"
        assert entry["updated_at"] > 0

    def test_record_merges_and_skips_none_fields(self, clean_handles):
        gc_handles.record("s-1", repo="/w/repo", status="running", model="m")
        gc_handles.record("s-1", status="completed", model=None)
        entry = gc_handles.get("s-1")
        assert entry["status"] == "completed"
        assert entry["repo"] == "/w/repo"  # merged, not replaced
        assert entry["model"] == "m"       # None did not clobber

    def test_get_returns_a_copy(self, clean_handles):
        gc_handles.record("s-1", status="running")
        gc_handles.get("s-1")["status"] = "mutated"
        assert gc_handles.get("s-1")["status"] == "running"

    def test_record_without_session_id_is_a_noop(self, clean_handles):
        gc_handles.record(None, repo="/w", status="running")
        gc_handles.record("", repo="/w", status="running")
        assert gc_handles._table == {}

    def test_unknown_or_missing_handle_returns_none(self, clean_handles):
        assert gc_handles.get("s-nope") is None
        assert gc_handles.get(None) is None
        assert gc_handles.get("") is None

    def test_persists_across_process_restart(self, clean_handles, monkeypatch):
        gc_handles.record("s-1", repo="/w/repo", status="cancelled")
        # Simulate a fresh process: wipe the in-memory dict, force a reload.
        monkeypatch.setattr(gc_handles, "_table", {})
        monkeypatch.setattr(gc_handles, "_loaded", False)
        assert gc_handles.get("s-1")["status"] == "cancelled"

    def test_corrupt_file_never_raises_and_recovers(self, clean_handles):
        path = gc_handles._state_file()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{not json at all", "utf-8")
        assert gc_handles.get("s-1") is None
        # record still works and repairs the file.
        gc_handles.record("s-1", status="running")
        assert gc_handles.get("s-1")["status"] == "running"
        assert json.loads(path.read_text("utf-8"))["s-1"]["status"] == "running"

    def test_unwritable_state_file_degrades_to_memory(self, clean_handles, monkeypatch):
        monkeypatch.setattr(gc_handles, "_state_file", lambda: None)
        gc_handles.record("s-1", status="running")
        assert gc_handles.get("s-1")["status"] == "running"  # in-memory

    def test_known_handles_most_recent_first(self, clean_handles):
        gc_handles.record("s-old", status="completed")
        gc_handles._table["s-old"]["updated_at"] = time.time() - 100
        gc_handles.record("s-new", status="running")
        assert gc_handles.known_handles() == ["s-new", "s-old"]
        assert gc_handles.known_handles(limit=1) == ["s-new"]

    def test_table_prunes_oldest_beyond_cap(self, clean_handles, monkeypatch):
        monkeypatch.setattr(gc_handles, "MAX_ENTRIES", 3)
        for i in range(5):
            gc_handles.record(f"s-{i}", status="completed")
            gc_handles._table[f"s-{i}"]["updated_at"] = float(i)
        gc_handles.record("s-final", status="running")
        assert len(gc_handles._table) <= 3
        assert "s-final" in gc_handles._table
        assert "s-0" not in gc_handles._table

    def test_no_auto_resume_surface_exists(self):
        """v0.3 contract: lookup is by exact handle only — the repo+timestamp
        auto-resume heuristic (get_recent) must not come back."""
        assert not hasattr(gc_handles, "get_recent")


# ---------------------------------------------------------------------------
# Git fallback — files_changed for edits the SDK stream carried no diff for
# ---------------------------------------------------------------------------

def _git(repo, *args):
    import subprocess

    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", *args],
        cwd=str(repo), check=True, capture_output=True,
    )


class TestGitFallback:
    @pytest.fixture
    def git_repo(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()
        _git(repo, "init", "-q")
        (repo / "tool.txt").write_text("orig\n")
        (repo / "pre.txt").write_text("pre\n")
        _git(repo, "add", "-A")
        _git(repo, "commit", "-qm", "init")
        return repo

    def test_shell_driven_edit_lands_via_git_fallback(
        self, clean_state, monkeypatch, git_repo
    ):
        # Pre-existing dirty file must NOT be attributed to the run.
        (git_repo / "pre.txt").write_text("pre dirty before run\n")

        def shell_edit_replay(*_a, **_k):
            yield ("cloud.session", {"agentId": "s-git", "cwd": str(git_repo), "model": "m"})
            # Simulates cursor editing through a shell command: no diff
            # content ever appears on the SDK stream.
            (git_repo / "tool.txt").write_text("orig\nedited by shell\n")
            (git_repo / "new.txt").write_text("brand new\n")
            yield ("cloud.result", {"status": "finished"})

        monkeypatch.setattr(gc_cloud, "run_cloud", shell_edit_replay)
        _start_run("edit via shell", repo=str(git_repo))
        job = _job_for("s-git")
        assert job.done_event.wait(10)

        result = job.result
        by_path = {Path(f["path"]).name: f for f in result["files_changed"]}
        assert set(by_path) == {"tool.txt", "new.txt"}  # pre.txt excluded
        assert by_path["tool.txt"]["status"] == "M"
        assert by_path["tool.txt"]["added"] == 1
        assert "edited by shell" in by_path["tool.txt"]["diff"]
        assert by_path["new.txt"]["status"] == "A"

    def test_sdk_stream_diff_wins_over_fallback(self, clean_state, monkeypatch, git_repo):
        """A file already captured from the SDK stream is not re-added."""

        def stream_and_shell(*_a, **_k):
            yield ("cloud.session", {"agentId": "s-win", "cwd": str(git_repo), "model": "m"})
            (git_repo / "tool.txt").write_text("orig\nedited\n")
            yield ("cloud.message", {
                "type": "tool_call", "call_id": "t1", "name": "edit_file",
                "status": "running", "args": {"path": str(git_repo / "tool.txt")},
            })
            yield ("cloud.message", {
                "type": "tool_call", "call_id": "t1", "name": "edit_file",
                "status": "completed",
                "result": {
                    "path": str(git_repo / "tool.txt"),
                    "oldText": "orig\n", "newText": "orig\nedited\n",
                },
            })
            yield ("cloud.result", {"status": "finished"})

        monkeypatch.setattr(gc_cloud, "run_cloud", stream_and_shell)
        _start_run("t", repo=str(git_repo))
        job = _job_for("s-win")
        assert job.done_event.wait(10)
        assert job.result["files_changed_count"] == 1
        assert job.result["files_changed"][0]["added"] == 1

    def test_unified_diff_text_counts(self):
        diff, added, removed = gc_events.unified_diff_text("a\nb\n", "a\nc\nd\n", "/w/f.txt")
        assert added == 2 and removed == 1
        assert "--- a/w/f.txt" in diff and "+++ b/w/f.txt" in diff


# ---------------------------------------------------------------------------
# All terminal states deliver a completion message — never a silent death
# ---------------------------------------------------------------------------

class TestAllTerminalStatesDeliver:
    def _run_armed(self, monkeypatch, tmp_path, sid, terminal_event,
                   pre_events=()):
        """Dispatch through create + first send so delivery gets armed; the replay
        emits ``pre_events`` + ``terminal_event`` only after the running
        handle was returned (deterministic — the run cannot finalize before
        arming)."""
        release = threading.Event()

        def replay(task, workdir, inactivity_timeout_s=0.0, max_wall_s=0.0,
                   cancel_check=None, agent_id=None, model=None, **_kw):
            yield ("cloud.session", {"agentId": sid, "cwd": str(workdir), "model": "m"})
            release.wait(10)
            yield from pre_events
            yield terminal_event

        monkeypatch.setattr(gc_cloud, "run_cloud", replay)
        res = _start_run("t", repo=str(tmp_path))
        assert "running in background" in res, f"run never armed: {res}"
        job = _job_for(sid)
        release.set()
        assert job.done_event.wait(10)
        events = _drain_completion_queue()
        assert len(events) == 1, f"expected exactly one delivery, got {events}"
        return job, events[0]

    def test_completed_delivers(self, clean_state, monkeypatch, tmp_path):
        job, evt = self._run_armed(
            monkeypatch, tmp_path, "s-ok",
            ("cloud.result", {"status": "finished"}),
        )
        assert job.status == "completed"
        assert evt["status"] == "completed"
        assert evt["result"]["success"] is True

    def test_cancelled_run_delivers(self, clean_state, monkeypatch, tmp_path):
        job, evt = self._run_armed(
            monkeypatch, tmp_path, "s-c",
            ("cloud.result", {"status": "cancelled"}),
        )
        # The JOB status names the real terminal state...
        assert job.status == "cancelled"
        assert evt["status"] == "cancelled"
        assert "cancel" in evt["error"]
        # ...while the result dict keeps the run's exact vocabulary (a
        # native cancel is result status "failed" — unchanged from v0.2).
        assert evt["result"]["status"] == "failed"

    def test_timeout_delivers(self, clean_state, monkeypatch, tmp_path):
        job, evt = self._run_armed(
            monkeypatch, tmp_path, "s-t",
            ("cloud.error", {"error": "cursor run timed out: no activity for 5s", "timeout": True}),
        )
        assert job.status == "timeout"
        assert evt["status"] == "timeout"
        assert "timed out" in evt["error"]
        assert evt["result"]["status"] == "timeout"

    def test_midrun_failure_delivers(self, clean_state, monkeypatch, tmp_path):
        job, evt = self._run_armed(
            monkeypatch, tmp_path, "s-x",
            ("cloud.error", {"error": "cursor-sdk stream failed mid-run: boom"}),
        )
        assert job.status == "failed"
        assert evt["status"] == "failed"
        assert "boom" in evt["error"]

    def test_terminal_error_detail_reaches_the_completion_summary(
        self, clean_state, monkeypatch, tmp_path
    ):
        """A terminal-error sdk.error with typed detail (retryable /
        retry_after) renders it on the failure line — not the bare
        'cursor run ended with status: error'. (The run completed a tool
        call first — durable progress — so the zero-progress auto-retry
        stays out of the way.)"""
        job, evt = self._run_armed(
            monkeypatch, tmp_path, "s-err-detail",
            ("cloud.error", {
                "error": "ServerError: upstream 502 from the agent backend",
                "retryable": True,
                "retry_after": "30",
                "run_status": "error",
            }),
            pre_events=[*_tool_round("t1"),
                        _narration_chunk("started digging in")],
        )
        assert job.status == "failed"
        assert evt["error"] == "ServerError: upstream 502 from the agent backend"
        assert evt["result"]["error_retryable"] is True
        assert evt["result"]["error_retry_after"] == "30"
        assert (
            "run failed: ServerError: upstream 502 from the agent backend "
            "(retryable, retry after 30s)" in evt["summary"]
        )

    def test_terminal_error_without_detail_stays_generic(
        self, clean_state, monkeypatch, tmp_path
    ):
        """No typed detail available: the generic terminal-error text is
        delivered with NO retry parenthetical (None = unknown, never
        rendered as 'not retryable')."""
        job, evt = self._run_armed(
            monkeypatch, tmp_path, "s-err-bare",
            ("cloud.error", {
                "error": "cursor run ended with status: error",
                "retryable": None,
                "retry_after": None,
                "run_status": "error",
            }),
            pre_events=[*_tool_round("t1"),
                        _narration_chunk("started digging in")],
        )
        assert job.status == "failed"
        assert "run failed: cursor run ended with status: error." in evt["summary"]
        assert "retryable" not in evt["summary"]
        assert "error_retryable" not in evt["result"]

    def test_transport_drop_delivers_failed_not_completed(
        self, clean_state, monkeypatch, tmp_path
    ):
        """Live repro (2026-07-03, ACP era): the stream died with a
        RetriableError narrated as the final message chunks, then a "clean"
        finish. The delivered completion must say failed with the error
        first-class in the header — not completed with the error buried in
        the summary. Kept under the SDK transport: the classification is
        content-based and transport-agnostic."""
        release = threading.Event()

        def replay(task, workdir, inactivity_timeout_s=0.0, max_wall_s=0.0,
                   cancel_check=None, agent_id=None, model=None, **_kw):
            yield ("cloud.session", {"agentId": "s-drop", "cwd": str(workdir),
                                   "model": "m"})
            release.wait(10)
            yield ("cloud.message", {
                "type": "assistant",
                "message": {"content": [{
                    "type": "text",
                    "text": "Now let me explore the relevant code\n",
                }]},
            })
            yield ("cloud.message", {
                "type": "assistant",
                "message": {"content": [{
                    "type": "text",
                    "text": "RetriableError: [canceled] http/2 stream "
                            "closed with error code CANCEL (0x8)",
                }]},
            })
            yield ("cloud.result", {"status": "finished"})

        monkeypatch.setattr(gc_cloud, "run_cloud", replay)
        res = _start_run("t", repo=str(tmp_path))
        assert "running in background" in res, f"run never armed: {res}"
        job = _job_for("s-drop")
        release.set()
        assert job.done_event.wait(10)

        events = _drain_completion_queue()
        assert len(events) == 1
        evt = events[0]
        assert job.status == "failed"
        assert evt["status"] == "failed"
        assert "http/2 stream closed with error code CANCEL (0x8)" in evt["error"]
        assert evt["result"]["success"] is False
        assert evt["result"]["status"] == "failed"
        assert "http/2 stream closed" in evt["result"]["error"]
        # The delivered text names the failure in the header, not just the
        # stitched summary body.
        assert "status: failed" in evt["summary"]
        assert "run failed:" in evt["summary"]
        assert "http/2 stream closed" in evt["summary"].split("cursor's summary:")[0]


# ---------------------------------------------------------------------------
# Progress subscriptions — periodic digests on the completion queue
# ---------------------------------------------------------------------------

def _digest_events(events):
    """The progress digests among drained completion-queue events."""
    return [e for e in events if e.get("cursor_progress_update")]


def _completion_events(events):
    """The terminal completions among drained completion-queue events."""
    return [e for e in events if not e.get("cursor_progress_update")]


def _collect_queue(collected, min_digests=1, timeout=5.0):
    """Keep draining the completion queue into ``collected`` until it holds
    at least ``min_digests`` digest events."""
    def _pump():
        collected.extend(_drain_completion_queue())
        return len(_digest_events(collected)) >= min_digests

    return _wait_until(_pump, timeout=timeout)


def _sub_tickers(name):
    """A session's live tickers, keyed by subscriber hermes session_key."""
    return {k[1]: t for k, t in gc_progress._tickers.items() if k[0] == name}


def _digest_id(name, n, sub_key=""):
    """The expected per-subscriber digest delegation_id."""
    return f"{name}#progress-{n}@{gc_progress.subscriber_suffix(sub_key)}"


class TestProgressSubscriptions:
    """cursor_send_message(update_interval_s) + cursor_subscribe: periodic
    digests ride the same completion_queue rail as terminal completions,
    numbered per session, and never outlive the run."""

    def _held_run(self, monkeypatch, tmp_path, sid="agent-digest", **send_kw):
        """A run held open on the returned release event, dispatched via
        create + send so subscription plumbing runs end-to-end."""
        release = threading.Event()
        monkeypatch.setattr(
            gc_cloud, "run_cloud", _gated_replay_factory(release, sid=sid)
        )
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        ack = cursor_send_message(name, "task", **send_kw)
        _assert_running_ack(ack)
        return name, _job_for(sid), release

    # -- subscription set at dispatch --------------------------------------

    def test_default_send_sets_180s_subscription(
        self, clean_state, monkeypatch, tmp_path
    ):
        name, job, release = self._held_run(monkeypatch, tmp_path)
        try:
            entry = gc_handles.get(name)
            # The send AUTO-SUBSCRIBED the calling hermes session ("" =
            # CLI in tests) in the per-subscriber map.
            assert gc_handles.subscribers_of(entry) == {"": 180.0}
            ticker = gc_progress._tickers.get((name, ""))
            assert ticker is not None
            assert ticker.interval_s == 180.0
        finally:
            release.set()
            assert job.done_event.wait(10)

    def test_explicit_interval_used_and_persisted(
        self, clean_state, monkeypatch, tmp_path
    ):
        name, job, release = self._held_run(
            monkeypatch, tmp_path, update_interval_s=45
        )
        try:
            assert gc_handles.subscribers_of(gc_handles.get(name)) == {"": 45.0}
            assert gc_progress._tickers[(name, "")].interval_s == 45.0
        finally:
            release.set()
            assert job.done_event.wait(10)

    def test_zero_interval_means_no_digests(
        self, clean_state, monkeypatch, tmp_path
    ):
        name, job, release = self._held_run(
            monkeypatch, tmp_path, update_interval_s=0
        )
        try:
            # Explicit 0 = the caller is NOT subscribed (no map entry).
            assert gc_handles.subscribers_of(gc_handles.get(name)) == {}
            assert _sub_tickers(name) == {}
            time.sleep(0.15)
            assert _digest_events(_drain_completion_queue()) == []
        finally:
            release.set()
            assert job.done_event.wait(10)
        events = _drain_completion_queue()
        assert _digest_events(events) == []
        assert len(_completion_events(events)) == 1

    # -- digest delivery + content -----------------------------------------

    def test_digest_rides_async_delegation_rail_with_status_and_events(
        self, clean_state, monkeypatch, tmp_path
    ):
        name, job, release = self._held_run(
            monkeypatch, tmp_path, update_interval_s=0.05
        )
        collected = []
        try:
            assert _collect_queue(collected, min_digests=1)
        finally:
            release.set()
            assert job.done_event.wait(10)

        digest = _digest_events(collected)[0]
        # The exact event shape every hermes-core consumer differentiates
        # on: type field, unique delegation_id (TUI dedup — per digest AND
        # per subscriber), session_key routing to the SUBSCRIBER — and NOT
        # deregistering anything is the producer's job.
        assert digest["type"] == "async_delegation"
        assert digest["delegation_id"] == _digest_id(name, 1)
        assert digest["session_key"] == ""
        assert digest["status"] == "running"
        assert digest["cursor_progress_update"] == 1
        assert "NOT the final result" in digest["goal"]

        text = digest["summary"]
        assert f"cursor session '{name}' — progress update 1" in text
        assert "status: running" in text
        assert "elapsed:" in text and "last activity:" in text
        # The early edit of the gated replay is visible either as the
        # files-so-far header count or in the events-since-last-tick body.
        assert "f1.py" in text

    def test_quiet_tick_says_no_new_events(
        self, clean_state, monkeypatch, tmp_path
    ):
        name, job, release = self._held_run(
            monkeypatch, tmp_path, update_interval_s=0.05
        )
        collected = []
        try:
            # By the second digest the held run has gone quiet — no events
            # stream while the replay blocks on the release gate.
            assert _collect_queue(collected, min_digests=2)
        finally:
            release.set()
            assert job.done_event.wait(10)

        quiet = _digest_events(collected)[-1]
        assert "no new events since last update" in quiet["summary"]
        # Header still carries the signal for a quiet run.
        assert "last activity:" in quiet["summary"]

    def test_digest_numbering_increments_and_tags_session(
        self, clean_state, monkeypatch, tmp_path
    ):
        name, job, release = self._held_run(
            monkeypatch, tmp_path, update_interval_s=0.05
        )
        collected = []
        try:
            assert _collect_queue(collected, min_digests=3)
        finally:
            release.set()
            assert job.done_event.wait(10)

        digests = _digest_events(collected)[:3]
        assert [d["cursor_progress_update"] for d in digests] == [1, 2, 3]
        for i, d in enumerate(digests, start=1):
            assert f"progress update {i}" in d["summary"]
            assert f"'{name}'" in d["summary"]

    # -- cursor_subscribe ---------------------------------------------------

    def test_subscribe_changes_interval_mid_run(
        self, clean_state, monkeypatch, tmp_path
    ):
        # Start slow (no tick due for 60s), then shorten mid-run: the
        # pending timer must be rescheduled immediately.
        name, job, release = self._held_run(
            monkeypatch, tmp_path, update_interval_s=60
        )
        collected = []
        try:
            ack = cursor_subscribe(name, 0.05)
            assert ack == gc_render.subscribe_ack(name, 0.05)
            assert "\n" not in ack  # 1-line plain-text ack
            assert name in ack
            assert gc_handles.subscribers_of(gc_handles.get(name)) == {"": 0.05}
            assert _collect_queue(collected, min_digests=1)
        finally:
            release.set()
            assert job.done_event.wait(10)

    def test_digest_next_update_line_reflects_midrun_interval_change(
        self, clean_state, monkeypatch, tmp_path
    ):
        """The ticker hands its CURRENT interval to the digest at tick
        time: a run dispatched at 60s and retuned mid-run via
        cursor_subscribe renders the NEW interval in the next digest,
        not the dispatch-time one."""
        name, job, release = self._held_run(
            monkeypatch, tmp_path, update_interval_s=60
        )
        collected = []
        try:
            cursor_subscribe(name, 0.05)
            assert _collect_queue(collected, min_digests=1)
        finally:
            release.set()
            assert job.done_event.wait(10)

        digest = _digest_events(collected)[0]
        # 0.05s rounds up to the 1s floor; the old 60s would say "1m".
        assert "next update in 1s" in digest["summary"]
        assert "next update in 1m" not in digest["summary"]

    def test_subscribe_zero_unsubscribes_mid_run(
        self, clean_state, monkeypatch, tmp_path
    ):
        name, job, release = self._held_run(
            monkeypatch, tmp_path, update_interval_s=0.05
        )
        collected = []
        try:
            assert _collect_queue(collected, min_digests=1)
            ack = cursor_subscribe(name, 0)
            assert "off" in ack and name in ack
            # 0 REMOVED the caller's subscription from the map.
            assert gc_handles.subscribers_of(gc_handles.get(name)) == {}
            assert _sub_tickers(name) == {}
            _drain_completion_queue()
            time.sleep(0.2)
            assert _digest_events(_drain_completion_queue()) == []
        finally:
            release.set()
            assert job.done_event.wait(10)
        # The terminal completion is unaffected by unsubscribing.
        events = _drain_completion_queue()
        assert len(_completion_events(events)) == 1

    def test_subscribe_without_run_persists_for_next_run(
        self, clean_state, monkeypatch, tmp_path
    ):
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        ack = cursor_subscribe(name, 0.05)
        assert name in ack
        assert gc_handles.subscribers_of(gc_handles.get(name)) == {"": 0.05}

        release = threading.Event()
        monkeypatch.setattr(
            gc_cloud, "run_cloud", _gated_replay_factory(release, sid="agent-persist")
        )
        collected = []
        _assert_running_ack(cursor_send_message(name, "task"))
        job = _job_for("agent-persist")
        try:
            # No update_interval_s on send — the persisted 0.05 drives it.
            assert gc_progress._tickers[(name, "")].interval_s == 0.05
            assert _collect_queue(collected, min_digests=1)
        finally:
            release.set()
            assert job.done_event.wait(10)

    def test_subscribe_unknown_session_is_actionable(self, clean_state):
        out = cursor_subscribe("no-such-session", 30)
        assert "no session named 'no-such-session'" in out

    def test_subscribe_rejects_negative_interval(
        self, clean_state, monkeypatch, tmp_path
    ):
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        assert ">= 0" in cursor_subscribe(name, -5)

    # -- terminal-state guarantees -------------------------------------------

    def test_digest_never_fires_after_terminal_state(
        self, clean_state, monkeypatch, tmp_path
    ):
        name, job, release = self._held_run(
            monkeypatch, tmp_path, update_interval_s=0.05
        )
        collected = []
        assert _collect_queue(collected, min_digests=1)
        release.set()
        assert job.done_event.wait(10)
        # Everything already enqueued at settle time is legal; nothing may
        # be added after it — the pending timer was cancelled at finalize.
        collected.extend(_drain_completion_queue())
        time.sleep(0.25)
        late = _drain_completion_queue()
        assert late == [], f"digest arrived after terminal state: {late}"
        # And on the collected sequence the completion is the LAST event.
        assert not _digest_events([collected[-1]])
        assert job.status == "completed"

    def test_completion_delivered_exactly_once_with_digests_active(
        self, clean_state, monkeypatch, tmp_path
    ):
        name, job, release = self._held_run(
            monkeypatch, tmp_path, update_interval_s=0.05
        )
        collected = []
        assert _collect_queue(collected, min_digests=2)
        release.set()
        assert job.done_event.wait(10)
        collected.extend(_drain_completion_queue())

        completions = _completion_events(collected)
        assert len(completions) == 1
        assert completions[0]["status"] == "completed"
        assert completions[0]["delegation_id"] == name
        assert len(_digest_events(collected)) >= 2

    def test_interrupt_and_reprompt_carries_subscription_and_numbering(
        self, clean_state, monkeypatch, tmp_path
    ):
        release1, release2 = threading.Event(), threading.Event()
        seq = _SdkSequence(
            _gated_replay_factory(release1, sid="agent-ir"),
            _gated_replay_factory(release2, sid="agent-ir"),
        )
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        _assert_running_ack(cursor_send_message(name, "task", update_interval_s=0.05))
        collected = []
        assert _collect_queue(collected, min_digests=1)
        first_n = max(d["cursor_progress_update"] for d in _digest_events(collected))

        # Interrupt + re-prompt: the new run inherits the subscription (no
        # explicit interval on this send) and numbering continues.
        ack = cursor_send_message(name, "follow-up")
        _assert_running_ack(ack)
        assert "interrupted" in ack
        job2 = _job_for("agent-ir")
        try:
            assert gc_progress._tickers[(name, "")].interval_s == 0.05
            collected2 = []
            assert _collect_queue(collected2, min_digests=1)
            nums = [d["cursor_progress_update"] for d in _digest_events(collected2)]
            assert min(nums) == first_n + 1, (
                f"numbering restarted: {nums} after {first_n}"
            )
        finally:
            release1.set()
            release2.set()
            assert job2.done_event.wait(10)

    # -- multiple sessions ---------------------------------------------------

    def test_multiple_sessions_with_different_intervals(
        self, clean_state, monkeypatch, tmp_path
    ):
        repo_a, repo_b = tmp_path / "a", tmp_path / "b"
        repo_a.mkdir()
        repo_b.mkdir()
        release_a, release_b = threading.Event(), threading.Event()
        seq = _SdkSequence(
            _gated_replay_factory(release_a, sid="agent-a"),
            _gated_replay_factory(release_b, sid="agent-b"),
        )
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)

        name_a = _created_name(cursor_create_session(repo=str(repo_a)))
        name_b = _created_name(cursor_create_session(repo=str(repo_b)))
        _assert_running_ack(
            cursor_send_message(name_a, "task a", update_interval_s=0.05)
        )
        _assert_running_ack(
            cursor_send_message(name_b, "task b", update_interval_s=60)
        )
        job_a, job_b = _job_for("agent-a"), _job_for("agent-b")
        collected = []
        try:
            assert gc_progress._tickers[(name_a, "")].interval_s == 0.05
            assert gc_progress._tickers[(name_b, "")].interval_s == 60.0
            assert _collect_queue(collected, min_digests=2)
        finally:
            release_a.set()
            release_b.set()
            assert job_a.done_event.wait(10)
            assert job_b.done_event.wait(10)

        digests = _digest_events(collected)
        # Only the fast session ticked; every digest is tagged with ITS name.
        assert all(f"'{name_a}'" in d["summary"] for d in digests)
        assert all(d["delegation_id"].startswith(f"{name_a}#progress-") for d in digests)
        assert not any(f"'{name_b}'" in d["summary"] for d in digests)

    # -- digest rendering (pure) ----------------------------------------------

    def test_digest_text_header_body_and_caps(self):
        events = [
            {"seq": 40 + i, "kind": "tool_use", "tool": "shell",
             "command": f"pytest test_{i}.py"}
            for i in range(8)
        ]
        text = gc_render.digest_text(
            name="busy-bee",
            n=4,
            status="running",
            elapsed_s=843,
            last_activity_s=12,
            files=[{"path": "calc.py", "added": 4, "removed": 0}],
            pending_tool="shell `pytest -q`",
            pending_tool_s=41,
            events=events,
            new_count=8,
            next_update_s=180,
        )
        assert "cursor session 'busy-bee' — progress update 4" in text
        assert (
            "status: running · elapsed: 843s · last activity: 12s ago · "
            "next update in 3m" in text
        )
        assert "files so far (1): calc.py +4 −0" in text
        assert "waiting on:" in text
        assert "shell `pytest -q` (41s)" in text
        assert "new events since last update (8):" in text
        # Body capped at DIGEST_MAX_EVENTS lines + an omission pointer.
        assert text.count("pytest test_") == gc_render.DIGEST_MAX_EVENTS
        assert "3 more — cursor_events('busy-bee')" in text
        assert len(text) < 2048

    def test_digest_text_quiet_tick(self):
        text = gc_render.digest_text(
            name="quiet-owl",
            n=2,
            status="running",
            elapsed_s=360,
            last_activity_s=181,
            files=[],
            events=[],
            new_count=0,
        )
        assert "progress update 2" in text
        assert "no new events since last update" in text
        # No interval provided → the fragment is omitted entirely.
        assert "next update in" not in text

    def test_digest_text_next_update_reflects_changed_interval(self):
        """The 'next update in' fragment renders whatever interval the
        ticker holds AT TICK TIME — a mid-run retune shows immediately."""
        kwargs = dict(
            name="retuned-fox", n=3, status="running", elapsed_s=100,
            last_activity_s=5, files=[], events=[], new_count=0,
        )
        before = gc_render.digest_text(**kwargs, next_update_s=180)
        after = gc_render.digest_text(**kwargs, next_update_s=45)
        assert "next update in 3m" in before
        assert "next update in 45s" in after
        assert "next update in 3m" not in after

    def test_pending_tool_call_visible_in_digest(
        self, clean_state, monkeypatch, tmp_path
    ):
        """A run stuck inside one long tool call shows it in the header —
        the 'long quiet tool call vs stall' signal from the spec."""
        release = threading.Event()

        def replay(task, workdir, inactivity_timeout_s=0.0, max_wall_s=0.0,
                   cancel_check=None, agent_id=None, model=None, **_kw):
            yield ("cloud.session", {"agentId": "agent-pending",
                                   "cwd": str(workdir), "model": "m"})
            yield ("cloud.message", {
                "type": "tool_call", "call_id": "t9", "name": "shell",
                "status": "running",
                "args": {"command": "sleep 999"},
            })
            while not release.is_set():
                if cancel_check and cancel_check():
                    yield ("cloud.result", {"status": "cancelled"})
                    return
                time.sleep(0.01)
            yield ("cloud.result", {"status": "finished"})

        monkeypatch.setattr(gc_cloud, "run_cloud", replay)
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        _assert_running_ack(
            cursor_send_message(name, "task", update_interval_s=0.05)
        )
        job = _job_for("agent-pending")
        collected = []
        try:
            assert _collect_queue(collected, min_digests=1)
        finally:
            release.set()
            assert job.done_event.wait(10)
        assert "pending tool call:" in _digest_events(collected)[0]["summary"] or (
            "waiting on:" in _digest_events(collected)[0]["summary"]
            and "sleep 999" in _digest_events(collected)[0]["summary"]
        )

    def test_digest_lists_concurrent_pending_calls(self):
        """Cursor runs tool calls CONCURRENTLY (two parallel sub-agents
        captured live 2026-07-09) — the digest must list every in-flight
        call, and a quiet window must read busy, not stalled."""
        text = gc_render.digest_text(
            name="parallel-swan",
            n=3,
            status="running",
            elapsed_s=250,
            last_activity_s=98,
            files=[],
            pending_tools=[
                {"tool": "subagent",
                 "title": "subagent — Explore blockchain test infrastructure",
                 "pending_s": 98},
                {"tool": "subagent",
                 "title": "subagent — Survey FlowOfFunds rule families",
                 "pending_s": 95},
            ],
            plan=[
                {"content": "Bootstrap env", "status": "completed"},
                {"content": "Wire nonce guard", "status": "in_progress"},
                {"content": "Run tests", "status": "pending"},
            ],
            events=[],
            new_count=0,
        )
        assert "waiting on:" in text
        assert "Explore blockchain test infrastructure (98s)" in text
        assert "Survey FlowOfFunds rule families (95s)" in text
        assert "plan: 1/3 done — current: Wire nonce guard" in text
        # A quiet window with pending calls is busy, not idle.
        assert "cursor is busy inside the calls above" in text
        assert "no new events since last update" not in text


# ---------------------------------------------------------------------------
# Interval validation at the tool boundary (issue #14)
# ---------------------------------------------------------------------------

def _reject_dispatch(*_args, **_kwargs):
    """run_sdk sentinel for rejection tests: an invalid interval must be
    refused BEFORE any run is dispatched, so reaching the SDK is a bug."""
    raise AssertionError("run dispatched despite invalid interval")


class TestIntervalValidation:
    """Issue #14: one shared validation contract for cursor_subscribe's
    interval_s and cursor_send_message's update_interval_s — negatives
    and non-numbers rejected with a clear message, 0 stays the documented
    unsubscribe, positives clamped into [MIN_UPDATE_INTERVAL_S,
    MAX_UPDATE_INTERVAL_S] with the clamp spelled out in the ack.

    clean_state drops MIN_UPDATE_INTERVAL_S to 0 for the timing tests
    above, so clamp tests patch their own minimum back in."""

    def test_production_bounds(self):
        # The real (unpatched) contract values: 15s floor, 24h ceiling.
        assert gc_progress.MIN_UPDATE_INTERVAL_S == 15.0
        assert gc_progress.MAX_UPDATE_INTERVAL_S == 24 * 3600.0

    # -- rejection: negative + non-numeric ----------------------------------

    def test_subscribe_negative_rejected_not_unsubscribed(
        self, clean_state, tmp_path
    ):
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        out = cursor_subscribe(name, -5)
        assert "interval_s must be >= 0" in out
        # NOT silently treated as unsubscribe: nothing was persisted.
        assert "update_interval_s" not in (gc_handles.get(name) or {})

    def test_send_negative_rejected_before_dispatch(
        self, clean_state, monkeypatch, tmp_path
    ):
        monkeypatch.setattr(gc_cloud, "run_cloud", _reject_dispatch)
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        out = cursor_send_message(name, "task", update_interval_s=-5)
        assert "update_interval_s must be >= 0" in out
        assert gc_progress._tickers == {}
        assert "update_interval_s" not in (gc_handles.get(name) or {})

    def test_subscribe_non_numeric_rejected(self, clean_state, tmp_path):
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        assert "interval_s must be a number" in cursor_subscribe(name, "soon")
        assert "interval_s must be a number" in cursor_subscribe(
            name, float("nan")
        )
        assert "update_interval_s" not in (gc_handles.get(name) or {})

    def test_send_non_numeric_rejected_before_dispatch(
        self, clean_state, monkeypatch, tmp_path
    ):
        monkeypatch.setattr(gc_cloud, "run_cloud", _reject_dispatch)
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        out = cursor_send_message(name, "task", update_interval_s="fast")
        assert "update_interval_s must be a number" in out
        assert gc_progress._tickers == {}

    # -- 0 stays the documented unsubscribe ----------------------------------

    def test_subscribe_zero_still_unsubscribes(self, clean_state, tmp_path):
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        ack = cursor_subscribe(name, 0)
        assert "progress updates off" in ack
        assert "clamped" not in ack
        assert gc_handles.subscribers_of(gc_handles.get(name)) == {}

    # -- clamping: below minimum / above maximum -----------------------------

    def test_subscribe_subminimum_clamps_up_with_ack_note(
        self, clean_state, monkeypatch, tmp_path
    ):
        monkeypatch.setattr(gc_progress, "MIN_UPDATE_INTERVAL_S", 20.0)
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        ack = cursor_subscribe(name, 5)
        assert "progress updates every 20s" in ack
        assert "interval_s clamped to the 20s minimum" in ack
        assert "\n" not in ack  # still the 1-line ack
        assert gc_handles.subscribers_of(gc_handles.get(name)) == {"": 20.0}

    def test_subscribe_huge_clamps_down_to_max_with_ack_note(
        self, clean_state, tmp_path
    ):
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        ack = cursor_subscribe(name, 999_999_999)
        assert "interval_s clamped to the 24h maximum" in ack
        assert gc_handles.subscribers_of(gc_handles.get(name)) == {
            "": 24 * 3600.0
        }

    def test_send_subminimum_clamps_up_and_ack_notes_it(
        self, clean_state, monkeypatch, tmp_path
    ):
        monkeypatch.setattr(gc_progress, "MIN_UPDATE_INTERVAL_S", 20.0)
        release = threading.Event()
        monkeypatch.setattr(
            gc_cloud, "run_cloud", _gated_replay_factory(release, sid="agent-clamp")
        )
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        ack = cursor_send_message(name, "task", update_interval_s=2)
        _assert_running_ack(ack)
        job = _job_for("agent-clamp")
        try:
            assert "update_interval_s clamped to the 20s minimum" in ack
            assert gc_handles.subscribers_of(gc_handles.get(name)) == {"": 20.0}
            assert gc_progress._tickers[(name, "")].interval_s == 20.0
        finally:
            release.set()
            assert job.done_event.wait(10)

    def test_send_huge_clamps_down_and_ack_notes_it(
        self, clean_state, monkeypatch, tmp_path
    ):
        release = threading.Event()
        monkeypatch.setattr(
            gc_cloud, "run_cloud", _gated_replay_factory(release, sid="agent-max")
        )
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        ack = cursor_send_message(name, "task", update_interval_s=999_999_999)
        _assert_running_ack(ack)
        job = _job_for("agent-max")
        try:
            assert "update_interval_s clamped to the 24h maximum" in ack
            assert gc_handles.subscribers_of(gc_handles.get(name)) == {
                "": 24 * 3600.0
            }
            assert gc_progress._tickers[(name, "")].interval_s == 24 * 3600.0
        finally:
            release.set()
            assert job.done_event.wait(10)

    # -- in-range values pass through unchanged -------------------------------

    def test_subscribe_valid_value_unchanged(
        self, clean_state, monkeypatch, tmp_path
    ):
        monkeypatch.setattr(gc_progress, "MIN_UPDATE_INTERVAL_S", 15.0)
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        ack = cursor_subscribe(name, 60)
        assert ack == gc_render.subscribe_ack(name, 60.0)
        assert "clamped" not in ack
        assert gc_handles.subscribers_of(gc_handles.get(name)) == {"": 60.0}

    def test_send_valid_value_unchanged(
        self, clean_state, monkeypatch, tmp_path
    ):
        monkeypatch.setattr(gc_progress, "MIN_UPDATE_INTERVAL_S", 15.0)
        release = threading.Event()
        monkeypatch.setattr(
            gc_cloud, "run_cloud", _gated_replay_factory(release, sid="agent-ok")
        )
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        ack = cursor_send_message(name, "task", update_interval_s=45)
        _assert_running_ack(ack)
        job = _job_for("agent-ok")
        try:
            assert "clamped" not in ack
            assert gc_handles.subscribers_of(gc_handles.get(name)) == {"": 45.0}
            assert gc_progress._tickers[(name, "")].interval_s == 45.0
        finally:
            release.set()
            assert job.done_event.wait(10)


# ---------------------------------------------------------------------------
# Digest flood guards — interval floor + single ticker chain (issue #10)
# ---------------------------------------------------------------------------

class TestDigestFloodGuards:
    """Issue #10 (live incident 2026-07-04): ~2,600 digests delivered in a
    6s run at update_interval_s=1, aggravated by interrupt-and-reprompt
    stacking digest loops. Two guarantees close it: interval_s is a HARD
    FLOOR between enqueued digests no matter what fires the tick, and a
    session can never have more than one live ticker chain — a stale
    chain tears itself down on its next fire."""

    def _held_run(self, monkeypatch, tmp_path, sid="agent-flood", **send_kw):
        release = threading.Event()
        monkeypatch.setattr(
            gc_cloud, "run_cloud", _gated_replay_factory(release, sid=sid)
        )
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        _assert_running_ack(cursor_send_message(name, "task", **send_kw))
        return name, _job_for(sid), release

    def test_interval_is_hard_floor_regardless_of_tick_source(
        self, clean_state, monkeypatch, tmp_path
    ):
        """Fire the delivery path 50 times back-to-back (what the stacked
        chains of the incident did): exactly ONE digest may pass per
        interval window, and the dropped ticks consume neither the digest
        numbering nor the events-since-last-tick cursor."""
        # 30s interval: the real timer never fires in-test, so every tick
        # here is driven manually.
        name, job, release = self._held_run(
            monkeypatch, tmp_path, update_interval_s=30
        )
        try:
            ticker = gc_progress._tickers[(name, "")]
            ticker._deliver()  # digest 1 — no floor yet
            seq_after_first = ticker.last_seq
            for _ in range(50):  # the flood
                ticker._deliver()
            digests = _digest_events(_drain_completion_queue())
            assert len(digests) == 1, (
                f"interval floor not enforced: {len(digests)} digests"
            )
            assert digests[0]["cursor_progress_update"] == 1
            assert ticker.last_seq == seq_after_first

            # Once the window elapses (rewind the floor clock instead of
            # sleeping 30s) delivery resumes with NO numbering gap — the
            # dropped ticks never consumed counters.
            with gc_progress._lock:
                gc_progress._last_emit[(name, "")] -= 30
            ticker._deliver()
            resumed = _digest_events(_drain_completion_queue())
            assert [d["cursor_progress_update"] for d in resumed] == [2]
        finally:
            release.set()
            assert job.done_event.wait(10)

    def test_floor_persists_across_interrupt_and_reprompt(
        self, clean_state, monkeypatch, tmp_path
    ):
        """The incident's aggravator: each re-prompt reset the digest loop.
        The floor is keyed by session (like the numbering), so the NEW
        run's first tick inside the previous digest's window is dropped."""
        release1, release2 = threading.Event(), threading.Event()
        seq = _SdkSequence(
            _gated_replay_factory(release1, sid="agent-fl-ir"),
            _gated_replay_factory(release2, sid="agent-fl-ir"),
        )
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        _assert_running_ack(
            cursor_send_message(name, "task", update_interval_s=30)
        )
        gc_progress._tickers[(name, "")]._deliver()  # digest 1 on run 1

        ack = cursor_send_message(name, "follow-up")
        _assert_running_ack(ack)
        assert "interrupted" in ack
        job2 = _job_for("agent-fl-ir")
        try:
            _drain_completion_queue()  # digest 1 (run 1's cancel is in-turn)
            ticker2 = gc_progress._tickers[(name, "")]
            assert ticker2.job is job2
            ticker2._deliver()  # inside digest 1's 30s window → dropped
            assert _digest_events(_drain_completion_queue()) == [], (
                "re-prompt reset the interval floor"
            )
        finally:
            release1.set()
            release2.set()
            assert job2.done_event.wait(10)

    def test_reprompt_swaps_ticker_and_stale_chain_tears_down(
        self, clean_state, monkeypatch, tmp_path
    ):
        """Re-prompting must not stack digest loops: the old run's ticker
        is cancelled at settle, only ONE ticker stays registered, and a
        stray un-registered chain (the pre-fix stacking) cancels itself on
        its next fire without delivering anything."""
        release1, release2 = threading.Event(), threading.Event()
        seq = _SdkSequence(
            _gated_replay_factory(release1, sid="agent-fl-stack"),
            _gated_replay_factory(release2, sid="agent-fl-stack"),
        )
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        _assert_running_ack(
            cursor_send_message(name, "task", update_interval_s=30)
        )
        old = gc_progress._tickers[(name, "")]

        ack = cursor_send_message(name, "follow-up")
        _assert_running_ack(ack)
        assert "interrupted" in ack
        job2 = _job_for("agent-fl-stack")
        try:
            new = gc_progress._tickers[(name, "")]
            assert new is not old
            assert new.job is job2
            # The interrupted run's chain is dead, not merely replaced.
            assert old._cancelled
            assert old._timer is None

            # A stale duplicate chain (what stacked pre-fix) fires once,
            # notices it is not the registered ticker, and tears itself
            # down without delivering.
            rogue = gc_progress._Ticker(job2, "", 0.01)
            rogue.start()
            assert _wait_until(lambda: rogue._cancelled, timeout=5), (
                "stale digest chain kept running"
            )
            assert rogue._timer is None
            assert gc_progress._tickers[(name, "")] is new
            # Nothing reached the queue: the rogue self-terminated and the
            # legit 30s ticker never came due.
            assert _digest_events(_drain_completion_queue()) == []
        finally:
            release1.set()
            release2.set()
            assert job2.done_event.wait(10)


# ---------------------------------------------------------------------------
# Multi-subscriber delivery — hermes_session <- cursor_session subscriptions
# ---------------------------------------------------------------------------

class TestMultiSubscriberDelivery:
    """Subscriptions are per (hermes session, cursor session): the handle
    entry's ``subscribers`` map. cursor_send_message auto-subscribes the
    DISPATCHING hermes session; cursor_subscribe subscribes the CALLING
    one — the reported bug was a cross-session cursor_subscribe silently
    retuning the dispatcher's feed instead of delivering to the
    subscriber. Every subscriber gets its own copy of every digest AND
    the completion, routed by its own session_key, with distinct
    delegation_ids (TUI dedup); the dispatcher's completion is guaranteed
    even when unsubscribed."""

    def _held_run_as(self, monkeypatch, tmp_path, key, sid, **send_kw):
        """A held-open run dispatched by hermes session ``key``."""
        release = threading.Event()
        monkeypatch.setattr(
            gc_cloud, "run_cloud", _gated_replay_factory(release, sid=sid)
        )
        monkeypatch.setattr(gc, "_resolve_session_key", lambda: key)
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        _assert_running_ack(cursor_send_message(name, "task", **send_kw))
        return name, _job_for(sid), release

    # -- auto-subscribe on send ---------------------------------------------

    def test_send_auto_subscribes_the_calling_hermes_session(
        self, clean_state, monkeypatch, tmp_path
    ):
        name, job, release = self._held_run_as(
            monkeypatch, tmp_path, "gw:alice", sid="agent-auto"
        )
        try:
            assert gc_handles.subscribers_of(gc_handles.get(name)) == {
                "gw:alice": 180.0
            }
            ticker = gc_progress._tickers[(name, "gw:alice")]
            assert ticker.interval_s == 180.0
            assert ticker.sub_key == "gw:alice"
        finally:
            release.set()
            assert job.done_event.wait(10)

    # -- cross-session subscribe (the reported bug) ---------------------------

    def test_cross_session_subscribe_delivers_to_the_subscriber(
        self, clean_state, monkeypatch, tmp_path
    ):
        """A cursor_subscribe from a hermes session OTHER than the
        dispatching one must deliver digests to the SUBSCRIBING session
        (its session_key on the events) — and must NOT retune the
        dispatcher's feed (the pre-fix failure mode)."""
        name, job, release = self._held_run_as(
            monkeypatch, tmp_path, "gw:alice", sid="agent-cross",
            update_interval_s=3600,
        )
        collected = []
        try:
            monkeypatch.setattr(gc, "_resolve_session_key", lambda: "gw:bob")
            ack = cursor_subscribe(name, 0.05)
            assert name in ack
            # Two independent subscriptions, each with its own ticker at
            # its own interval — bob did not retune alice.
            assert gc_handles.subscribers_of(gc_handles.get(name)) == {
                "gw:alice": 3600.0, "gw:bob": 0.05,
            }
            assert gc_progress._tickers[(name, "gw:alice")].interval_s == 3600.0
            assert gc_progress._tickers[(name, "gw:bob")].interval_s == 0.05
            assert _collect_queue(collected, min_digests=2)
        finally:
            release.set()
            assert job.done_event.wait(10)

        digests = _digest_events(collected)
        # Everything delivered went to bob (alice's 1h tick never came
        # due), routed by BOB's session_key with bob-suffixed ids and
        # bob's own numbering from 1.
        assert digests
        assert all(d["session_key"] == "gw:bob" for d in digests)
        assert digests[0]["delegation_id"] == _digest_id(name, 1, "gw:bob")
        assert [d["cursor_progress_update"] for d in digests[:2]] == [1, 2]

    def test_two_subscribers_each_get_their_own_copy(
        self, clean_state, monkeypatch, tmp_path
    ):
        """Duplicate events ACROSS hermes sessions are by design: both
        subscribers receive digests, each copy carries its subscriber's
        session_key, numbering is per subscriber, and every delegation_id
        is unique (two n=1 digests must not dedup against each other)."""
        name, job, release = self._held_run_as(
            monkeypatch, tmp_path, "gw:alice", sid="agent-dup",
            update_interval_s=0.05,
        )
        collected = []
        try:
            monkeypatch.setattr(gc, "_resolve_session_key", lambda: "gw:bob")
            cursor_subscribe(name, 0.05)

            def _both_delivered():
                collected.extend(_drain_completion_queue())
                keys = {d["session_key"] for d in _digest_events(collected)}
                return {"gw:alice", "gw:bob"} <= keys

            assert _wait_until(_both_delivered, timeout=5)
        finally:
            release.set()
            assert job.done_event.wait(10)

        digests = _digest_events(collected)
        ids = [d["delegation_id"] for d in digests]
        assert len(ids) == len(set(ids)), f"delegation_ids collide: {ids}"
        first_n = {}
        for d in digests:
            first_n.setdefault(d["session_key"], d["cursor_progress_update"])
        assert first_n == {"gw:alice": 1, "gw:bob": 1}

    # -- unsubscribe scoping ---------------------------------------------------

    def test_unsubscribe_removes_only_the_calling_session(
        self, clean_state, monkeypatch, tmp_path
    ):
        name, job, release = self._held_run_as(
            monkeypatch, tmp_path, "gw:alice", sid="agent-unsub",
            update_interval_s=0.05,
        )
        try:
            monkeypatch.setattr(gc, "_resolve_session_key", lambda: "gw:bob")
            cursor_subscribe(name, 3600)
            assert (name, "gw:bob") in gc_progress._tickers
            ack = cursor_subscribe(name, 0)
            assert "off" in ack
            # Only bob's subscription went; alice's feed is untouched and
            # still delivering.
            assert gc_handles.subscribers_of(gc_handles.get(name)) == {
                "gw:alice": 0.05
            }
            assert set(_sub_tickers(name)) == {"gw:alice"}
            _drain_completion_queue()
            collected = []
            assert _collect_queue(collected, min_digests=1)
            assert all(
                d["session_key"] == "gw:alice"
                for d in _digest_events(collected)
            )
        finally:
            release.set()
            assert job.done_event.wait(10)

    def test_same_session_resubscribe_retunes_instead_of_duplicating(
        self, clean_state, monkeypatch, tmp_path
    ):
        name, job, release = self._held_run_as(
            monkeypatch, tmp_path, "gw:alice", sid="agent-retune",
            update_interval_s=3600,
        )
        try:
            # Same hermes session subscribing again = retune, not a
            # second feed: still one map entry, still ONE ticker.
            cursor_subscribe(name, 1800)
            assert gc_handles.subscribers_of(gc_handles.get(name)) == {
                "gw:alice": 1800.0
            }
            tickers = _sub_tickers(name)
            assert set(tickers) == {"gw:alice"}
            assert tickers["gw:alice"].interval_s == 1800.0
        finally:
            release.set()
            assert job.done_event.wait(10)

    # -- completion fan-out ------------------------------------------------------

    def test_completion_fans_out_to_every_subscriber(
        self, clean_state, monkeypatch, tmp_path
    ):
        name, job, release = self._held_run_as(
            monkeypatch, tmp_path, "gw:alice", sid="agent-fan",
            update_interval_s=3600,
        )
        monkeypatch.setattr(gc, "_resolve_session_key", lambda: "gw:bob")
        cursor_subscribe(name, 3600)
        release.set()
        assert job.done_event.wait(10)

        completions = _completion_events(_drain_completion_queue())
        assert len(completions) == 2
        by_key = {c["session_key"]: c for c in completions}
        assert set(by_key) == {"gw:alice", "gw:bob"}
        # Distinct delegation_ids (TUI dedup): the dispatcher keeps the
        # plain session name, other subscribers get the hash suffix.
        assert by_key["gw:alice"]["delegation_id"] == name
        assert by_key["gw:bob"]["delegation_id"] == (
            f"{name}@{gc_progress.subscriber_suffix('gw:bob')}"
        )
        # Identical payloads apart from routing.
        assert by_key["gw:alice"]["status"] == "completed"
        assert by_key["gw:bob"]["status"] == "completed"
        assert by_key["gw:alice"]["result"] == by_key["gw:bob"]["result"]
        assert by_key["gw:alice"]["summary"] == by_key["gw:bob"]["summary"]

    def test_dispatcher_completion_guaranteed_even_when_unsubscribed(
        self, clean_state, monkeypatch, tmp_path
    ):
        """update_interval_s=0 leaves the dispatcher UNSUBSCRIBED (no
        digests) — but its completion must never be lost, so the fan-out
        still includes it alongside the actual subscribers."""
        name, job, release = self._held_run_as(
            monkeypatch, tmp_path, "gw:alice", sid="agent-guar",
            update_interval_s=0,
        )
        monkeypatch.setattr(gc, "_resolve_session_key", lambda: "gw:bob")
        cursor_subscribe(name, 3600)
        assert gc_handles.subscribers_of(gc_handles.get(name)) == {
            "gw:bob": 3600.0
        }
        release.set()
        assert job.done_event.wait(10)

        completions = _completion_events(_drain_completion_queue())
        assert len(completions) == 2
        assert {c["session_key"] for c in completions} == {
            "gw:alice", "gw:bob"
        }

    def test_unsubscribed_non_dispatcher_gets_nothing(
        self, clean_state, monkeypatch, tmp_path
    ):
        """A hermes session that unsubscribed (interval 0) gets no digests
        AND no completion — the dispatcher guarantee is only for the
        dispatching session of the live run."""
        name, job, release = self._held_run_as(
            monkeypatch, tmp_path, "gw:alice", sid="agent-gone",
            update_interval_s=3600,
        )
        monkeypatch.setattr(gc, "_resolve_session_key", lambda: "gw:bob")
        cursor_subscribe(name, 3600)
        cursor_subscribe(name, 0)  # bob leaves before the run settles
        release.set()
        assert job.done_event.wait(10)

        completions = _completion_events(_drain_completion_queue())
        assert len(completions) == 1
        assert completions[0]["session_key"] == "gw:alice"

    def test_cli_empty_key_subscribers_dedupe_to_one_copy(
        self, clean_state, monkeypatch, tmp_path
    ):
        """The CLI's session_key is "" — an auto-subscribed "" dispatcher
        plus a "" cursor_subscribe is ONE subscription (same hermes
        session), so exactly one completion copy is enqueued."""
        name, job, release = self._held_run_as(
            monkeypatch, tmp_path, "", sid="agent-cli",
            update_interval_s=3600,
        )
        cursor_subscribe(name, 3600)  # same "" session: retune, no dup
        assert gc_handles.subscribers_of(gc_handles.get(name)) == {"": 3600.0}
        assert set(_sub_tickers(name)) == {""}
        release.set()
        assert job.done_event.wait(10)

        completions = _completion_events(_drain_completion_queue())
        assert len(completions) == 1
        assert completions[0]["session_key"] == ""
        assert completions[0]["delegation_id"] == name

    # -- legacy scalar migration ---------------------------------------------

    def test_legacy_scalar_migrates_as_the_dispatchers_subscription(
        self, clean_state
    ):
        gc_handles.record(
            "old-timer", repo="/tmp/r", status="created",
            session_key="gw:old", update_interval_s=45.0,
        )
        entry = gc_handles.get("old-timer")
        assert "subscribers" not in entry  # genuinely legacy-shaped
        assert gc_handles.subscribers_of(entry) == {"gw:old": 45.0}
        # The dispatch-time resolution honors the migrated subscription
        # for THAT hermes session only; others fall to the 180s default.
        assert gc_progress.resolve_interval(entry, None, "gw:old") == 45.0
        assert gc_progress.resolve_interval(entry, None, "gw:new") == 180.0

    def test_legacy_zero_scalar_is_no_subscription(self, clean_state):
        gc_handles.record(
            "old-quiet", session_key="gw:old", update_interval_s=0.0
        )
        assert gc_handles.subscribers_of(gc_handles.get("old-quiet")) == {}

    def test_set_subscriber_migrates_before_writing(self, clean_state):
        """A new subscriber landing on a legacy entry must not clobber the
        legacy subscription — the scalar migrates into the map first, and
        the scalar keeps mirroring the dispatcher for old readers."""
        gc_handles.record(
            "old-timer", session_key="gw:old", update_interval_s=45.0
        )
        gc_handles.set_subscriber("old-timer", "gw:new", 30.0)
        entry = gc_handles.get("old-timer")
        assert gc_handles.subscribers_of(entry) == {
            "gw:old": 45.0, "gw:new": 30.0,
        }
        assert entry["update_interval_s"] == 45.0  # dispatcher mirror

    def test_legacy_subscription_drives_the_next_run(
        self, clean_state, monkeypatch, tmp_path
    ):
        """End-to-end migration: a legacy entry's scalar seeds the
        dispatching session's ticker on the next send (no explicit
        interval), exactly like a persisted map subscription."""
        release = threading.Event()
        monkeypatch.setattr(
            gc_cloud, "run_cloud",
            _gated_replay_factory(release, sid="agent-legacy"),
        )
        monkeypatch.setattr(gc, "_resolve_session_key", lambda: "gw:old")
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        # Regress the entry to the legacy shape: scalar only, no map.
        with gc_handles._lock:
            gc_handles._table[name].pop("subscribers", None)
            gc_handles._table[name]["update_interval_s"] = 45.0
        _assert_running_ack(cursor_send_message(name, "task"))
        job = _job_for("agent-legacy")
        try:
            assert gc_progress._tickers[(name, "gw:old")].interval_s == 45.0
            assert gc_handles.subscribers_of(gc_handles.get(name)) == {
                "gw:old": 45.0
            }
        finally:
            release.set()
            assert job.done_event.wait(10)


# ---------------------------------------------------------------------------
# Zero-progress auto-retry (born from the stale-bridge live incident 2026-07-04)
# ---------------------------------------------------------------------------

def _terminal_error_replay(sid="agent-zp", retryable=True, retry_after=None,
                           meaningful=False, edit=False, content_chunks=0,
                           release=None):
    """A replay that settles with terminal status "error" (the enriched
    cloud.error payload from cloud_runner), optionally after progress —
    ``meaningful`` = one completed shell round, ``edit`` = one completed
    file edit (folds a file_diff), ``content_chunks`` = that many trivial
    narration deltas — optionally held open on ``release`` first."""

    def replay(task, workdir, inactivity_timeout_s=0.0, max_wall_s=0.0,
               cancel_check=None, agent_id=None, model=None,
               first_event_timeout_s=None, **_kw):
        yield ("cloud.session", {"agentId": sid, "cwd": str(workdir),
                               "model": "m", "resumed": bool(agent_id)})
        if release is not None:
            while not release.is_set():
                if cancel_check and cancel_check():
                    yield ("cloud.result", {"status": "cancelled"})
                    return
                time.sleep(0.01)
        for i in range(content_chunks):
            yield _narration_chunk(f"narration {i} ")
        if edit:
            yield ("cloud.message", {
                "type": "tool_call", "call_id": "e1", "name": "edit_file",
                "status": "running", "args": {"path": f"{workdir}/f1.py"},
            })
            yield ("cloud.message", {
                "type": "tool_call", "call_id": "e1", "name": "edit_file",
                "status": "completed",
                "result": {"path": f"{workdir}/f1.py",
                           "oldText": "a\n", "newText": "a\nb\n"},
            })
        if meaningful:
            yield ("cloud.message", {
                "type": "tool_call", "call_id": "t1", "name": "shell",
                "status": "running", "args": {"command": "ls"},
            })
            yield ("cloud.message", {
                "type": "tool_call", "call_id": "t1", "name": "shell",
                "status": "completed",
                "result": {"exitCode": 0, "stdout": "ok"},
            })
        yield ("cloud.error", {
            "error": "ServerError: bridge went stale",
            "retryable": retryable,
            "retry_after": retry_after,
            "run_status": "error",
        })

    return replay


def _lifecycle_trail(name):
    """(event, record) for every lifecycle event in a session's jsonl log,
    in seq order."""
    page = gc_eventlog.read_events(name, offset=0, limit=500)
    return [
        (e.get("event"), e)
        for e in (page or {}).get("events") or []
        if e.get("kind") == "lifecycle"
    ]


class TestZeroProgressAutoRetry:
    """A terminal-error run with ZERO meaningful events (born from the
    stale-bridge live incident 2026-07-04; the signature is transport-
    agnostic) is transparently re-sent on the same agent — jsonl-only
    lifecycle signal, no user-facing failure. Meaningful progress, a
    non-retryable error, or an exhausted budget surfaces the detailed
    failure from the error-observability path instead."""

    def _fast_retries(self, monkeypatch):
        """Zero the backoff ladder so retries fire immediately."""
        monkeypatch.setattr(gc, "_AUTO_RETRY_BACKOFF_S", (0.0, 0.0))

    def test_zero_progress_error_is_retried_and_retry_succeeds(
        self, clean_state, monkeypatch, tmp_path
    ):
        self._fast_retries(monkeypatch)
        release = threading.Event()
        seq = _SdkSequence(
            _terminal_error_replay(sid="agent-zp"),
            _gated_replay_factory(release, sid="agent-zp"),
        )
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)

        _assert_running_ack(_start_run("t", repo=str(tmp_path)))
        job = _job_for("agent-zp")
        # Same job across the retry — the digest subscription (default
        # 180s) keeps its ticker.
        assert gc_progress._tickers[(job.session_name, "")].job is job
        release.set()
        assert job.done_event.wait(10)

        # One job, retried in place, clean success — no user-facing failure.
        assert job.status == "completed"
        assert job.result["success"] is True
        assert "error" not in job.result
        assert len(seq.calls) == 2
        assert seq.calls[1]["agent_id"] == "agent-zp"  # SAME agent resumed

        events = _drain_completion_queue()
        completions = _completion_events(events)
        assert len(completions) == 1
        assert completions[0]["status"] == "completed"
        assert completions[0]["error"] is None

        # The jsonl log shows the transparent recovery, in order: failed
        # first run → autoretry marker → clean second run.
        trail = _lifecycle_trail(job.session_name)
        marks = [n for n, _ in trail
                 if n in ("run.started", "run.failed", "cloud.autoretry",
                          "run.completed")]
        assert marks == ["run.started", "run.failed", "cloud.autoretry",
                         "run.started", "run.completed"]
        autoretry = next(e for n, e in trail if n == "cloud.autoretry")
        assert autoretry["attempt"] == 1
        assert "zero-progress" in autoretry["reason"]
        assert "ServerError: bridge went stale" in autoretry["reason"]

    def test_error_after_meaningful_progress_does_not_auto_retry(
        self, clean_state, monkeypatch, tmp_path
    ):
        self._fast_retries(monkeypatch)
        release = threading.Event()
        seq = _SdkSequence(
            _terminal_error_replay(sid="agent-mp", meaningful=True,
                                   release=release),
        )
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)

        _assert_running_ack(_start_run("t", repo=str(tmp_path)))
        job = _job_for("agent-mp")
        release.set()
        assert job.done_event.wait(10)

        assert len(seq.calls) == 1  # no re-send
        assert job.status == "failed"
        assert not [n for n, _ in _lifecycle_trail(job.session_name)
                    if n == "cloud.autoretry"]
        completions = _completion_events(_drain_completion_queue())
        assert len(completions) == 1
        evt = completions[0]
        assert evt["status"] == "failed"
        assert evt["error"] == "ServerError: bridge went stale"
        # The detailed failure from the error-observability path.
        assert ("run failed: ServerError: bridge went stale (retryable)"
                in evt["summary"])

    def test_retries_exhausted_surface_the_detailed_failure(
        self, clean_state, monkeypatch, tmp_path
    ):
        self._fast_retries(monkeypatch)
        release = threading.Event()
        seq = _SdkSequence(
            _terminal_error_replay(sid="agent-ex", release=release,
                                   retry_after="0"),
            _terminal_error_replay(sid="agent-ex"),
            _terminal_error_replay(sid="agent-ex"),
            _terminal_error_replay(sid="agent-ex"),
        )
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)

        _assert_running_ack(_start_run("t", repo=str(tmp_path)))
        job = _job_for("agent-ex")
        release.set()
        assert job.done_event.wait(10)

        assert len(seq.calls) == 4  # the send + all three retries (RFC §5 cap)
        assert job.status == "failed"
        trail = [e for n, e in _lifecycle_trail(job.session_name)
                 if n == "cloud.autoretry"]
        assert [e["attempt"] for e in trail] == [1, 2, 3]
        completions = _completion_events(_drain_completion_queue())
        assert len(completions) == 1
        evt = completions[0]
        assert evt["error"] == "ServerError: bridge went stale"
        assert evt["result"]["error_retryable"] is True
        assert ("run failed: ServerError: bridge went stale (retryable)"
                in evt["summary"])

    def test_error_after_file_diff_progress_does_not_auto_retry(
        self, clean_state, monkeypatch, tmp_path
    ):
        """Issue #17: a run that edited files (a folded file_diff — the
        signature of committed/pushed work) and then died with terminal
        status "error" must NOT be re-prompted; the failure is delivered
        immediately through the normal path."""
        self._fast_retries(monkeypatch)
        release = threading.Event()
        seq = _SdkSequence(
            _terminal_error_replay(sid="agent-fd", edit=True,
                                   release=release),
        )
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)

        _assert_running_ack(_start_run("t", repo=str(tmp_path)))
        job = _job_for("agent-fd")
        release.set()
        assert job.done_event.wait(10)

        assert len(seq.calls) == 1  # no re-send
        assert job.status == "failed"
        assert not [n for n, _ in _lifecycle_trail(job.session_name)
                    if n == "cloud.autoretry"]
        completions = _completion_events(_drain_completion_queue())
        assert len(completions) == 1
        evt = completions[0]
        assert evt["status"] == "failed"
        assert evt["error"] == "ServerError: bridge went stale"
        # The partial work travels with the delivered failure.
        assert evt["result"]["partial"] is True
        assert evt["result"]["files_changed_count"] == 1

    def test_trivial_content_only_error_still_auto_retries(
        self, clean_state, monkeypatch, tmp_path
    ):
        """Issue #17 gate calibration: a half-started narration (a couple
        of content deltas, no diffs, no completed tools) is still the
        zero-progress signature — the transparent retry must fire and its
        sdk.autoretry marker must land in the session jsonl."""
        self._fast_retries(monkeypatch)
        release, release2 = threading.Event(), threading.Event()
        release2.set()  # the retry run flows straight through
        seq = _SdkSequence(
            _terminal_error_replay(sid="agent-tc", content_chunks=2,
                                   release=release),
            _gated_replay_factory(release2, sid="agent-tc"),
        )
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)

        _assert_running_ack(_start_run("t", repo=str(tmp_path)))
        job = _job_for("agent-tc")
        release.set()
        assert job.done_event.wait(10)

        assert len(seq.calls) == 2  # retried once, in place
        assert job.status == "completed"
        trail = [e for n, e in _lifecycle_trail(job.session_name)
                 if n == "cloud.autoretry"]
        assert [e["attempt"] for e in trail] == [1]
        assert "zero-progress" in trail[0]["reason"]
        completions = _completion_events(_drain_completion_queue())
        assert len(completions) == 1
        assert completions[0]["status"] == "completed"

    def test_zero_event_retry_gets_tight_first_event_watchdog(
        self, clean_state, monkeypatch, tmp_path
    ):
        """Issue #17: the retry attempt is dispatched with the TIGHT
        first-event window (independent of the user's inactivity timeout);
        a retry that streams nothing settles the job FAILED — not a
        multi-minute silent "running" zombie — and the failure is
        delivered."""
        self._fast_retries(monkeypatch)
        monkeypatch.setattr(gc, "_AUTO_RETRY_FIRST_EVENT_S", 0.2)
        release = threading.Event()

        def silent_retry(task, workdir, inactivity_timeout_s=0.0,
                         max_wall_s=0.0, cancel_check=None, agent_id=None,
                         model=None, first_event_timeout_s=None, **_kw):
            yield ("cloud.session", {"agentId": "agent-wd", "cwd": str(workdir),
                                   "model": "m", "resumed": bool(agent_id)})
            if not first_event_timeout_s:
                # Unfixed plumbing (no watchdog handed to the retry):
                # finish clean so the assertions below fail fast instead
                # of hanging on a watchdog that never fires.
                yield ("cloud.result", {"status": "finished"})
                return
            # Emulate the real run_sdk contract (unit-tested separately in
            # TestSdkRunner): total silence until the first-event window
            # lapses, then the plain non-timeout, no-run_status error.
            deadline = time.monotonic() + float(first_event_timeout_s)
            while time.monotonic() < deadline:
                if cancel_check and cancel_check():
                    yield ("cloud.result", {"status": "cancelled"})
                    return
                time.sleep(0.01)
            yield ("cloud.error", {"error": (
                "cursor run aborted: produced no stream events within "
                f"{first_event_timeout_s}s of dispatch"
            )})

        seq = _SdkSequence(
            _terminal_error_replay(sid="agent-wd", release=release),
            silent_retry,
        )
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)

        _assert_running_ack(
            cursor_send_message(
                _created_name(cursor_create_session(repo=str(tmp_path))),
                "t", inactivity_timeout_s=1800,
            )
        )
        job = _job_for("agent-wd")
        release.set()
        assert job.done_event.wait(10)

        # The first attempt ran without the watchdog; the retry got the
        # tight window, NOT the user's 1800s inactivity timeout.
        assert len(seq.calls) == 2
        assert seq.calls[0]["first_event_timeout_s"] is None
        assert seq.calls[1]["first_event_timeout_s"] == 0.2
        assert seq.calls[1]["inactivity_timeout_s"] == 1800.0

        # Settled FAILED (not timeout/cancelled), no further retries, and
        # the failure delivered with the autoretry marker in the log.
        assert job.status == "failed"
        assert "no stream events" in (job.run_error or "")
        trail = [e for n, e in _lifecycle_trail(job.session_name)
                 if n == "cloud.autoretry"]
        assert [e["attempt"] for e in trail] == [1]
        completions = _completion_events(_drain_completion_queue())
        assert len(completions) == 1
        assert completions[0]["status"] == "failed"
        assert "no stream events" in completions[0]["error"]

    def test_non_retryable_zero_progress_error_does_not_retry(
        self, clean_state, monkeypatch, tmp_path
    ):
        self._fast_retries(monkeypatch)
        release = threading.Event()
        seq = _SdkSequence(
            _terminal_error_replay(sid="agent-nr", retryable=False,
                                   release=release),
        )
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)

        _assert_running_ack(_start_run("t", repo=str(tmp_path)))
        job = _job_for("agent-nr")
        release.set()
        assert job.done_event.wait(10)

        assert len(seq.calls) == 1
        assert job.status == "failed"
        assert not [n for n, _ in _lifecycle_trail(job.session_name)
                    if n == "cloud.autoretry"]
        completions = _completion_events(_drain_completion_queue())
        assert len(completions) == 1
        assert ("run failed: ServerError: bridge went stale (not retryable)"
                in completions[0]["summary"])


# ---------------------------------------------------------------------------
# Registration + availability gate
# ---------------------------------------------------------------------------

class TestRegistration:
    def test_register_wires_seven_tools_into_ghost_cursor_toolset(self):
        calls = []
        ctx = SimpleNamespace(register_tool=lambda **kw: calls.append(kw))
        register(ctx)

        by_name = {c["name"]: c for c in calls}
        assert set(by_name) == {
            CREATE_TOOL_NAME, SEND_TOOL_NAME, STATUS_TOOL_NAME,
            STOP_TOOL_NAME, EVENTS_TOOL_NAME, LIST_TOOL_NAME,
            SUBSCRIBE_TOOL_NAME,
        }
        for name, schema, required in (
            (CREATE_TOOL_NAME, CURSOR_CREATE_SCHEMA, []),
            (SEND_TOOL_NAME, CURSOR_SEND_SCHEMA, ["session", "message"]),
            (STATUS_TOOL_NAME, CURSOR_STATUS_SCHEMA, ["session"]),
            (STOP_TOOL_NAME, CURSOR_STOP_SCHEMA, ["session"]),
            (EVENTS_TOOL_NAME, CURSOR_EVENTS_SCHEMA, ["session"]),
            (LIST_TOOL_NAME, CURSOR_LIST_SCHEMA, []),
            (SUBSCRIBE_TOOL_NAME, CURSOR_SUBSCRIBE_SCHEMA, ["session", "interval_s"]),
        ):
            entry = by_name[name]
            assert entry["toolset"] == TOOLSET
            assert entry["check_fn"] is check_cursor_available
            assert entry["schema"] is schema
            assert entry["schema"]["parameters"]["required"] == required
            assert callable(entry["handler"])

    def test_send_schema_steers_delegation(self):
        desc = CURSOR_SEND_SCHEMA["description"].lower()
        assert "prefer this" in desc
        assert "delegate" in desc

    def test_create_schema_promises_lazy_spawn(self):
        desc = CURSOR_CREATE_SCHEMA["description"].lower()
        assert "dispatches nothing" in desc
        assert "lazily" in desc

    def test_create_schema_title_asks_for_a_meaningful_phrase(self):
        prop = CURSOR_CREATE_SCHEMA["parameters"]["properties"]["title"]
        desc = prop["description"]
        assert "WITH SPACES" in desc
        assert "3-8 words" in desc
        assert "Fix payment webhook retries" in desc  # a concrete example
        assert str(gc.MAX_TITLE_CHARS) in desc
        # Optional: create must keep working without a title (fallback).
        assert "title" not in CURSOR_CREATE_SCHEMA["parameters"]["required"]

    def test_events_schema_documents_paging_semantics(self):
        desc = CURSOR_EVENTS_SCHEMA["description"]
        assert "LAST 10" in desc
        assert "kind" in desc
        assert "2KB" in desc and "20KB" in desc

    def test_status_schema_promises_read_only(self):
        desc = CURSOR_STATUS_SCHEMA["description"].lower()
        assert "read-only" in desc
        assert "never cancels" in desc

    def test_send_schema_states_interrupt_semantics(self):
        desc = CURSOR_SEND_SCHEMA["description"].lower()
        assert "interrupt" in desc
        assert "re-prompt" in desc

    def test_check_fn_false_without_http_layer(self, monkeypatch):
        monkeypatch.setattr(gc_cloud, "rest_available", lambda: False)
        assert check_cursor_available() is False

    def test_check_fn_false_without_resolvable_repo(self, monkeypatch):
        monkeypatch.setattr(gc_cloud, "rest_available", lambda: True)
        monkeypatch.setattr(gc, "_default_repo", lambda: None)
        assert check_cursor_available() is False

    def test_check_fn_true_with_http_layer_and_repo(self, monkeypatch):
        monkeypatch.setattr(gc_cloud, "rest_available", lambda: True)
        # _default_repo falls back to os.getcwd(), which always exists.
        assert check_cursor_available() is True

    def test_check_fn_never_raises(self, monkeypatch):
        def boom():
            raise RuntimeError("probe failed")

        monkeypatch.setattr(gc_cloud, "rest_available", boom)
        assert check_cursor_available() is False


# ---------------------------------------------------------------------------
# Legacy --print runner + normalize_harness (kept as fallback/reference)
# ---------------------------------------------------------------------------

def _fixture_events():
    out = []
    for line in FIXTURE.read_text().splitlines():
        obj = json.loads(line)
        out.append((gc_runner.event_key(obj), obj))
    return out


class TestNormalizeHarness:
    def _all_envelopes(self):
        envs = []
        for key, obj in _fixture_events():
            envs.extend(gc_events.normalize_harness(key, obj))
        return envs

    def test_every_envelope_carries_ghost_source(self):
        envs = self._all_envelopes()
        assert envs, "fixture produced no envelopes"
        assert all(e.get("source") == "ghost" for e in envs)

    def test_edit_tool_call_yields_file_diff(self):
        diffs = [e for e in self._all_envelopes() if e["kind"] == "file_diff"]
        assert len(diffs) == 1
        d = diffs[0]
        assert d["path"] == "/tmp/ph2probe/calc.py"
        assert d["added"] == 3
        assert d["removed"] == 0
        assert d["status"] == "M"  # existing file modified
        assert "multiply" in d["after"]
        assert "multiply" not in d["before"]
        assert d["diff"]  # diffString captured

    def test_thinking_maps_to_reasoning_lifecycle(self):
        reasoning = [
            e for e in self._all_envelopes()
            if e["kind"] == "lifecycle" and e.get("event") == "reasoning"
        ]
        assert reasoning
        joined = "".join(e["text"] for e in reasoning)
        assert "calc.py" in joined

    def test_harness_error_timeout_maps_to_run_failed(self):
        envs = gc_events.normalize_harness(
            "harness.error", {"error": "harness timed out after 600s", "timeout": True}
        )
        assert len(envs) == 1
        assert envs[0]["event"] == "run.failed"
        assert envs[0]["timeout"] is True

    def test_unknown_event_passes_through(self):
        envs = gc_events.normalize_harness("mystery.event", {"x": 1})
        assert envs == [
            {"source": "ghost", "kind": "lifecycle", "event": "passthrough",
             "name": "mystery.event", "data": {"x": 1}}
        ]

    def test_file_diff_truncates_oversized_content(self):
        big = "x" * (gc_events.MAX_CONTENT_CHARS + 500)
        env = gc_events.file_diff("f.py", before=big, after="y", diff="d")
        assert len(env["before"]) < len(big)
        assert "truncated" in env["before"]


class TestLegacyRunner:
    def test_event_key_composition(self):
        assert gc_runner.event_key({"type": "tool_call", "subtype": "completed"}) == "tool_call.completed"
        assert gc_runner.event_key({"type": "assistant"}) == "assistant"

    def test_empty_task_raises(self, tmp_path):
        with pytest.raises(gc_runner.HarnessError):
            list(gc_runner.run_harness("  ", str(tmp_path)))

    def test_bad_repo_raises(self):
        with pytest.raises(gc_runner.HarnessError):
            gc_runner.resolve_repo("/nope/nothing/here")

    def test_local_bin_prepended_to_path(self):
        env = gc_runner.subprocess_env()
        assert str(Path.home() / ".local" / "bin") in env["PATH"].split(":")

    def test_run_harness_skips_noise_and_yields_json(self, tmp_path, monkeypatch):
        """Drive run_harness with a fake 'cursor-agent' that prints noise +
        JSON lines, verifying parse/skip behavior end-to-end via a real
        subprocess."""
        fake = tmp_path / "cursor-agent"
        fake.write_text(
            "#!/bin/sh\n"
            "echo 'not json banner'\n"
            "echo '{\"type\":\"assistant\",\"message\":{\"role\":\"assistant\","
            "\"content\":[{\"type\":\"text\",\"text\":\"hi\"}]}}'\n"
        )
        fake.chmod(0o755)
        monkeypatch.setattr(
            gc_runner, "subprocess_env", _prepend_path_env(tmp_path),
        )
        events = list(gc_runner.run_harness("do it", str(tmp_path)))
        assert events == [
            ("assistant", {
                "type": "assistant",
                "message": {"role": "assistant",
                            "content": [{"type": "text", "text": "hi"}]},
            })
        ]

    def test_run_harness_timeout_kills_and_reports(self, tmp_path, monkeypatch):
        # `sleep 30` is a grandchild holding the stdout pipe — the group kill
        # must reap it too, or readline blocks for the full 30s (regression
        # caught by this test before start_new_session + killpg were added).
        fake = tmp_path / "cursor-agent"
        fake.write_text("#!/bin/sh\nsleep 30\n")
        fake.chmod(0o755)
        monkeypatch.setattr(
            gc_runner, "subprocess_env", _prepend_path_env(tmp_path),
        )
        t0 = time.monotonic()
        events = list(gc_runner.run_harness("do it", str(tmp_path), timeout=1.0))
        elapsed = time.monotonic() - t0
        assert events[-1][0] == "harness.error"
        assert events[-1][1]["timeout"] is True
        assert elapsed < 15, f"group kill did not tear down the pipe ({elapsed:.1f}s)"


# ---------------------------------------------------------------------------
# CursorJob progress buffer — rolling, bounded, line-safe
# ---------------------------------------------------------------------------

class TestProgressBuffer:
    def test_buffer_accumulates_and_rolls(self, clean_state):
        job = gc_jobs.CursorJob(job_id="cursor_test", task="t", repo="/r",
                                inactivity_timeout_s=60)
        monkey_cap = 500
        old_cap = gc_jobs.MAX_PROGRESS_CHARS
        gc_jobs.MAX_PROGRESS_CHARS = monkey_cap
        try:
            for i in range(100):
                job.append_progress({"kind": "lifecycle", "event": "reasoning",
                                     "text": f"chunk {i:03d} " + "x" * 40})
            assert len(job.progress_buffer) <= monkey_cap
            assert job.progress_events == 100
            # Oldest entries rolled out, newest survive.
            assert "chunk 099" in job.progress_buffer
            assert "chunk 000" not in job.progress_buffer
            # Rolling trims at a line boundary — every line stays valid JSON.
            for line in job.progress_buffer.strip().splitlines():
                json.loads(line)
        finally:
            gc_jobs.MAX_PROGRESS_CHARS = old_cap

    def test_buffer_drops_full_file_content_but_keeps_diff(self, clean_state):
        job = gc_jobs.CursorJob(job_id="cursor_test2", task="t", repo="/r",
                                inactivity_timeout_s=60)
        job.append_progress({
            "kind": "file_diff", "path": "/r/f.py",
            "before": "B" * 10_000, "after": "A" * 10_000,
            "diff": "+line\n" * 1_000, "added": 1000, "removed": 0, "status": "M",
        })
        line = json.loads(job.progress_buffer.strip())
        assert "before" not in line and "after" not in line
        assert line["path"] == "/r/f.py"
        assert len(line["diff"]) <= gc_jobs._BUFFER_DIFF_CHARS + 40


# ---------------------------------------------------------------------------
# eventlog.py — per-session JSONL spill log: creation, pagination, compaction
# ---------------------------------------------------------------------------

def _log_lines(sid):
    path = gc_eventlog.log_path(sid)
    assert path is not None and path.is_file(), f"no spill log for {sid!r}"
    return [json.loads(l) for l in path.read_text("utf-8").splitlines() if l.strip()]


class TestEventLog:
    def test_append_creates_jsonl_under_hermes_home(
        self, clean_state, _isolate_hermes_home
    ):
        gc_eventlog.append("s-log", {"kind": "content", "delta": "hi"})
        path = gc_eventlog.log_path("s-log")
        assert path == (
            _isolate_hermes_home / "state" / "ghost_cursor" / "logs" / "s-log.jsonl"
        )
        lines = _log_lines("s-log")
        assert len(lines) == 1
        assert lines[0]["seq"] == 0
        assert lines[0]["ts"] > 0
        assert lines[0]["kind"] == "content"
        assert lines[0]["delta"] == "hi"

    def test_seq_is_monotonic_and_survives_a_process_restart(self, clean_state):
        for i in range(3):
            gc_eventlog.append("s-seq", {"kind": "content", "delta": f"e{i}"})
        # Simulate a fresh process: the writer must re-derive next_seq from
        # the file tail, not restart at 0.
        gc_eventlog._reset_for_tests()
        gc_eventlog.append("s-seq", {"kind": "content", "delta": "e3"})
        assert [l["seq"] for l in _log_lines("s-seq")] == [0, 1, 2, 3]

    def test_unsafe_handle_characters_cannot_escape_the_logs_dir(self, clean_state):
        gc_eventlog.append("../../evil/sid", {"kind": "content", "delta": "x"})
        path = gc_eventlog.log_path("../../evil/sid")
        assert path.parent == gc_eventlog.logs_dir()
        assert path.is_file()

    def test_append_never_raises_without_a_session_id(self, clean_state):
        gc_eventlog.append("", {"kind": "content", "delta": "x"})
        gc_eventlog.append(None, {"kind": "content", "delta": "x"})

    def test_stats_reports_path_and_total_events(self, clean_state):
        assert gc_eventlog.stats("s-none") is None
        for i in range(5):
            gc_eventlog.append("s-st", {"kind": "content", "delta": str(i)})
        stats = gc_eventlog.stats("s-st")
        assert stats["path"] == str(gc_eventlog.log_path("s-st"))
        assert stats["total_events"] == 5

    def test_read_page_slices_by_seq(self, clean_state):
        for i in range(30):
            gc_eventlog.append("s-page", {"kind": "content", "delta": f"event {i:02d}"})

        page = gc_eventlog.read_page("s-page", offset=10, limit=5)
        assert [e["seq"] for e in page["events"]] == [10, 11, 12, 13, 14]
        assert page["events"][0]["delta"] == "event 10"
        assert page["offset"] == 10 and page["limit"] == 5
        assert page["total_events"] == 30
        assert page["log_path"] == str(gc_eventlog.log_path("s-page"))

        # Last (partial) page, then past-the-end.
        assert [e["seq"] for e in gc_eventlog.read_page(
            "s-page", offset=28, limit=10)["events"]] == [28, 29]
        assert gc_eventlog.read_page("s-page", offset=100, limit=10)["events"] == []

    def test_read_page_defaults_and_bad_params_are_forgiving(self, clean_state):
        for i in range(3):
            gc_eventlog.append("s-def", {"kind": "content", "delta": str(i)})
        page = gc_eventlog.read_page("s-def")
        assert len(page["events"]) == 3
        page = gc_eventlog.read_page("s-def", offset="junk", limit="junk")
        assert page["offset"] == 0 and page["limit"] == gc_eventlog.DEFAULT_PAGE_LIMIT
        assert gc_eventlog.read_page("s-def", offset=-5, limit=0)["limit"] == 1
        assert gc_eventlog.read_page(
            "s-def", limit=10_000)["limit"] == gc_eventlog.MAX_PAGE_LIMIT
        assert gc_eventlog.read_page("s-missing") is None

    def test_oversized_fields_clip_inline_but_stay_full_in_the_jsonl(
        self, clean_state
    ):
        big = "x" * (gc_eventlog.PAGE_FIELD_CHARS * 3)
        gc_eventlog.append("s-big", {"kind": "tool_result", "id": "t1",
                                     "status": "done", "output": big})
        # Full fidelity on disk...
        assert _log_lines("s-big")[0]["output"] == big
        # ...clipped inline on the paged view, with a pointer to the log.
        paged = gc_eventlog.read_page("s-big", offset=0, limit=1)["events"][0]
        assert len(paged["output"]) < len(big)
        assert paged["output"].startswith("x" * gc_eventlog.PAGE_FIELD_CHARS)
        assert "truncated" in paged["output"]
        assert paged["status"] == "done"  # small fields untouched

    def test_log_compacts_to_head_plus_tail_at_the_byte_cap(
        self, clean_state, monkeypatch
    ):
        monkeypatch.setattr(gc_eventlog, "MAX_LOG_BYTES", 20_000)
        monkeypatch.setattr(gc_eventlog, "HEAD_RETAIN_BYTES", 5_000)
        monkeypatch.setattr(gc_eventlog, "TAIL_RETAIN_BYTES", 10_000)

        n = 300  # ~100 bytes/line → several compactions past the 20KB cap
        for i in range(n):
            gc_eventlog.append("s-cap", {"kind": "content",
                                         "delta": f"event {i:03d} " + "p" * 60})

        path = gc_eventlog.log_path("s-cap")
        assert path.stat().st_size <= 20_000

        lines = _log_lines("s-cap")
        seqs = [l["seq"] for l in lines if isinstance(l.get("seq"), int)]
        markers = [l for l in lines if l.get("kind") == "log_compaction"]
        # Head (oldest) and tail (newest) both survive; the middle is gone.
        assert seqs[0] == 0
        assert seqs[-1] == n - 1
        assert len(seqs) < n
        assert markers, "compaction must leave a marker in the gap"
        marker = markers[-1]
        assert marker["dropped_events"] > 0
        assert 0 < marker["first_dropped_seq"] <= marker["last_dropped_seq"] < n - 1
        # Every retained line is still valid JSON with its ORIGINAL seq —
        # compaction never renumbers.
        assert seqs == sorted(seqs)
        # total_events counts everything ever appended, dropped included.
        assert gc_eventlog.stats("s-cap")["total_events"] == n

    def test_read_page_reports_compaction_gaps_in_range(
        self, clean_state, monkeypatch
    ):
        monkeypatch.setattr(gc_eventlog, "MAX_LOG_BYTES", 20_000)
        monkeypatch.setattr(gc_eventlog, "HEAD_RETAIN_BYTES", 5_000)
        monkeypatch.setattr(gc_eventlog, "TAIL_RETAIN_BYTES", 10_000)
        for i in range(300):
            gc_eventlog.append("s-gap", {"kind": "content",
                                         "delta": f"event {i:03d} " + "p" * 60})
        marker = [l for l in _log_lines("s-gap")
                  if l.get("kind") == "log_compaction"][-1]
        dropped_seq = marker["first_dropped_seq"]

        page = gc_eventlog.read_page("s-gap", offset=dropped_seq, limit=5)
        assert page["gaps"], "a page over dropped seqs must disclose the gap"
        assert "compaction" in page["note"]
        # A page fully inside the retained tail has no gap disclosure.
        tail_page = gc_eventlog.read_page("s-gap", offset=295, limit=5)
        assert len(tail_page["events"]) == 5
        assert "gaps" not in tail_page


# ---------------------------------------------------------------------------
# Spill integration — runs stream to the JSONL log; cursor_status pages it
# ---------------------------------------------------------------------------

class TestEventLogIntegration:
    def _run_to_completion(self, monkeypatch, tmp_path, sid="s-spill"):
        monkeypatch.setattr(
            gc_cloud, "run_cloud", _gated_replay_factory(_preset_event(), sid=sid)
        )
        _start_run("t", repo=str(tmp_path))
        job = _job_for(sid)
        assert job.done_event.wait(10)
        return job

    def test_run_streams_full_envelopes_to_the_session_log(
        self, clean_state, monkeypatch, tmp_path
    ):
        """The log is keyed by the session NAME — one named session, one log."""
        job = self._run_to_completion(monkeypatch, tmp_path)
        lines = _log_lines(job.session_name)
        # Every folded envelope landed, in order, seq from 0 — plus the
        # supervisor's log-only session.settled event appended at finalize.
        assert [l["seq"] for l in lines] == list(range(len(lines)))
        assert len(lines) == job.progress_events + 1
        assert lines[-1]["event"] == "session.settled"
        kinds = [l["kind"] for l in lines]
        assert "lifecycle" in kinds and "tool_result" in kinds
        # FULL fidelity: file_diff lines keep before/after content that the
        # in-memory rolling buffer strips.
        diffs = [l for l in lines if l["kind"] == "file_diff"]
        assert diffs and all("before" in d and "after" in d for d in diffs)

    def test_cursor_status_reports_log_path_and_total_event_count(
        self, clean_state, monkeypatch, tmp_path
    ):
        job = self._run_to_completion(monkeypatch, tmp_path)
        status = cursor_status("s-spill")
        path = str(gc_eventlog.log_path(job.session_name))
        # +1: the supervisor's session.settled event is log-only (never
        # buffered), so the log total leads progress_events by one.
        assert f"events: {job.progress_events + 1} total · log: {path}" in status

    def test_cursor_events_pages_the_persisted_log_forward(
        self, clean_state, monkeypatch, tmp_path
    ):
        job = self._run_to_completion(monkeypatch, tmp_path)
        total = job.progress_events + 1  # + the settled event (log-only)

        first = cursor_events("s-spill", offset=0, limit=2)
        assert f"events 0–1 of {total}" in first

        rest = cursor_events("s-spill", offset=2, limit=500)
        assert f"events 2–{total - 1} of {total}" in rest

    def test_events_page_across_a_process_restart(
        self, clean_state, monkeypatch, tmp_path
    ):
        """The log outlives the process: a persisted handle with no live job
        still pages, and status still shows the log location."""
        job = self._run_to_completion(monkeypatch, tmp_path)
        name = job.session_name
        gc_jobs.registry._reset_for_tests()
        gc_eventlog._reset_for_tests()

        status = cursor_status("s-spill")
        assert "not tracked live" in status
        assert str(gc_eventlog.log_path(name)) in status

        page = cursor_events("s-spill", offset=0, limit=3)
        assert "events 0–2 of" in page

    def test_paging_an_unknown_log_is_graceful(self, clean_state, tmp_path):
        gc_handles.record("s-nolog", repo=str(tmp_path), status="completed")
        out = cursor_events("s-nolog", offset=0, limit=5)
        assert "no events recorded for session 's-nolog'" in out

    def test_oversized_tool_output_full_in_log_clipped_on_page(
        self, clean_state, monkeypatch, tmp_path
    ):
        """Massive per-event output: full in the JSONL, clipped to 2KB with
        an explicit marker when paged back through cursor_events."""
        big = "y" * 50_000

        def replay(task, workdir, inactivity_timeout_s=0.0, max_wall_s=0.0,
                   cancel_check=None, agent_id=None, model=None, **_kw):
            yield ("cloud.session", {"agentId": "s-bigout", "cwd": str(workdir),
                                   "model": "m", "resumed": False})
            yield ("cloud.message", {
                "type": "tool_call", "call_id": "t1", "name": "shell",
                "status": "running", "args": {"command": "generate"},
            })
            yield ("cloud.message", {
                "type": "tool_call", "call_id": "t1", "name": "shell",
                "status": "completed", "result": {"exitCode": 0, "stdout": big},
            })
            yield ("cloud.result", {"status": "finished"})

        monkeypatch.setattr(gc_cloud, "run_cloud", replay)
        _start_run("t", repo=str(tmp_path))
        job = _job_for("s-bigout")
        assert job.done_event.wait(10)

        on_disk = [l for l in _log_lines(job.session_name)
                   if l["kind"] == "tool_result"][0]
        assert on_disk["output"] == big
        paged = cursor_events("s-bigout", offset=on_disk["seq"], limit=1)
        assert len(paged) < 4_000  # 2KB body + headers, nowhere near 50KB
        assert "y" * 100 in paged
        assert "full event in the JSONL log" in paged


# ---------------------------------------------------------------------------
# Handle scoping — session_key isolation, scope='all', explicit-id crossing
# ---------------------------------------------------------------------------

class TestHandleScoping:
    def _seed_two_sessions(self):
        gc_handles.record("s-alice", repo="/r/a", status="running",
                          session_key="gw:alice")
        gc_handles.record("s-bob", repo="/r/b", status="completed",
                          session_key="gw:bob")

    def test_session_scope_isolates_two_session_keys(self, clean_handles):
        self._seed_two_sessions()
        assert gc_handles.known_handles(
            scope="session", session_key="gw:alice") == ["s-alice"]
        assert gc_handles.known_handles(
            scope="session", session_key="gw:bob") == ["s-bob"]

    def test_scope_all_sees_both(self, clean_handles):
        self._seed_two_sessions()
        assert set(gc_handles.known_handles(scope="all", session_key="gw:alice")) == {
            "s-alice", "s-bob",
        }

    def test_explicit_id_lookup_crosses_scopes(self, clean_handles):
        self._seed_two_sessions()
        # get() takes no scope at all — an explicit handle is explicit intent.
        assert gc_handles.get("s-bob")["session_key"] == "gw:bob"
        assert gc_handles.get("s-alice")["repo"] == "/r/a"

    def test_legacy_entries_without_session_key_belong_to_cli(self, clean_handles):
        gc_handles.record("s-legacy", repo="/r", status="completed")  # no key
        assert gc_handles.known_handles(scope="session", session_key="") == ["s-legacy"]
        assert gc_handles.known_handles(scope="session", session_key="gw:x") == []

    def test_unknown_scope_falls_back_to_session(self, clean_handles):
        self._seed_two_sessions()
        assert gc_handles.known_handles(
            scope="everything", session_key="gw:alice") == ["s-alice"]

    def test_dispatch_records_the_hermes_session_key_on_the_handle(
        self, clean_state, monkeypatch, tmp_path
    ):
        monkeypatch.setattr(gc, "_resolve_session_key", lambda: "gw:alice")
        monkeypatch.setattr(
            gc_cloud, "run_cloud", _gated_replay_factory(_preset_event(), sid="s-mine")
        )
        _start_run("t", repo=str(tmp_path))
        assert _job_for("s-mine").done_event.wait(10)
        assert gc_handles.get("s-mine")["session_key"] == "gw:alice"

    def test_unknown_handle_hints_are_scoped_by_default(
        self, clean_state, monkeypatch
    ):
        """Two Hermes sessions share the table: session A's error hints must
        not leak session B's handles unless scope='all' is requested."""
        gc_handles.record("s-mine", repo="/r", status="completed",
                          session_key="gw:alice")
        gc_handles.record("s-theirs", repo="/r", status="completed",
                          session_key="gw:bob")
        monkeypatch.setattr(gc, "_resolve_session_key", lambda: "gw:alice")

        for out in (
            cursor_status("s-nope"),
            cursor_send_message("s-nope", "hi"),
            cursor_stop("s-nope"),
            cursor_events("s-nope"),
        ):
            assert "no session named 's-nope'" in out
            assert "s-mine" in out
            assert "s-theirs" not in out

        opened = cursor_status("s-nope", scope="all")
        assert "s-mine" in opened and "s-theirs" in opened

    def test_explicit_status_lookup_crosses_hermes_sessions(
        self, clean_state, monkeypatch
    ):
        gc_handles.record("s-theirs", repo="/r/b", status="completed",
                          session_key="gw:bob", task="their task")
        monkeypatch.setattr(gc, "_resolve_session_key", lambda: "gw:alice")
        status = cursor_status("s-theirs")
        assert status.startswith("status: completed")
        assert "working on: their task" in status


# ---------------------------------------------------------------------------
# Handle pruning — age out old terminal entries; cap spares live handles
# ---------------------------------------------------------------------------

class TestHandlePrune:
    def test_old_terminal_handles_age_out_on_write(self, clean_handles):
        gc_handles.record("s-old-done", status="completed")
        gc_handles._table["s-old-done"]["updated_at"] = (
            time.time() - gc_handles.PRUNE_TERMINAL_AFTER_S - 60
        )
        gc_handles.record("s-fresh", status="running")
        assert gc_handles.get("s-old-done") is None
        assert gc_handles.get("s-fresh") is not None

    def test_old_running_handles_are_not_aged_out(self, clean_handles):
        """Age-based pruning only touches TERMINAL states — a long-running
        (or stale-but-unresolved) handle is not silently forgotten."""
        gc_handles.record("s-old-run", status="running")
        gc_handles._table["s-old-run"]["updated_at"] = (
            time.time() - gc_handles.PRUNE_TERMINAL_AFTER_S - 60
        )
        gc_handles.record("s-fresh", status="running")
        assert gc_handles.get("s-old-run") is not None

    def test_recent_terminal_handles_survive(self, clean_handles):
        gc_handles.record("s-done", status="completed")
        gc_handles.record("s-fresh", status="running")
        assert gc_handles.get("s-done") is not None

    def test_cap_evicts_terminal_entries_before_running_ones(
        self, clean_handles, monkeypatch
    ):
        monkeypatch.setattr(gc_handles, "MAX_ENTRIES", 3)
        now = time.time()
        gc_handles.record("s-run-old", status="running")
        gc_handles._table["s-run-old"]["updated_at"] = now - 300
        gc_handles.record("s-done-new", status="completed")
        gc_handles._table["s-done-new"]["updated_at"] = now - 10
        gc_handles.record("s-run-new", status="running")
        gc_handles._table["s-run-new"]["updated_at"] = now - 5
        gc_handles.record("s-final", status="running")
        # Over cap by one: the TERMINAL entry goes, even though a running
        # entry is older — finished runs never push out live handles.
        assert len(gc_handles._table) <= 3
        assert gc_handles.get("s-done-new") is None
        assert gc_handles.get("s-run-old") is not None
        assert gc_handles.get("s-run-new") is not None
        assert gc_handles.get("s-final") is not None

    def test_cap_still_enforced_when_everything_is_running(
        self, clean_handles, monkeypatch
    ):
        monkeypatch.setattr(gc_handles, "MAX_ENTRIES", 2)
        for i in range(3):
            gc_handles.record(f"s-run-{i}", status="running")
            gc_handles._table[f"s-run-{i}"]["updated_at"] = float(i)
        gc_handles.record("s-run-final", status="running")
        assert len(gc_handles._table) <= 2
        assert "s-run-final" in gc_handles._table
        assert "s-run-0" not in gc_handles._table

    def test_pruned_handle_deletes_its_event_log(self, clean_handles):
        """A pruned entry takes its JSONL log with it — without this every
        pruned handle leaks one file under state/ghost_cursor/logs/."""
        gc_handles.record("Old finished task", status="completed")
        gc_eventlog.append("Old finished task", {"kind": "content", "delta": "x"})
        log = gc_eventlog.log_path("Old finished task")
        assert log is not None and log.is_file()

        gc_handles._table["Old finished task"]["updated_at"] = (
            time.time() - gc_handles.PRUNE_TERMINAL_AFTER_S - 60
        )
        gc_handles.record("Fresh task", status="running")  # write → prune
        assert gc_handles.get("Old finished task") is None
        assert not log.exists()

    def test_cap_eviction_deletes_the_evicted_log(
        self, clean_handles, monkeypatch
    ):
        monkeypatch.setattr(gc_handles, "MAX_ENTRIES", 2)
        now = time.time()
        gc_handles.record("Oldest done", status="completed")
        gc_handles._table["Oldest done"]["updated_at"] = now - 300
        gc_eventlog.append("Oldest done", {"kind": "content", "delta": "x"})
        log = gc_eventlog.log_path("Oldest done")
        assert log is not None and log.is_file()

        gc_handles.record("Newer run", status="running")
        gc_handles.record("Newest run", status="running")  # over cap → evict
        assert gc_handles.get("Oldest done") is None
        assert not log.exists()

    def test_surviving_handles_keep_their_logs(self, clean_handles):
        gc_handles.record("Live task", status="running")
        gc_eventlog.append("Live task", {"kind": "content", "delta": "x"})
        log = gc_eventlog.log_path("Live task")
        gc_handles.record("Another task", status="running")  # write → prune
        assert log.is_file()

    def test_prune_survives_a_log_delete_failure(
        self, clean_handles, monkeypatch
    ):
        """handles.py never raises: a failing log delete cannot break the
        write (the entry is still pruned, the caller sees nothing)."""
        gc_handles.record("Doomed task", status="completed")
        gc_handles._table["Doomed task"]["updated_at"] = (
            time.time() - gc_handles.PRUNE_TERMINAL_AFTER_S - 60
        )

        def boom(_name):
            raise OSError("disk says no")

        monkeypatch.setattr(gc_eventlog, "log_path", boom)
        gc_handles.record("Fresh task", status="running")
        assert gc_handles.get("Doomed task") is None
        assert gc_handles.get("Fresh task") is not None


# ---------------------------------------------------------------------------
# cursor_create_session — title handles, duplicate/over-long rejection,
# deterministic repo-basename fallback + UUID alias resolution
# ---------------------------------------------------------------------------

class TestCursorCreateSession:
    def test_title_becomes_the_handle_and_dispatches_nothing(
        self, clean_state, monkeypatch, tmp_path
    ):
        boom = lambda *a, **k: pytest.fail("create must not start a run")
        monkeypatch.setattr(gc_cloud, "run_cloud", boom)

        ack = cursor_create_session(
            repo=str(tmp_path), title="Fix payment webhook retries"
        )
        assert _created_name(ack) == "Fix payment webhook retries"
        # The exact ack format from the spec: 2 headers + instruction.
        assert ack == (
            "session: Fix payment webhook retries\n"
            f"repo: {_resolved(tmp_path)} · model: "
            f"{gc._resolve_model(None) or 'default'} · runtime: local\n"
            "created. send work with cursor_send_message."
        )
        # LAZY: nothing running, but the handle exists as 'created' —
        # keyed by the title verbatim.
        assert gc_jobs.registry.list_jobs() == []
        entry = gc_handles.get("Fix payment webhook retries")
        assert entry["status"] == "created"
        assert entry["repo"] == _resolved(tmp_path)
        assert gc_handles.resolve("Fix payment webhook retries") == (
            "Fix payment webhook retries"
        )

    def test_title_whitespace_is_trimmed_and_collapsed(
        self, clean_state, tmp_path
    ):
        ack = cursor_create_session(
            repo=str(tmp_path), title="  Fix   payment\twebhook  retries  "
        )
        assert _created_name(ack) == "Fix payment webhook retries"
        assert gc_handles.get("Fix payment webhook retries") is not None

    def test_duplicate_title_fails_with_status_and_age(
        self, clean_state, tmp_path
    ):
        gc_handles.record("Fix payment webhook retries", repo="/elsewhere",
                          status="completed")
        gc_handles._table["Fix payment webhook retries"]["updated_at"] = (
            time.time() - 2 * 86400
        )

        out = cursor_create_session(
            repo=str(tmp_path), title="Fix payment webhook retries"
        )
        assert "cannot create session" in out
        assert "'Fix payment webhook retries'" in out
        assert "already in use" in out
        assert "completed, 2d ago" in out       # the existing entry's state
        assert "more specific" in out           # asks for a better name
        # No entry was recorded or clobbered by the failed create.
        entry = gc_handles.get("Fix payment webhook retries")
        assert entry["status"] == "completed"
        assert entry["repo"] == "/elsewhere"

    def test_duplicate_title_error_shows_running_state(
        self, clean_state, tmp_path
    ):
        gc_handles.record("Ship dark mode", repo="/r", status="running")
        out = cursor_create_session(repo=str(tmp_path), title="Ship dark mode")
        assert "cannot create session" in out
        assert "running" in out

    def test_duplicate_check_never_auto_suffixes(self, clean_state, tmp_path):
        gc_handles.record("Ship dark mode", repo="/r", status="running")
        cursor_create_session(repo=str(tmp_path), title="Ship dark mode")
        assert gc_handles.resolve("Ship dark mode 2") is None

    def test_title_matching_an_agent_id_alias_is_also_taken(
        self, clean_state, tmp_path
    ):
        gc_handles.record("Older session", status="completed",
                          cursor_session_id="bc-alias-1")
        out = cursor_create_session(repo=str(tmp_path), title="bc-alias-1")
        assert "cannot create session" in out
        assert "already in use" in out

    def test_over_long_title_is_rejected_not_truncated(
        self, clean_state, tmp_path
    ):
        long_title = "Fix " + "very " * 30 + "long title"  # > 80 chars
        out = cursor_create_session(repo=str(tmp_path), title=long_title)
        assert "cannot create session" in out
        assert str(gc.MAX_TITLE_CHARS) in out   # names the cap
        assert "shorten" in out
        # Nothing recorded — neither the full nor a truncated key.
        assert gc_handles.known_handles(scope="all") == []

    def test_max_length_title_is_accepted(self, clean_state, tmp_path):
        title = "x" * gc.MAX_TITLE_CHARS
        ack = cursor_create_session(repo=str(tmp_path), title=title)
        assert _created_name(ack) == title

    def test_omitted_title_falls_back_to_repo_basename(
        self, clean_state, tmp_path
    ):
        ack = cursor_create_session(repo=str(tmp_path))
        expected = f"{Path(_resolved(tmp_path)).name} session"
        assert _created_name(ack) == expected
        assert gc_handles.get(expected)["status"] == "created"

    def test_fallback_suffixes_past_collisions_and_never_fails(
        self, clean_state, tmp_path
    ):
        base = f"{Path(_resolved(tmp_path)).name} session"
        names = [
            _created_name(cursor_create_session(repo=str(tmp_path)))
            for _ in range(3)
        ]
        assert names == [base, f"{base} 2", f"{base} 3"]

    def test_blank_title_takes_the_fallback_path(self, clean_state, tmp_path):
        ack = cursor_create_session(repo=str(tmp_path), title="   ")
        assert _created_name(ack) == f"{Path(_resolved(tmp_path)).name} session"

    def test_explicit_model_is_recorded_on_the_handle(
        self, clean_state, tmp_path
    ):
        ack = cursor_create_session(repo=str(tmp_path), model="composer-x")
        name = _created_name(ack)
        assert "model: composer-x" in ack
        assert gc_handles.get(name)["model"] == "composer-x"

    def test_unresolvable_repo_is_actionable_prose(self, clean_state, monkeypatch):
        monkeypatch.setattr(gc, "_default_repo", lambda: None)
        out = cursor_create_session()
        assert "no workspace repo resolvable" in out
        assert gc.REPO_ENV_VAR in out

    def test_missing_repo_dir_is_actionable_prose(self, clean_state):
        out = cursor_create_session(repo="/definitely/not/a/dir")
        assert "cannot create session" in out
        assert "not an existing directory" in out

    def test_handler_maps_args(self, clean_state, tmp_path):
        ack = _handle_cursor_create_session({
            "repo": str(tmp_path), "model": "m-x",
            "title": "Wire the handler args",
        })
        assert ack.startswith("session: Wire the handler args")
        assert "model: m-x" in ack

    def test_uuid_alias_resolves_everywhere_a_name_does(
        self, clean_state, monkeypatch, tmp_path
    ):
        """After the first send binds the cursor UUID, name and UUID are
        interchangeable across status/stop/events/send."""
        monkeypatch.setattr(
            gc_cloud, "run_cloud",
            _gated_replay_factory(_preset_event(), sid="11111111-2222-3333"),
        )
        name = _created_name(cursor_create_session(
            repo=str(tmp_path), title="Alias resolution check"
        ))
        assert name == "Alias resolution check"
        cursor_send_message(name, "do the thing")
        assert gc_jobs.registry.get_by_name(name).done_event.wait(10)

        assert gc_handles.resolve("11111111-2222-3333") == name
        by_name, by_uuid = cursor_status(name), cursor_status("11111111-2222-3333")
        # Identical view, however addressed (allow the elapsed clock to tick).
        assert by_uuid.splitlines()[0] == by_name.splitlines()[0]
        assert f"session: {name}" in by_uuid
        assert f"session: {name}" in cursor_stop("11111111-2222-3333")
        assert "events" in cursor_events("11111111-2222-3333")

    def test_title_is_the_agent_name_on_the_rest_create(
        self, clean_state, monkeypatch, tmp_path
    ):
        """One string end to end: the title IS the `name` POST /v1/agents
        receives, so the session shows up on cursor.com under it."""
        client = _FakeRestClient()
        _install_fake_rest(monkeypatch, client)

        name = _created_name(cursor_create_session(
            repo=str(tmp_path), title="Session naming rework"
        ))
        cursor_send_message(name, "do the thing")
        assert gc_jobs.registry.get_by_name(name).done_event.wait(10)
        assert client.create_calls[0]["name"] == "Session naming rework"


# ---------------------------------------------------------------------------
# cursor_create_session model validation (issue #12) — obviously-invalid
# model strings fail at CREATE (shape check only, create stays lazy); a
# well-formed id the catalog doesn't know still fails on the first send,
# but the failure names the model and says it was set at create.
# ---------------------------------------------------------------------------

class TestCreateModelValidation:
    @pytest.mark.parametrize("bad", [
        "claude-fable-5[thinking",   # unparseable bracket suffix
        "m[thinking=]",              # malformed bracket parameter
        "m-thinking-banana",         # unknown '-thinking-…' level
        "totally fake model!!!",     # characters no catalog id uses
    ])
    def test_malformed_model_is_rejected_at_create(
        self, clean_state, monkeypatch, tmp_path, bad
    ):
        # The rejection must stay lazy: no run, no API contact.
        boom = lambda *a, **k: pytest.fail("create must not touch the API")
        monkeypatch.setattr(gc_cloud, "run_cloud", boom)
        monkeypatch.setattr(gc_cloud, "make_client", boom)

        out = cursor_create_session(repo=str(tmp_path), model=bad)
        assert "cannot create session" in out
        assert bad in out  # the failure names the offending string
        assert "falling back" not in out  # rejected, never substituted
        # No handle was minted for the failed create.
        assert gc_handles.known_handles(scope="all") == []

    def test_well_formed_unknown_id_still_creates_lazily(
        self, clean_state, monkeypatch, tmp_path
    ):
        """Only the sdk knows the catalog — a shape-valid id passes create
        (the documented deferred-validation contract) and the first send
        owns the failure."""
        boom = lambda *a, **k: pytest.fail("create must not start a run")
        monkeypatch.setattr(gc_cloud, "run_cloud", boom)

        ack = cursor_create_session(
            repo=str(tmp_path), model="totally-fake-model-9000"
        )
        name = _created_name(ack)
        assert gc_handles.get(name)["model"] == "totally-fake-model-9000"

    def test_legacy_translatable_forms_are_still_accepted_at_create(
        self, clean_state, tmp_path
    ):
        for legacy in (
            "claude-fable-5-thinking-high",
            "claude-fable-5[thinking=true,context=300k,effort=high]",
        ):
            ack = cursor_create_session(repo=str(tmp_path), model=legacy)
            assert ack.startswith("session: "), ack

    def test_send_failure_names_the_model_set_at_create(
        self, clean_state, monkeypatch, tmp_path
    ):
        """The deferred (catalog) failure at first send must attribute the
        error to the model param chosen at create, not read like a generic
        API failure on the send."""
        client = _FakeRestClient(create_error=gc_rest.RestApiError(
            'cursor api POST /v1/agents -> 400 invalid_model: model '
            '"totally-fake-model-9000" not found',
            status_code=400, code="invalid_model",
        ))
        _install_fake_rest(monkeypatch, client)

        name = _created_name(cursor_create_session(
            repo=str(tmp_path), model="totally-fake-model-9000"
        ))
        out = cursor_send_message(name, "hi")
        assert "agent create failed" in out
        assert "'totally-fake-model-9000'" in out
        assert CREATE_TOOL_NAME in out  # "...was set at cursor_create_session"

    def test_send_failure_without_create_model_is_not_attributed(
        self, clean_state, monkeypatch, tmp_path
    ):
        """No explicit model at create → the same failure stays generic
        (the default/configured-model path is untouched)."""
        monkeypatch.setattr(gc, "_configured_model", lambda: None)
        client = _FakeRestClient(create_error=gc_rest.RestApiError(
            "cursor api POST /v1/agents -> 500: boom", status_code=500,
        ))
        _install_fake_rest(monkeypatch, client)

        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        out = cursor_send_message(name, "hi")
        assert "agent create failed" in out
        assert "was set at" not in out


# ---------------------------------------------------------------------------
# cursor_events — tail defaults, negative offsets, kind filter, clips, cap
# ---------------------------------------------------------------------------

def _seed_session_log(name, envelopes, repo="/r"):
    """A persisted handle + synthetic JSONL history for paging tests."""
    gc_handles.record(name, repo=repo, status="completed")
    for env in envelopes:
        gc_eventlog.append(name, env)


class TestCursorEventsPaging:
    def test_bare_call_defaults_to_the_last_10_events(self, clean_state):
        _seed_session_log("tail-y-log", [
            {"kind": "content", "delta": f"e{i}"} for i in range(25)
        ])
        out = cursor_events("tail-y-log")
        assert "events 15–24 of 25" in out
        assert '"e15"' in out and '"e24"' in out
        assert '"e14"' not in out

    def test_negative_offset_pages_backwards_python_style(self, clean_state):
        _seed_session_log("neg-off-log", [
            {"kind": "content", "delta": f"e{i}"} for i in range(25)
        ])
        # offset=-11 = window ENDING at the 11th-from-last event.
        out = cursor_events("neg-off-log", offset=-11, limit=10)
        assert "events 5–14 of 25" in out
        # More negative than the history: clamps to the start, no crash.
        assert "events 0–1 of 25" in cursor_events(
            "neg-off-log", offset=-24, limit=2
        )

    def test_positive_offset_pages_forward_from_that_seq(self, clean_state):
        _seed_session_log("fwd-log", [
            {"kind": "content", "delta": f"e{i}"} for i in range(25)
        ])
        out = cursor_events("fwd-log", offset=3, limit=4)
        assert "events 3–6 of 25" in out

    def test_kind_filter_selects_only_that_kind(self, clean_state):
        envs = []
        for i in range(6):
            envs.append({"kind": "content", "delta": f"prose{i}"})
            envs.append({"kind": "lifecycle", "event": "reasoning",
                         "text": f"thinking hard {i}"})
        _seed_session_log("kind-log", envs)

        out = cursor_events("kind-log", kind="reasoning")
        assert "6 matching (kind=reasoning) of 12" in out
        assert "thinking hard 5" in out
        assert "prose" not in out

        # The window applies to the FILTERED sequence.
        two = cursor_events("kind-log", kind="reasoning", offset=-1, limit=2)
        assert "thinking hard 4" in two and "thinking hard 5" in two
        assert "thinking hard 3" not in two

    def test_limit_clamps_at_500(self, clean_state):
        _seed_session_log("clamp-log", [
            {"kind": "content", "delta": f"e{i}"} for i in range(510)
        ])
        out = cursor_events("clamp-log", offset=0, limit=99999)
        assert "events 0–499 of 510" in out  # 500, not 510

    def test_reasoning_text_clips_inline_at_2kb_with_log_pointer(
        self, clean_state
    ):
        wall = "reason " * 2000  # ~14KB of thinking
        _seed_session_log("clip-log", [
            {"kind": "lifecycle", "event": "reasoning", "text": wall},
        ])
        out = cursor_events("clip-log", kind="reasoning")
        assert len(out) < 4_000
        assert "full event in the JSONL log" in out
        # The full text stayed intact on disk.
        assert _log_lines("clip-log")[0]["text"] == wall

    def test_total_response_caps_at_20kb_with_truncation_note(
        self, clean_state
    ):
        # 30 events x ~1.9KB inline (each under the 2KB per-event clip) —
        # only the response-level cap can bound this page.
        _seed_session_log("cap-log", [
            {"kind": "lifecycle", "event": "reasoning", "text": f"e{i} " + "x" * 1900}
            for i in range(30)
        ])
        out = cursor_events("cap-log", offset=0, limit=30)
        assert len(out) <= gc_render.EVENTS_RESPONSE_CAP + 200
        assert gc_render.EVENTS_TRUNCATION_NOTE in out
        assert "e0 " in out          # the page kept its head...
        assert "e29 " not in out     # ...and dropped whole trailing events

    def test_empty_window_names_the_filter(self, clean_state):
        _seed_session_log("empty-log", [{"kind": "content", "delta": "hi"}])
        out = cursor_events("empty-log", kind="file_diff")
        assert "no events of kind 'file_diff'" in out

    def test_unknown_session_is_actionable_prose(self, clean_state):
        assert "no session named 's-nope'" in cursor_events("s-nope")

    def test_handler_maps_args(self, clean_state):
        _seed_session_log("handler-log", [
            {"kind": "content", "delta": f"e{i}"} for i in range(5)
        ])
        out = _handle_cursor_events({
            "session": "handler-log", "offset": 0, "limit": 2,
        })
        assert "events 0–1 of 5" in out


# ---------------------------------------------------------------------------
# events since prompt — the last_prompt_seq marker + the labeled line
# ---------------------------------------------------------------------------

class TestEventsSincePrompt:
    """cursor_send_message stamps the log position onto the handle at
    dispatch; status / events / completion output render
    "events since prompt: N" with the exact cursor_events page for them."""

    def test_send_records_the_marker_and_persists_it(
        self, clean_state, monkeypatch, tmp_path
    ):
        release2 = threading.Event()
        seq = _SdkSequence(
            _gated_replay_factory(_preset_event(), sid="s-mark"),
            _gated_replay_factory(release2, sid="s-mark", early_edit=False),
        )
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)

        _start_run("task A", repo=str(tmp_path))
        job = _job_for("s-mark")
        name = job.session_name
        assert job.done_event.wait(10)
        # First send on a fresh session: the log was empty at dispatch.
        assert gc_handles.get(name)["last_prompt_seq"] == 0
        total_after_first = gc_eventlog.stats(name)["total_events"]
        assert total_after_first > 0
        _drain_completion_queue()

        ack = cursor_send_message(name, "follow up")
        try:
            _assert_running_ack(ack)
            # The follow-up moved the marker to the log position at dispatch
            # (events the new run appends count as since-prompt)...
            assert gc_handles.get(name)["last_prompt_seq"] == total_after_first
            # ...and the marker is persisted, not process-local.
            on_disk = json.loads(gc_handles._state_file().read_text("utf-8"))
            assert on_disk[name]["last_prompt_seq"] == total_after_first
        finally:
            release2.set()
        assert gc_jobs.registry.get_by_name(name).done_event.wait(10)

    def test_interrupt_reprompt_updates_the_marker(
        self, clean_state, monkeypatch, tmp_path
    ):
        release1 = threading.Event()  # never set: run 1 ends only via cancel
        release2 = threading.Event()
        seq = _SdkSequence(
            _gated_replay_factory(release1, sid="s-remark"),
            _gated_replay_factory(release2, sid="s-remark", early_edit=False),
        )
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)

        _assert_running_ack(_start_run("task A", repo=str(tmp_path)))
        first_job = _job_for("s-remark")
        name = first_job.session_name
        assert gc_handles.get(name)["last_prompt_seq"] == 0
        assert _wait_until(lambda: first_job.files)  # run-1 events landed

        ack = cursor_send_message(name, "steer it")
        try:
            _assert_running_ack(ack)
            marker = gc_handles.get(name)["last_prompt_seq"]
            # The interrupted run's events sit BEHIND the fresh marker...
            assert marker > 0
            # ...which is a real log position, never past the log's end.
            assert marker <= gc_eventlog.stats(name)["total_events"]
        finally:
            release2.set()
        assert gc_jobs.registry.get_by_name(name).done_event.wait(10)

    def test_rejected_send_leaves_the_marker_alone(
        self, clean_state, monkeypatch, tmp_path
    ):
        release = threading.Event()
        monkeypatch.setattr(
            gc_cloud, "run_cloud", _gated_replay_factory(release, sid="s-busy2")
        )
        _assert_running_ack(_start_run("task A", repo=str(tmp_path)))
        job = _job_for("s-busy2")
        try:
            other = _created_name(cursor_create_session(repo=str(tmp_path)))
            out = cursor_send_message(other, "second task")
            assert "cannot start" in out
            # No prompt went out for the rejected send — no marker written.
            assert "last_prompt_seq" not in gc_handles.get(other)
        finally:
            release.set()
        assert job.done_event.wait(10)

    def test_status_shows_count_and_pager_pointer(self, clean_state):
        _seed_session_log("since-st", [
            {"kind": "content", "delta": f"e{i}"} for i in range(12)
        ])
        gc_handles.record("since-st", last_prompt_seq=4)
        out = cursor_status("since-st")
        assert (
            "events since prompt: 8 — "
            "cursor_events('since-st', offset=4, limit=8)"
        ) in out

    def test_events_page_carries_the_since_prompt_line(self, clean_state):
        _seed_session_log("since-ev", [
            {"kind": "content", "delta": f"e{i}"} for i in range(10)
        ])
        gc_handles.record("since-ev", last_prompt_seq=7)
        out = cursor_events("since-ev")
        assert (
            "events since prompt: 3 — "
            "cursor_events('since-ev', offset=7, limit=3)"
        ) in out

    def test_delivered_completion_reports_events_since_prompt(
        self, clean_state, monkeypatch, tmp_path
    ):
        release = threading.Event()
        monkeypatch.setattr(
            gc_cloud, "run_cloud", _gated_replay_factory(release, sid="s-cmark")
        )
        _assert_running_ack(_start_run("t", repo=str(tmp_path)))
        job = _job_for("s-cmark")
        release.set()
        assert job.done_event.wait(10)

        events = _drain_completion_queue()
        assert len(events) == 1
        # First prompt's marker is 0, so every event of the run counts.
        total = gc_eventlog.stats(job.session_name)["total_events"]
        assert total > 0
        assert (
            f"events since prompt: {total} — "
            f"cursor_events('{job.session_name}', offset=0, limit={total})"
        ) in events[0]["summary"]

    def test_zero_since_prompt_renders_the_bare_count(self, clean_state):
        _seed_session_log("since-zero", [
            {"kind": "content", "delta": f"e{i}"} for i in range(5)
        ])
        gc_handles.record("since-zero", last_prompt_seq=5)
        out = cursor_status("since-zero")
        assert "events since prompt: 0" in out
        assert "events since prompt: 0 —" not in out  # nothing to page

    def test_legacy_handle_without_the_field_counts_the_whole_log(
        self, clean_state
    ):
        # _seed_session_log records a handle with NO last_prompt_seq — the
        # exact shape of entries persisted before the field existed.
        _seed_session_log("since-legacy", [
            {"kind": "content", "delta": f"e{i}"} for i in range(6)
        ])
        assert "last_prompt_seq" not in gc_handles.get("since-legacy")
        status = cursor_status("since-legacy")
        assert (
            "events since prompt: 6 — "
            "cursor_events('since-legacy', offset=0, limit=6)"
        ) in status
        assert "events since prompt: 6" in cursor_events("since-legacy")

    def test_corrupt_marker_sanitizes_to_zero_instead_of_crashing(
        self, clean_state
    ):
        _seed_session_log("since-corrupt", [{"kind": "content", "delta": "hi"}])
        gc_handles.record("since-corrupt", last_prompt_seq="not-a-number")
        out = cursor_status("since-corrupt")
        assert out.startswith("status: completed")
        assert "events since prompt: 1" in out

    def test_pager_limit_clamps_at_max_page_limit(self, clean_state):
        line = gc_render.since_prompt_line("big", 1200, 100)
        assert line == (
            "events since prompt: 1100 — "
            "cursor_events('big', offset=100, limit=500)"
        )


# ---------------------------------------------------------------------------
# cursor_list — TSV shape + hermes-session scoping
# ---------------------------------------------------------------------------

class TestCursorList:
    def test_empty_table_is_helpful_prose(self, clean_state, monkeypatch):
        monkeypatch.setattr(gc, "_resolve_session_key", lambda: "gw:me")
        assert "no cursor sessions in this hermes session" in cursor_list()
        assert "no cursor sessions exist yet" in cursor_list(scope="all")

    def test_tsv_has_header_row_and_tab_separated_columns(
        self, clean_state, monkeypatch
    ):
        monkeypatch.setattr(gc, "_resolve_session_key", lambda: "gw:me")
        gc_handles.record(
            "brave-jade-owl", repo="/r/a", status="completed",
            session_key="gw:me", files_changed_count=3, duration_s=61.2,
            runtime="local",
        )
        out = cursor_list()
        lines = out.splitlines()
        header = [c.strip() for c in lines[0].split("\t")]
        assert header == ["session", "repo", "runtime", "status", "elapsed",
                          "files", "last_activity"]
        row = [c.strip() for c in lines[1].split("\t")]
        assert row == ["brave-jade-owl", "/r/a", "local", "completed",
                       "61s", "3", "—"]

    def test_default_scope_is_the_current_hermes_session(
        self, clean_state, monkeypatch
    ):
        monkeypatch.setattr(gc, "_resolve_session_key", lambda: "gw:me")
        gc_handles.record("mine-fox", repo="/a", status="completed",
                          session_key="gw:me")
        gc_handles.record("theirs-owl", repo="/b", status="completed",
                          session_key="gw:other")
        mine = cursor_list()
        assert "mine-fox" in mine and "theirs-owl" not in mine
        everyone = cursor_list(scope="all")
        assert "mine-fox" in everyone and "theirs-owl" in everyone

    def test_running_session_renders_live_state(
        self, clean_state, monkeypatch, tmp_path
    ):
        release = threading.Event()
        monkeypatch.setattr(gc_cloud, "run_cloud",
                            _gated_replay_factory(release, sid="s-live-list"))
        _start_run("t", repo=str(tmp_path))
        job = _job_for("s-live-list")
        try:
            assert _wait_until(lambda: job.files)
            row = [
                l for l in cursor_list(scope="all").splitlines()
                if job.session_name in l
            ][0]
            cells = [c.strip() for c in row.split("\t")]
            assert cells[3] == "running"
            assert cells[5] == "1"  # live files count, not the stale record
        finally:
            release.set()
        assert job.done_event.wait(10)

    def test_handler_maps_scope(self, clean_state, monkeypatch):
        monkeypatch.setattr(gc, "_resolve_session_key", lambda: "gw:me")
        gc_handles.record("theirs-owl", repo="/b", status="completed",
                          session_key="gw:other")
        assert "theirs-owl" in _handle_cursor_list({"scope": "all"})
        assert "theirs-owl" not in _handle_cursor_list({})


# ---------------------------------------------------------------------------
# cloud_runner.run_cloud — faked REST client (no network, no workers)
# ---------------------------------------------------------------------------

def _sse(event, data, id=None):
    """A decoded SSE event as the fake stream yields it."""
    return gc_rest.SseEvent(
        event=event, data=data, id=id,
        raw_data=json.dumps(data) if isinstance(data, dict) else str(data or ""),
    )


def _happy_stream(run_id="run-1"):
    """status + assistant + one tool round-trip + result + done."""
    return [
        _sse("status", {"status": "RUNNING"}),
        _sse("assistant", {"text": "working on it"}, id="1"),
        _sse("tool_call", {"callId": "t1", "name": "shell",
                           "status": "running",
                           "args": {"command": "ls -la"}}, id="2"),
        _sse("tool_call", {"callId": "t1", "name": "shell",
                           "status": "completed",
                           "args": {"command": "ls -la"},
                           "result": {"exitCode": 0, "stdout": "calc.py"}},
             id="3"),
        _sse("assistant", {"text": "all done"}, id="4"),
        _sse("result", {"status": "FINISHED", "text": "all done"}, id="5"),
        _sse("done", {}),
    ]


class _FakeRestClient:
    """A scriptable stand-in for rest_client.CursorRestClient.

    ``streams`` is a list of per-attach scripts; each script's items are
    SseEvents, Exception instances (raised mid-stream — a drop), or
    callables (invoked between events, e.g. to block on a gate; a callable
    returning an SseEvent yields it). Successive ``stream_run_events``
    calls consume successive scripts; the last script repeats when the
    reconnects outnumber the scripts. ``statuses`` is the sequence of
    GET runs/{id} statuses (last one repeats) — the settle authority.
    """

    def __init__(
        self,
        streams=None,
        statuses=("FINISHED",),
        create_error=None,
        followup_error=None,
        models=None,
        models_error=None,
        agent_id="bc-fake-1",
        run_id="run-1",
    ):
        self.streams = [list(s) for s in (streams or [_happy_stream()])]
        self.statuses = list(statuses)
        self.create_error = create_error
        self.followup_error = followup_error
        self.models = models
        self.models_error = models_error
        self.agent_id = agent_id
        self.run_id = run_id
        self.create_calls = []
        self.followup_calls = []
        self.cancel_calls = []
        self.stream_calls = []
        self.get_run_calls = 0
        self.cancelled = threading.Event()

    # -- agents ------------------------------------------------------------

    def create_agent(self, prompt_text, *, model_id=None, model_params=None,
                     env=None, repos=None, work_on_current_branch=None,
                     name=None):
        self.create_calls.append({
            "prompt": prompt_text, "model_id": model_id,
            "model_params": model_params, "env": env, "repos": repos,
            "work_on_current_branch": work_on_current_branch, "name": name,
        })
        if self.create_error is not None:
            err, self.create_error = self.create_error, None
            raise err
        return {
            "agent": {"id": self.agent_id,
                      "url": f"https://cursor.com/agents/{self.agent_id}"},
            "run": {"id": self.run_id},
        }

    def send_followup(self, agent_id, prompt_text):
        self.followup_calls.append({"agent_id": agent_id,
                                    "prompt": prompt_text})
        if self.followup_error is not None:
            err, self.followup_error = self.followup_error, None
            raise err
        return {"run": {"id": self.run_id}}

    # -- runs --------------------------------------------------------------

    def list_runs(self, agent_id, limit=20):
        return {"runs": [{"id": self.run_id}]}

    def get_run(self, agent_id, run_id):
        self.get_run_calls += 1
        idx = min(self.get_run_calls - 1, len(self.statuses) - 1)
        return {"id": run_id, "status": self.statuses[idx]}

    def cancel_run(self, agent_id, run_id):
        self.cancel_calls.append((agent_id, run_id))
        self.cancelled.set()
        return {}

    # -- models --------------------------------------------------------------

    def list_models(self):
        if self.models_error is not None:
            raise self.models_error
        if self.models is None:
            raise gc_rest.RestNetworkError("no model catalog scripted")
        return [{"id": m} for m in self.models]

    # -- SSE -----------------------------------------------------------------

    def stream_run_events(self, agent_id, run_id, last_event_id=None):
        self.stream_calls.append(last_event_id)
        idx = min(len(self.stream_calls) - 1, len(self.streams) - 1)
        for item in self.streams[idx]:
            if self.cancelled.is_set():
                return
            if isinstance(item, Exception):
                raise item
            if callable(item):
                item = item()
            if item is None:
                continue
            yield item


def _worker_record(name="test-worker", repo="/w", verified=True):
    return gc_workers.WorkerRecord(
        name=name, repo_path=repo, pid=4242, log_path="/dev/null",
        started_at=time.time(), verified=verified,
    )


def _install_fake_rest(monkeypatch, client, worker=None):
    """Route run_cloud at a fake REST client: no network, no git, no
    worker spawn. The ensure fake honors the atomic lease contract when
    the worker record actually exists on disk (lease-lifecycle tests)."""
    monkeypatch.setenv("CURSOR_API_KEY", "key-test")
    monkeypatch.setattr(gc_cloud, "make_client", lambda: client)
    monkeypatch.setattr(
        gc_cloud, "derive_repo_ref",
        lambda path: ("https://github.com/example/repo", "main"),
    )

    def fake_ensure(repo, lease_id=None, lease_session=""):
        record = worker or _worker_record(repo=str(repo))
        if lease_id and gc_workers._read_record(record.name) is not None:
            gc_workers.acquire_lease(record.name, lease_id, session=lease_session)
            record = gc_workers._read_record(record.name)
        return record

    monkeypatch.setattr(gc_workers, "ensure_worker", fake_ensure)
    monkeypatch.setattr(gc_workers, "mark_verified", lambda name: None)
    # The per-process model-catalog cache must not leak across tests.
    monkeypatch.setattr(gc_cloud, "_catalog_ids", None)
    monkeypatch.setattr(gc_cloud, "_catalog_all", None)
    monkeypatch.setattr(gc_cloud, "_catalog_at", 0.0)


def _run_cloud_events(tmp_path, **kw):
    kw.setdefault("inactivity_timeout_s", 30.0)
    kw.setdefault("cancel_check", lambda: False)
    return list(gc_cloud.run_cloud("do it", str(tmp_path), **kw))


class TestCloudRunner:
    def test_happy_path_yields_session_messages_result(
        self, tmp_path, monkeypatch
    ):
        client = _FakeRestClient()
        _install_fake_rest(monkeypatch, client)
        events = _run_cloud_events(tmp_path)
        keys = [k for k, _ in events]
        assert keys[0] == "cloud.session"
        session = events[0][1]
        assert session["agentId"] == "bc-fake-1"
        assert session["run_id"] == "run-1"
        assert session["resumed"] is False
        assert session["runtime"] == "local"
        assert session["worker"] == "test-worker"
        assert session["agents_ui_url"] == "https://cursor.com/agents/bc-fake-1"
        messages = [o for k, o in events if k == "cloud.message"]
        assert [m["type"] for m in messages] == [
            "assistant", "tool_call", "tool_call", "assistant",
        ]
        # Simplified SSE payloads land in the SDKMessage dict shapes the
        # normalizer parses (snake_case call_id, verbatim args/result).
        assert messages[0]["message"] == "working on it"
        assert messages[1]["call_id"] == "t1"
        assert messages[2]["result"]["stdout"] == "calc.py"
        assert keys[-1] == "cloud.result"
        assert events[-1][1]["status"] == "finished"
        # The create carried the task + machine env + current-branch pin.
        call = client.create_calls[0]
        assert call["prompt"] == "do it"
        assert call["env"] == {"type": "machine", "name": "test-worker"}
        assert call["work_on_current_branch"] is True
        assert call["repos"] == [{
            "url": "https://github.com/example/repo", "startingRef": "main",
        }]
        # Settle authority: the final GET confirmed FINISHED.
        assert client.get_run_calls >= 1

    def test_interaction_update_duplicates_are_ignored(
        self, tmp_path, monkeypatch
    ):
        client = _FakeRestClient(streams=[[
            _sse("status", {"status": "RUNNING"}),
            _sse("assistant", {"text": "hi"}, id="1"),
            _sse("interaction_update", {"interaction": {"richText": "hi"}},
                 id="1"),
            _sse("result", {"status": "FINISHED"}, id="2"),
            _sse("done", {}),
        ]])
        _install_fake_rest(monkeypatch, client)
        events = _run_cloud_events(tmp_path)
        messages = [o for k, o in events if k == "cloud.message"]
        assert [m["type"] for m in messages] == ["assistant"]

    def test_unknown_sse_event_passes_through(self, tmp_path, monkeypatch):
        client = _FakeRestClient(streams=[[
            _sse("mystery", {"x": 1}, id="1"),
            _sse("result", {"status": "FINISHED"}, id="2"),
            _sse("done", {}),
        ]])
        _install_fake_rest(monkeypatch, client)
        events = _run_cloud_events(tmp_path)
        messages = [o for k, o in events if k == "cloud.message"]
        assert messages == [{"type": "sse.mystery", "x": 1}]

    def test_missing_api_key_raises_actionable_error(
        self, tmp_path, monkeypatch
    ):
        _install_fake_rest(monkeypatch, _FakeRestClient())
        monkeypatch.delenv("CURSOR_API_KEY", raising=False)
        with pytest.raises(gc_cloud.CloudRunnerError) as err:
            _run_cloud_events(tmp_path)
        assert "CURSOR_API_KEY" in str(err.value)

    def test_empty_task_and_bad_repo_preflight(self, tmp_path, monkeypatch):
        _install_fake_rest(monkeypatch, _FakeRestClient())
        with pytest.raises(gc_runner.HarnessError):
            list(gc_cloud.run_cloud("   ", str(tmp_path)))
        with pytest.raises(gc_runner.HarnessError):
            list(gc_cloud.run_cloud("t", str(tmp_path / "nope")))

    def test_unknown_runtime_raises(self, tmp_path, monkeypatch):
        _install_fake_rest(monkeypatch, _FakeRestClient())
        with pytest.raises(gc_cloud.CloudRunnerError) as err:
            list(gc_cloud.run_cloud("t", str(tmp_path), runtime="warp"))
        assert "unknown runtime" in str(err.value)

    def test_non_github_origin_raises_actionable_error(
        self, tmp_path, monkeypatch
    ):
        """derive_repo_ref's preflight failure surfaces as CloudRunnerError
        BEFORE any run (real derive_repo_ref, no git repo at tmp_path)."""
        monkeypatch.setenv("CURSOR_API_KEY", "key-test")
        with pytest.raises(gc_cloud.CloudRunnerError) as err:
            list(gc_cloud.run_cloud("t", str(tmp_path)))
        assert "origin" in str(err.value)

    def test_create_failure_raises_actionable_error(
        self, tmp_path, monkeypatch
    ):
        client = _FakeRestClient(create_error=gc_rest.RestApiError(
            "cursor api POST /v1/agents -> 400 invalid_model: bad model",
            status_code=400, code="invalid_model",
        ))
        _install_fake_rest(monkeypatch, client)
        with pytest.raises(gc_cloud.CloudRunnerError) as err:
            _run_cloud_events(tmp_path, model="bogus-model")
        assert "agent create failed" in str(err.value)
        assert "bogus-model" in str(err.value)
        assert "CURSOR_API_KEY" in str(err.value)

    def test_worker_spawn_failure_raises_actionable_error(
        self, tmp_path, monkeypatch
    ):
        client = _FakeRestClient()
        _install_fake_rest(monkeypatch, client)

        def boom(repo, lease_id=None, lease_session=""):
            raise gc_workers.WorkerError("the 'agent' CLI is not on PATH")

        monkeypatch.setattr(gc_workers, "ensure_worker", boom)
        with pytest.raises(gc_cloud.CloudRunnerError) as err:
            _run_cloud_events(tmp_path)
        assert "agent" in str(err.value)
        assert client.create_calls == []  # never dispatched

    def test_cloud_runtime_skips_the_worker(self, tmp_path, monkeypatch):
        client = _FakeRestClient()
        _install_fake_rest(monkeypatch, client)
        monkeypatch.setattr(
            gc_workers, "ensure_worker",
            lambda repo: pytest.fail("cloud runtime must not touch workers"),
        )
        events = _run_cloud_events(tmp_path, runtime="cloud")
        assert events[0][1]["worker"] == ""
        call = client.create_calls[0]
        assert call["env"] is None
        assert call["work_on_current_branch"] is None

    def test_cloud_runtime_accepts_a_github_url_directly(
        self, tmp_path, monkeypatch
    ):
        client = _FakeRestClient()
        _install_fake_rest(monkeypatch, client)
        monkeypatch.setattr(
            gc_cloud, "derive_repo_ref",
            lambda path: pytest.fail("no local checkout to introspect"),
        )
        events = list(gc_cloud.run_cloud(
            "t", "https://github.com/acme/widgets", runtime="cloud",
            inactivity_timeout_s=30.0, cancel_check=lambda: False,
        ))
        assert events[0][1]["repo_url"] == "https://github.com/acme/widgets"
        assert client.create_calls[0]["repos"] == [
            {"url": "https://github.com/acme/widgets"}
        ]

    def test_native_cancel_settles_run_cancelled(self, tmp_path, monkeypatch):
        client = _FakeRestClient(
            streams=[[
                _sse("status", {"status": "RUNNING"}),
                _sse("thinking", {"text": "hm"}, id="1"),
                lambda: client.cancelled.wait(10) and None,  # hold the stream
            ]],
            statuses=("CANCELLED",),
        )
        _install_fake_rest(monkeypatch, client)
        polls = []

        def cancel_after_two_polls():
            polls.append(1)
            return len(polls) > 2

        events = _run_cloud_events(tmp_path,
                                   cancel_check=cancel_after_two_polls)
        assert "cloud.session" in [k for k, _ in events]
        assert events[-1] == ("cloud.result", {"status": "cancelled"})
        assert client.cancel_calls == [("bc-fake-1", "run-1")]

    def test_inactivity_watchdog_fires_on_true_silence(
        self, tmp_path, monkeypatch
    ):
        client = _FakeRestClient(
            streams=[[
                _sse("thinking", {"text": "hm"}, id="1"),
                lambda: client.cancelled.wait(10) and None,
            ]],
            statuses=("CANCELLED",),
        )
        _install_fake_rest(monkeypatch, client)
        events = _run_cloud_events(tmp_path, inactivity_timeout_s=0.4)
        key, obj = events[-1]
        assert key == "cloud.error"
        assert obj["timeout"] is True
        assert "no activity" in obj["error"]
        assert client.cancel_calls, "watchdog must cancel the live run"

    def test_pending_tool_call_suspends_inactivity_clock(
        self, tmp_path, monkeypatch
    ):
        def slow_tool_result():
            time.sleep(1.0)  # silent, but a tool call is in flight
            return _sse("tool_call", {
                "callId": "t-slow", "name": "shell", "status": "completed",
                "result": {"exitCode": 0, "stdout": "ok"},
            }, id="2")

        client = _FakeRestClient(streams=[[
            _sse("tool_call", {"callId": "t-slow", "name": "shell",
                               "status": "running",
                               "args": {"command": "npx tsc"}}, id="1"),
            slow_tool_result,
            _sse("result", {"status": "FINISHED"}, id="3"),
            _sse("done", {}),
        ]])
        _install_fake_rest(monkeypatch, client)
        events = _run_cloud_events(tmp_path, inactivity_timeout_s=0.4)
        assert events[-1] == ("cloud.result", {"status": "finished"})

    def test_finished_tool_call_does_not_suspend_the_clock(
        self, tmp_path, monkeypatch
    ):
        client = _FakeRestClient(
            streams=[[
                _sse("tool_call", {"callId": "t-done", "name": "shell",
                                   "status": "running",
                                   "args": {"command": "ls"}}, id="1"),
                _sse("tool_call", {"callId": "t-done", "name": "shell",
                                   "status": "completed",
                                   "result": {"exitCode": 0, "stdout": ""}},
                     id="2"),
                lambda: client.cancelled.wait(10) and None,  # true silence
            ]],
            statuses=("CANCELLED",),
        )
        _install_fake_rest(monkeypatch, client)
        events = _run_cloud_events(tmp_path, inactivity_timeout_s=0.4)
        key, obj = events[-1]
        assert key == "cloud.error" and obj["timeout"] is True

    def test_max_wall_ceiling_kills_runaway_streams(
        self, tmp_path, monkeypatch
    ):
        def chatty():
            if client.cancelled.is_set():
                return None
            time.sleep(0.05)
            return _sse("thinking", {"text": "still going"})

        client = _FakeRestClient(streams=[[chatty] * 1000],
                                 statuses=("CANCELLED",))
        _install_fake_rest(monkeypatch, client)
        started = time.monotonic()
        events = _run_cloud_events(tmp_path, max_wall_s=0.6)
        assert time.monotonic() - started < 10
        key, obj = events[-1]
        assert key == "cloud.error"
        assert obj["timeout"] is True
        assert "max wall time" in obj["error"]

    def test_first_event_watchdog_aborts_a_run_that_streams_nothing(
        self, tmp_path, monkeypatch
    ):
        """Issue #17: with first_event_timeout_s armed, a run that streams
        NO events is aborted within the window and settles as a PLAIN
        failure — no timeout flag, no run_status — so the caller neither
        reports a timeout nor re-enters the auto-retry gate."""
        client = _FakeRestClient(
            streams=[[lambda: client.cancelled.wait(10) and None]],
            statuses=("CANCELLED",),
        )
        _install_fake_rest(monkeypatch, client)
        started = time.monotonic()
        events = _run_cloud_events(tmp_path, first_event_timeout_s=0.4)
        assert time.monotonic() - started < 10
        key, obj = events[-1]
        assert key == "cloud.error"
        assert "no stream events" in obj["error"]
        assert not obj.get("timeout")
        assert obj.get("run_status") is None
        assert client.cancel_calls, "watchdog must cancel the live run"

    def test_first_event_watchdog_is_inert_once_events_flow(
        self, tmp_path, monkeypatch
    ):
        def slow_finish():
            time.sleep(0.6)  # well past the 0.2s first-event window
            return _sse("result", {"status": "FINISHED"}, id="2")

        client = _FakeRestClient(streams=[[
            _sse("thinking", {"text": "hm"}, id="1"),
            slow_finish,
            _sse("done", {}),
        ]])
        _install_fake_rest(monkeypatch, client)
        events = _run_cloud_events(tmp_path, first_event_timeout_s=0.2)
        assert events[-1] == ("cloud.result", {"status": "finished"})

    def test_followup_reuses_the_agent_and_reports_resumed(
        self, tmp_path, monkeypatch
    ):
        client = _FakeRestClient(agent_id="bc-prior")
        _install_fake_rest(monkeypatch, client)
        events = _run_cloud_events(tmp_path, agent_id="bc-prior")
        assert client.followup_calls == [
            {"agent_id": "bc-prior", "prompt": "do it"}
        ]
        assert client.create_calls == []
        assert events[0][1]["resumed"] is True
        assert events[0][1]["agentId"] == "bc-prior"
        assert events[-1] == ("cloud.result", {"status": "finished"})

    def test_failed_followup_falls_back_to_fresh_agent(
        self, tmp_path, monkeypatch
    ):
        client = _FakeRestClient(
            agent_id="bc-fresh",
            followup_error=gc_rest.RestApiError(
                "cursor api POST .../runs -> 404 not_found: unknown agent",
                status_code=404, code="not_found",
            ),
        )
        _install_fake_rest(monkeypatch, client)
        events = _run_cloud_events(tmp_path, agent_id="bc-gone")
        assert [c["agent_id"] for c in client.followup_calls] == ["bc-gone"]
        assert len(client.create_calls) == 1
        assert events[0][1]["resumed"] is False
        assert events[0][1]["agentId"] == "bc-fresh"
        assert events[-1] == ("cloud.result", {"status": "finished"})

    def test_conflicting_followup_409_surfaces_not_forks(
        self, tmp_path, monkeypatch
    ):
        client = _FakeRestClient(
            followup_error=gc_rest.RestApiError(
                "cursor api POST .../runs -> 409 run_active: busy",
                status_code=409, code="run_active",
            ),
        )
        _install_fake_rest(monkeypatch, client)
        with pytest.raises(gc_cloud.CloudRunnerError) as err:
            _run_cloud_events(tmp_path, agent_id="bc-busy")
        assert "409" in str(err.value)
        assert client.create_calls == []  # never silently forked

    def test_model_threads_into_create(self, tmp_path, monkeypatch):
        client = _FakeRestClient()
        _install_fake_rest(monkeypatch, client)
        events = _run_cloud_events(tmp_path, model="gpt-5.3-codex")
        call = client.create_calls[0]
        assert call["model_id"] == "gpt-5.3-codex"
        assert call["model_params"] is None
        assert events[0][1]["model"] == "gpt-5.3-codex"

    def test_model_catalog_validation_rejects_unknown_id(
        self, tmp_path, monkeypatch
    ):
        client = _FakeRestClient(models=["claude-fable-5", "gpt-5.3-codex"])
        _install_fake_rest(monkeypatch, client)
        with pytest.raises(gc_cloud.CloudRunnerError) as err:
            _run_cloud_events(tmp_path, model="totally-fake-model-9000")
        assert "not in the cursor model catalog" in str(err.value)
        assert "claude-fable-5" in str(err.value)  # valid ids listed
        assert client.create_calls == []  # rejected before dispatch

    def test_unfetchable_catalog_skips_validation(self, tmp_path, monkeypatch):
        """A flaky catalog endpoint must not block sends — the server
        rejects invalid models itself."""
        client = _FakeRestClient(
            models_error=gc_rest.RestNetworkError("catalog down"),
        )
        _install_fake_rest(monkeypatch, client)
        events = _run_cloud_events(tmp_path, model="gpt-5.3-codex")
        assert events[-1] == ("cloud.result", {"status": "finished"})

    def test_dropped_stream_reattaches_with_last_event_id(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(gc_cloud, "_REATTACH_BACKOFF_S", 0.0)
        client = _FakeRestClient(
            streams=[
                [
                    _sse("thinking", {"text": "before "}, id="1"),
                    _sse("thinking", {"text": "the drop"}, id="2"),
                    gc_rest.RestNetworkError("stream dropped"),
                ],
                [
                    _sse("thinking", {"text": "after the drop"}, id="3"),
                    _sse("result", {"status": "FINISHED"}, id="4"),
                    _sse("done", {}),
                ],
            ],
            statuses=("RUNNING", "FINISHED"),
        )
        _install_fake_rest(monkeypatch, client)
        events = _run_cloud_events(tmp_path)
        # Reconnected exactly where it left off — nothing lost, nothing
        # duplicated, no synthetic user-visible messages.
        assert client.stream_calls == [None, "2"]
        texts = [o.get("text") for k, o in events
                 if k == "cloud.message" and o.get("type") == "thinking"]
        assert texts == ["before ", "the drop", "after the drop"]
        reattached = [o for k, o in events if k == "sse.reattached"]
        assert len(reattached) == 1
        assert reattached[0]["last_event_id"] == "2"
        assert events[-1] == ("cloud.result", {"status": "finished"})

    def test_stream_unavailable_then_close_is_a_reconnect_not_a_failure(
        self, tmp_path, monkeypatch
    ):
        """error:stream_unavailable + done on a LIVE run (captured live,
        run_c_precancel.sse) means reconnect — never a run failure."""
        monkeypatch.setattr(gc_cloud, "_REATTACH_BACKOFF_S", 0.0)
        client = _FakeRestClient(
            streams=[
                [
                    _sse("thinking", {"text": "hm"}, id="1"),
                    _sse("error", {"code": "stream_unavailable"}),
                    _sse("done", {}),
                ],
                [
                    _sse("result", {"status": "FINISHED"}, id="2"),
                    _sse("done", {}),
                ],
            ],
            statuses=("RUNNING", "FINISHED"),
        )
        _install_fake_rest(monkeypatch, client)
        events = _run_cloud_events(tmp_path)
        assert len(client.stream_calls) == 2
        assert not [k for k, _ in events if k == "cloud.error"]
        assert events[-1] == ("cloud.result", {"status": "finished"})

    def test_reattach_budget_exhaustion_fails_the_run(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(gc_cloud, "_REATTACH_BACKOFF_S", 0.0)
        client = _FakeRestClient(
            streams=[[gc_rest.RestNetworkError("stream dropped")]],
            statuses=("RUNNING",),  # forever live — reconnects keep trying
        )
        _install_fake_rest(monkeypatch, client)
        events = _run_cloud_events(tmp_path)
        assert len(client.stream_calls) == gc_cloud.MAX_STREAM_REATTACHES + 1
        key, obj = events[-1]
        assert key == "cloud.error"
        assert "stream failed mid-run" in obj["error"]

    def test_409_on_stream_open_reattaches_instead_of_failing(
        self, tmp_path, monkeypatch
    ):
        """A 409 (e.g. stream_unavailable) on the stream GET of a LIVE run
        is 'not attachable right now', NOT a run failure — captured live:
        stream died 4s in with a 409 while the worker kept executing. It
        must ride the same reattach path as network drops."""
        monkeypatch.setattr(gc_cloud, "_REATTACH_BACKOFF_S", 0.0)
        client = _FakeRestClient(
            streams=[
                [
                    _sse("thinking", {"text": "pre-409"}, id="1"),
                    gc_rest.RestApiError(
                        "cursor api GET stream -> 409 conflict: "
                        "stream_unavailable",
                        status_code=409,
                    ),
                ],
                [
                    _sse("thinking", {"text": "post-409"}, id="2"),
                    _sse("result", {"status": "FINISHED"}, id="3"),
                    _sse("done", {}),
                ],
            ],
            statuses=("RUNNING", "FINISHED"),
        )
        _install_fake_rest(monkeypatch, client)
        events = _run_cloud_events(tmp_path)
        # Reattached with Last-Event-ID, run completed, zero cloud.error.
        assert client.stream_calls == [None, "1"]
        reattached = [o for k, o in events if k == "sse.reattached"]
        assert len(reattached) == 1
        assert not [k for k, _ in events if k == "cloud.error"]
        assert events[-1] == ("cloud.result", {"status": "finished"})

    def test_persistent_409_exhausts_budget_then_fails(
        self, tmp_path, monkeypatch
    ):
        """409s stay within the reattach budget — a permanently unattachable
        stream on a live run fails only after MAX_STREAM_REATTACHES."""
        monkeypatch.setattr(gc_cloud, "_REATTACH_BACKOFF_S", 0.0)
        client = _FakeRestClient(
            streams=[[gc_rest.RestApiError(
                "cursor api GET stream -> 409 conflict: stream_unavailable",
                status_code=409,
            )]],
            statuses=("RUNNING",),  # forever live
        )
        _install_fake_rest(monkeypatch, client)
        events = _run_cloud_events(tmp_path)
        assert len(client.stream_calls) == gc_cloud.MAX_STREAM_REATTACHES + 1
        key, obj = events[-1]
        assert key == "cloud.error"

    def test_404_on_stream_open_still_fails_immediately(
        self, tmp_path, monkeypatch
    ):
        """Regression guard: genuinely non-reconnectable statuses (404/410)
        on a live run must NOT enter the reattach loop."""
        monkeypatch.setattr(gc_cloud, "_REATTACH_BACKOFF_S", 0.0)
        client = _FakeRestClient(
            streams=[[gc_rest.RestApiError(
                "cursor api GET stream -> 404 not_found", status_code=404,
            )]],
            statuses=("RUNNING",),
        )
        _install_fake_rest(monkeypatch, client)
        events = _run_cloud_events(tmp_path)
        assert len(client.stream_calls) == 1  # no reattach attempts
        key, obj = events[-1]
        assert key == "cloud.error"

    def test_terminal_run_with_dead_stream_settles_from_get(
        self, tmp_path, monkeypatch
    ):
        """The stream died reporting the run's end: GET runs/{id} says
        FINISHED, so the drop is not an error."""
        client = _FakeRestClient(
            streams=[[
                _sse("assistant", {"text": "done"}, id="1"),
                gc_rest.RestNetworkError("stream dropped at the end"),
            ]],
            statuses=("FINISHED",),
        )
        _install_fake_rest(monkeypatch, client)
        events = _run_cloud_events(tmp_path)
        assert events[-1] == ("cloud.result", {"status": "finished"})

    def test_lying_finished_replay_settles_from_the_get_authority(
        self, tmp_path, monkeypatch
    ):
        """A cancelled run's SSE replay says FINISHED (captured live,
        run_c_postcancel.sse) — the final GET is the authority."""
        client = _FakeRestClient(
            streams=[[
                _sse("status", {"status": "FINISHED"}),  # the lie
                _sse("result", {"status": "FINISHED"}, id="1"),
                _sse("done", {}),
            ]],
            statuses=("CANCELLED",),
        )
        _install_fake_rest(monkeypatch, client)
        events = _run_cloud_events(tmp_path)
        assert events[-1] == ("cloud.result", {"status": "cancelled"})

    def test_terminal_error_carries_run_status_and_text(
        self, tmp_path, monkeypatch
    ):
        client = _FakeRestClient(
            streams=[[
                _sse("assistant", {"text": "hm"}, id="1"),
                _sse("result", {"status": "ERROR", "text": "upstream 502"},
                     id="2"),
                _sse("done", {}),
            ]],
            statuses=("ERROR",),
        )
        _install_fake_rest(monkeypatch, client)
        events = _run_cloud_events(tmp_path)
        key, obj = events[-1]
        assert key == "cloud.error"
        assert obj["run_status"] == "error"
        assert "upstream 502" in obj["error"]
        assert "unroutable_worker" not in obj  # conversation flowed

    def test_fast_error_with_zero_conversation_flags_unroutable_worker(
        self, tmp_path, monkeypatch
    ):
        """The phase-0 signature: a machine-routed run on a NEVER-verified
        worker errors fast with zero conversation events → non-retryable
        unroutable-worker failure, not a generic error."""
        client = _FakeRestClient(
            streams=[[
                _sse("status", {"status": "RUNNING"}),
                _sse("result", {"status": "ERROR"}, id="1"),
                _sse("done", {}),
            ]],
            statuses=("ERROR",),
        )
        worker = _worker_record(name="fresh-worker", verified=False)
        _install_fake_rest(monkeypatch, client, worker=worker)
        monkeypatch.setattr(gc_workers, "live_workers", lambda: [])
        events = _run_cloud_events(tmp_path)
        key, obj = events[-1]
        assert key == "cloud.error"
        assert obj["unroutable_worker"] is True
        assert obj["retryable"] is False
        assert obj["run_status"] == "error"
        assert "fresh-worker" in obj["error"]
        assert "not routable" in obj["error"]

    def test_conversation_marks_the_worker_verified(
        self, tmp_path, monkeypatch
    ):
        client = _FakeRestClient()
        worker = _worker_record(name="fresh-worker", verified=False)
        _install_fake_rest(monkeypatch, client, worker=worker)
        verified = []
        monkeypatch.setattr(
            gc_workers, "mark_verified", lambda name: verified.append(name)
        )
        events = _run_cloud_events(tmp_path)
        assert events[-1] == ("cloud.result", {"status": "finished"})
        assert verified == ["fresh-worker"]

    def test_legacy_model_string_warns_and_uses_default(
        self, tmp_path, monkeypatch
    ):
        client = _FakeRestClient()
        _install_fake_rest(monkeypatch, client)
        events = _run_cloud_events(tmp_path, model="claude-fable-5[borked")
        # The warning is the FIRST event, so the substitution lands in the
        # event log before any run activity.
        key, obj = events[0]
        assert key == "cloud.model_warning"
        assert obj["requested"] == "claude-fable-5[borked"
        assert obj["using"] == gc_runner.DEFAULT_MODEL
        assert client.create_calls[0]["model_id"] == gc_runner.DEFAULT_MODEL
        assert events[-1] == ("cloud.result", {"status": "finished"})


# ---------------------------------------------------------------------------
# translate_model — legacy ACP-era model strings → SDK id + params
# ---------------------------------------------------------------------------

# The exact params the legacy forms below must translate to (parameter ids
# verified live against Cursor.models.list() for claude-fable-5).
_FABLE_BRACKET_SELECTION = {
    "id": "claude-fable-5",
    "params": [
        {"id": "thinking", "value": "true"},
        {"id": "context", "value": "300k"},
        {"id": "effort", "value": "high"},
    ],
}
_FABLE_THINKING_HIGH_SELECTION = {
    "id": "claude-fable-5",
    "params": [
        {"id": "thinking", "value": "true"},
        {"id": "effort", "value": "high"},
    ],
}


class TestModelTranslation:
    """The sdk model catalog has BASE ids only — combined slugs like
    "claude-fable-5-thinking-high" (the old CLI shorthand, previously our
    DEFAULT_MODEL) and ACP-era handle records like
    "claude-fable-5[thinking=true,context=300k,effort=high]" are rejected
    with BadRequestError. translate_model maps both onto id + params."""

    def test_default_model_is_a_base_catalog_id(self):
        assert gc_runner.DEFAULT_MODEL == "claude-fable-5"
        assert "[" not in gc_runner.DEFAULT_MODEL
        assert "-thinking" not in gc_runner.DEFAULT_MODEL

    def test_plain_base_id_passes_through(self):
        assert gc_cloud.translate_model("gpt-5.3-codex") == ("gpt-5.3-codex", None)

    def test_none_and_blank_pass_through(self):
        assert gc_cloud.translate_model(None) == (None, None)
        assert gc_cloud.translate_model("   ") == (None, None)

    def test_thinking_level_suffix_becomes_params(self):
        value, warning = gc_cloud.translate_model("claude-fable-5-thinking-high")
        assert warning is None
        assert value == _FABLE_THINKING_HIGH_SELECTION

    def test_bare_thinking_suffix_becomes_thinking_param(self):
        value, warning = gc_cloud.translate_model("claude-sonnet-5-thinking")
        assert warning is None
        assert value == {
            "id": "claude-sonnet-5",
            "params": [{"id": "thinking", "value": "true"}],
        }

    def test_extra_high_level_maps_to_catalog_xhigh(self):
        value, warning = gc_cloud.translate_model(
            "claude-fable-5-thinking-extra-high"
        )
        assert warning is None
        assert {"id": "effort", "value": "xhigh"} in value["params"]

    def test_bracket_suffix_becomes_params(self):
        value, warning = gc_cloud.translate_model(
            "claude-fable-5[thinking=true,context=300k,effort=high]"
        )
        assert warning is None
        assert value == _FABLE_BRACKET_SELECTION

    def test_empty_bracket_reduces_to_base_id(self):
        assert gc_cloud.translate_model("claude-fable-5[]") == (
            "claude-fable-5", None,
        )

    def test_unparseable_bracket_falls_back_to_default_with_warning(self):
        value, warning = gc_cloud.translate_model("claude-fable-5[thinking")
        assert value == gc_runner.DEFAULT_MODEL
        assert warning and "claude-fable-5[thinking" in warning
        assert gc_runner.DEFAULT_MODEL in warning

    def test_malformed_bracket_pair_falls_back_with_warning(self):
        value, warning = gc_cloud.translate_model("m[thinking=]")
        assert value == gc_runner.DEFAULT_MODEL
        assert warning

    def test_unknown_thinking_level_falls_back_with_warning(self):
        value, warning = gc_cloud.translate_model("m-thinking-banana")
        assert value == gc_runner.DEFAULT_MODEL
        assert warning

    def test_dash_suffix_threads_params_into_create(self, tmp_path, monkeypatch):
        client = _FakeRestClient()
        _install_fake_rest(monkeypatch, client)
        events = _run_cloud_events(tmp_path,
                                   model="claude-fable-5-thinking-high")
        call = client.create_calls[0]
        assert call["model_id"] == _FABLE_THINKING_HIGH_SELECTION["id"]
        assert call["model_params"] == _FABLE_THINKING_HIGH_SELECTION["params"]
        assert events[0][1]["model"] == "claude-fable-5"
        assert not [o for k, o in events if k == "cloud.model_warning"]
        assert events[-1] == ("cloud.result", {"status": "finished"})

    def test_legacy_bracket_record_threads_params_into_create(
        self, tmp_path, monkeypatch
    ):
        """The exact model string a pre-swap handle recorded must never
        reach the create body verbatim (BadRequestError live under the
        bridge; the REST ModelRef wants base id + params)."""
        client = _FakeRestClient()
        _install_fake_rest(monkeypatch, client)
        events = _run_cloud_events(
            tmp_path,
            model="claude-fable-5[thinking=true,context=300k,effort=high]",
        )
        call = client.create_calls[0]
        assert call["model_id"] == _FABLE_BRACKET_SELECTION["id"]
        assert call["model_params"] == _FABLE_BRACKET_SELECTION["params"]
        assert events[0][1]["model"] == "claude-fable-5"
        assert not [o for k, o in events if k == "cloud.model_warning"]
        assert events[-1] == ("cloud.result", {"status": "finished"})


class TestInvalidModelReason:
    """invalid_model_reason — the pure SHAPE check behind create-time model
    validation (issue #12): no sdk contact, so it only rules out strings no
    catalog id could be; catalog membership stays a first-send concern."""

    def test_blank_and_well_formed_ids_pass(self):
        for ok in (
            None, "", "   ",
            "claude-fable-5",
            "gpt-5.3-codex",
            "totally-fake-model-9000",  # shape-valid; only the catalog knows
            "claude-fable-5-thinking-high",
            "claude-fable-5[thinking=true,context=300k,effort=high]",
            "claude-fable-5[]",
        ):
            assert gc_cloud.invalid_model_reason(ok) is None, ok

    def test_forms_that_would_silently_fall_back_are_rejected(self):
        for bad in ("claude-fable-5[thinking", "m[thinking=]", "m-thinking-banana"):
            reason = gc_cloud.invalid_model_reason(bad)
            assert reason and bad in reason, bad
            # A create-time caller REJECTS — the reason must not read like
            # the send-time DEFAULT_MODEL substitution.
            assert "falling back" not in reason

    def test_base_ids_no_catalog_id_could_be_are_rejected(self):
        for junk in (
            "totally fake model!!!",
            "model with spaces[thinking=true]",
            '"quoted-model"',
        ):
            assert gc_cloud.invalid_model_reason(junk), junk


# ---------------------------------------------------------------------------
# events.SdkNormalizer — SDKMessage dicts → canonical envelopes
# ---------------------------------------------------------------------------

class TestSdkNormalizer:
    def _norm(self):
        return gc_events.SdkNormalizer()

    def test_session_event_maps_to_run_started(self):
        envs = self._norm().normalize(
            "cloud.session",
            {"agentId": "agent-1", "cwd": "/w", "model": "m", "resumed": False,
             "runtime": "local", "worker": "w-1", "run_id": "run-1",
             "agents_ui_url": "https://cursor.com/agents/agent-1"},
        )
        assert envs == [{
            "source": "ghost", "kind": "lifecycle", "event": "run.started",
            "model": "m", "cwd": "/w", "harness_session_id": "agent-1",
            "runtime": "local", "worker": "w-1", "run_id": "run-1",
            "agents_ui_url": "https://cursor.com/agents/agent-1",
        }]

    def test_reattached_maps_to_log_only_lifecycle(self):
        envs = self._norm().normalize(
            "sse.reattached", {"last_event_id": "41", "attempt": 1}
        )
        assert envs[0]["event"] == "sse.reattached"
        assert envs[0]["last_event_id"] == "41"

    def test_finished_maps_to_run_completed(self):
        envs = self._norm().normalize("cloud.result", {"status": "finished"})
        assert envs[0]["event"] == "run.completed"
        assert envs[0]["status"] == "completed"

    def test_cancelled_maps_to_run_failed_cancelled(self):
        envs = self._norm().normalize("cloud.result", {"status": "cancelled"})
        assert envs[0]["event"] == "run.failed"
        assert envs[0]["cancelled"] is True
        assert "cancel" in envs[0]["error"]

    def test_expired_maps_to_run_failed_timeout(self):
        envs = self._norm().normalize("cloud.result", {"status": "expired"})
        assert envs[0]["event"] == "run.failed"
        assert envs[0]["timeout"] is True

    def test_error_status_maps_to_run_failed(self):
        envs = self._norm().normalize("cloud.result", {"status": "error"})
        assert envs[0]["event"] == "run.failed"
        assert "error" in envs[0]["error"]

    def test_sdk_error_maps_to_run_failed(self):
        envs = self._norm().normalize(
            "cloud.error", {"error": "cursor run timed out: no activity for 600s",
                          "timeout": True}
        )
        assert envs[0]["event"] == "run.failed"
        assert envs[0]["timeout"] is True

    def test_assistant_message_maps_to_content(self):
        envs = self._norm().normalize("cloud.message", {
            "type": "assistant",
            "message": {"content": [{"type": "text", "text": "hello "},
                                    {"type": "text", "text": "world"}]},
        })
        assert envs == [{
            "source": "ghost", "kind": "content", "delta": "hello world",
            "done": False,
        }]

    def test_thinking_maps_to_reasoning(self):
        envs = self._norm().normalize("cloud.message", {
            "type": "thinking", "text": "pondering notes.txt",
        })
        assert envs[0]["event"] == "reasoning"
        assert envs[0]["text"] == "pondering notes.txt"

    def test_noise_types_produce_no_envelopes(self):
        norm = self._norm()
        for mtype in ("system", "user", "request", "status"):
            assert norm.normalize("cloud.message", {"type": mtype}) == []

    def test_shell_tool_call_round_trip(self):
        norm = self._norm()
        started = norm.normalize("cloud.message", {
            "type": "tool_call", "call_id": "t1", "name": "shell",
            "status": "running", "args": {"command": "ls -la"},
        })
        assert started == [{
            "source": "ghost", "kind": "tool_use", "id": "t1",
            "tool": "shell", "status": "running", "title": "ls -la",
            "name": "shell", "command": "ls -la",
        }]
        done = norm.normalize("cloud.message", {
            "type": "tool_call", "call_id": "t1", "name": "shell",
            "status": "completed",
            "result": {"exitCode": 0, "stdout": "calc.py"},
        })
        assert done[0]["kind"] == "tool_result"
        assert done[0]["status"] == "done"
        assert "calc.py" in done[0]["output"]
        assert isinstance(done[0]["durationMs"], int)

    def test_repeated_running_messages_are_deduped(self):
        norm = self._norm()
        first = norm.normalize("cloud.message", {
            "type": "tool_call", "call_id": "t1", "name": "shell",
            "status": "running", "args": {"command": "ls"},
        })
        assert len(first) == 1
        # Args already captured — a re-emit adds nothing and is dropped.
        again = norm.normalize("cloud.message", {
            "type": "tool_call", "call_id": "t1", "name": "shell",
            "status": "running", "args": {"command": "ls"},
        })
        assert again == []

    def test_argless_running_message_upgraded_when_args_arrive(self):
        # Captured live 2026-07-09: the FIRST running message often has no
        # args; the description/command arrives on a re-emit. The upgrade
        # emits ONE refined tool_use under the same call id, then further
        # re-emits are dropped.
        norm = self._norm()
        first = norm.normalize("cloud.message", {
            "type": "tool_call", "call_id": "t1", "name": "shell",
            "status": "running", "args": {},
        })
        assert len(first) == 1 and first[0]["title"] == "shell"
        upgraded = norm.normalize("cloud.message", {
            "type": "tool_call", "call_id": "t1", "name": "shell",
            "status": "running",
            "args": {"command": "ls", "description": "List repo root"},
        })
        assert len(upgraded) == 1
        assert upgraded[0]["id"] == "t1"
        assert upgraded[0]["updated"] is True
        # Title prefers the model-written description; command survives.
        assert upgraded[0]["title"] == "List repo root"
        assert upgraded[0]["command"] == "ls"
        # A third re-emit (args unchanged) is dropped.
        assert norm.normalize("cloud.message", {
            "type": "tool_call", "call_id": "t1", "name": "shell",
            "status": "running",
            "args": {"command": "ls", "description": "List repo root"},
        }) == []

    def test_subagent_task_tool_call_normalizes_typed(self):
        # `task` args captured live 2026-07-09 (run-c99dd650).
        norm = self._norm()
        envs = norm.normalize("cloud.message", {
            "type": "tool_call", "call_id": "t2", "name": "task",
            "status": "running",
            "args": {
                "description": "Explore blockchain test infrastructure",
                "prompt": "In the repo at /workspace I need a thorough report…",
                "subagentType": "explore",
                "model": "composer-2.5-fast",
            },
        })
        assert len(envs) == 1
        env = envs[0]
        assert env["tool"] == "subagent"
        assert env["title"] == "Explore blockchain test infrastructure"
        assert env["subagent_model"] == "composer-2.5-fast"
        assert env["subagent_type"] == "explore"
        # Snippet only — never the multi-KB prompt verbatim beyond the cap.
        assert env["prompt_snippet"].startswith("In the repo at /workspace")

    def test_subagent_argless_falls_back_generic(self):
        # args are optional/truncatable upstream — the envelope must stay
        # informative with nothing but the name.
        envs = self._norm().normalize("cloud.message", {
            "type": "tool_call", "call_id": "t3", "name": "task",
            "status": "running",
        })
        assert envs[0]["tool"] == "subagent"
        assert envs[0]["title"] == "sub-agent"

    def test_todo_write_normalizes_to_plan_items(self):
        envs = self._norm().normalize("cloud.message", {
            "type": "tool_call", "call_id": "t4", "name": "todo_write",
            "status": "running",
            "args": {"todos": [
                {"id": "env", "content": "Bootstrap env",
                 "status": "TODO_STATUS_COMPLETED"},
                {"id": "impl", "content": "Wire nonce guard",
                 "status": "TODO_STATUS_IN_PROGRESS"},
            ]},
        })
        env = envs[0]
        assert env["tool"] == "plan"  # NOT file-edit despite the _write name
        assert env["plan_items"] == [
            {"content": "Bootstrap env", "status": "completed"},
            {"content": "Wire nonce guard", "status": "in_progress"},
        ]
        assert "1/2 done" in env["title"]
        assert "Wire nonce guard" in env["title"]

    def test_todo_write_args_only_at_completion_still_fold(self):
        # Captured live 2026-07-09 (fixture tool_vocabulary_live.jsonl):
        # todo_write streams an ARGLESS running message and the todos only
        # arrive on the completed message. The salvage upgrade must emit a
        # refined tool_use (with plan_items) before the result.
        norm = self._norm()
        first = norm.normalize("cloud.message", {
            "type": "tool_call", "call_id": "t10", "name": "todo_write",
            "status": "running",
        })
        assert first[0]["title"] == "plan updated"
        assert "plan_items" not in first[0]
        envs = norm.normalize("cloud.message", {
            "type": "tool_call", "call_id": "t10", "name": "todo_write",
            "status": "completed",
            "args": {"todos": [
                {"id": "a", "content": "Bootstrap env",
                 "status": "TODO_STATUS_IN_PROGRESS"},
            ]},
        })
        kinds = [e["kind"] for e in envs]
        assert kinds == ["tool_use", "tool_result"]
        assert envs[0]["updated"] is True
        assert envs[0]["plan_items"] == [
            {"content": "Bootstrap env", "status": "in_progress"},
        ]

    def test_live_fixture_tool_vocabulary_replay(self):
        """Replay the sanitized live capture (2026-07-09, run-c99dd650)
        through the REAL sse→message→normalizer path and assert the
        envelope/args relations hold — no snapshots, invariants only."""
        # Two layouts: in-place (fixtures beside the plugin modules) and CI
        # (unit.yml copies *.py into plugins/ghost_cursor/ but fixtures next
        # to the relocated tests) — probe both.
        rel = Path("fixtures") / "rest_v1" / "tool_vocabulary_live.jsonl"
        candidates = [
            Path(gc_events.__file__).parent / rel,
            Path(__file__).parent / rel,
        ]
        fixture = next((p for p in candidates if p.is_file()), None)
        assert fixture is not None, f"fixture missing from all of: {candidates}"
        norm = self._norm()
        by_call: dict = {}
        for line in fixture.read_text().splitlines():
            if not line.strip() or line.startswith("#"):
                continue
            rec = json.loads(line)
            msg = gc_cloud._message_from_sse(
                gc_rest.SseEvent(event=rec["event"], data=rec["data"],
                                 id=None, raw_data="")
            )
            assert msg is not None
            for env in norm.normalize("cloud.message", msg):
                if env.get("kind") == "tool_use":
                    by_call.setdefault(env["id"], []).append(env)

        # Every described shell call surfaces the description as title and
        # keeps the raw command on the envelope.
        shell = [e for envs in by_call.values() for e in envs
                 if e.get("name") == "run_terminal_cmd"]
        assert shell, "fixture must contain a described shell call"
        for env in shell:
            assert env["title"] == env["description"]
            assert env["command"]
        # The task call is a typed subagent envelope with model + snippet.
        tasks = [e for envs in by_call.values() for e in envs
                 if e.get("tool") == "subagent"]
        assert tasks and all(
            e.get("subagent_model") and e.get("prompt_snippet")
            for e in tasks
        )
        # The argless-running→args-at-completion pattern (todo_write) folds
        # to plan items via the salvage upgrade.
        plans = [e for envs in by_call.values() for e in envs
                 if e.get("tool") == "plan" and e.get("plan_items")]
        assert plans, "todo_write args at completion must still fold"
        # await surfaced as its own kind, never a bare shell.
        awaits = [e for envs in by_call.values() for e in envs
                  if e.get("tool") == "await"]
        assert awaits
        # NOTHING in the fixture may normalize to an untitled envelope —
        # the exact regression this change kills.
        for envs in by_call.values():
            assert str(envs[-1].get("title") or "").strip(), envs[-1]

    def test_await_and_search_and_unknown_tools_stay_specific(self):
        norm = self._norm()
        awaited = norm.normalize("cloud.message", {
            "type": "tool_call", "call_id": "t5", "name": "await",
            "status": "running", "args": {"taskId": "rush-install"},
        })
        assert awaited[0]["tool"] == "await"
        assert awaited[0]["title"] == "awaiting `rush-install`"
        assert awaited[0]["await_task_id"] == "rush-install"
        search = norm.normalize("cloud.message", {
            "type": "tool_call", "call_id": "t6", "name": "grep_search",
            "status": "running",
            "args": {"pattern": "prepareCalls|nonce", "path": "/ws/src"},
        })
        assert search[0]["tool"] == "search"
        assert search[0]["title"] == "searching 'prepareCalls|nonce' in /ws/src"
        # Unknown tool name (vocabulary is undocumented upstream): generic
        # fallback keeps the real name + salvaged description, never a
        # bare unlabeled shell.
        unknown = norm.normalize("cloud.message", {
            "type": "tool_call", "call_id": "t7", "name": "future_tool",
            "status": "running", "args": {"description": "Do a new thing"},
        })
        assert unknown[0]["name"] == "future_tool"
        assert unknown[0]["title"] == "Do a new thing"

    def test_nonzero_exit_code_marks_result_error(self):
        norm = self._norm()
        norm.normalize("cloud.message", {
            "type": "tool_call", "call_id": "t8", "name": "shell",
            "status": "running", "args": {"command": "false"},
        })
        envs = norm.normalize("cloud.message", {
            "type": "tool_call", "call_id": "t8", "name": "shell",
            "status": "completed",
            "result": {"exitCode": 1, "stdout": "", "stderr": "nope"},
        })
        assert envs[0]["status"] == "error"
        assert "nope" in envs[0]["output"]

    def test_error_status_tool_call_maps_to_error_result(self):
        norm = self._norm()
        norm.normalize("cloud.message", {
            "type": "tool_call", "call_id": "t9", "name": "shell",
            "status": "running", "args": {"command": "boom"},
        })
        envs = norm.normalize("cloud.message", {
            "type": "tool_call", "call_id": "t9", "name": "shell",
            "status": "error", "result": "command not found: boom",
        })
        assert envs[0]["kind"] == "tool_result"
        assert envs[0]["status"] == "error"
        assert "not found" in envs[0]["output"]

    def test_edit_tool_full_content_yields_file_diff(self):
        norm = self._norm()
        norm.normalize("cloud.message", {
            "type": "tool_call", "call_id": "e1", "name": "edit_file",
            "status": "running", "args": {"path": "/w/calc.py"},
        })
        envs = norm.normalize("cloud.message", {
            "type": "tool_call", "call_id": "e1", "name": "edit_file",
            "status": "completed",
            "result": {"path": "/w/calc.py",
                       "beforeFullFileContent": "a\n",
                       "afterFullFileContent": "a\nb\n"},
        })
        assert envs[0]["kind"] == "tool_result"
        assert envs[0]["additions"] == 1 and envs[0]["deletions"] == 0
        diff = envs[1]
        assert diff["kind"] == "file_diff"
        assert diff["path"] == "/w/calc.py"
        assert diff["status"] == "M"
        assert "+b" in diff["diff"]

    def test_edit_tool_old_new_text_blocks_yield_file_diff(self):
        norm = self._norm()
        norm.normalize("cloud.message", {
            "type": "tool_call", "call_id": "e2", "name": "write",
            "status": "running", "args": {"path": "/w/notes.txt"},
        })
        envs = norm.normalize("cloud.message", {
            "type": "tool_call", "call_id": "e2", "name": "write",
            "status": "completed",
            "result": {"content": [{"path": "/w/notes.txt",
                                    "oldText": "", "newText": "hello\n"}]},
        })
        diffs = [e for e in envs if e["kind"] == "file_diff"]
        assert len(diffs) == 1
        assert diffs[0]["status"] == "A"
        assert diffs[0]["after"] == "hello\n"

    def test_real_sdk_edit_payload_yields_file_diff(self):
        """Regression for the live blind spot (2026-07-03): a REAL run
        created + committed a file but the completion said "no files were
        changed". The real edit tool's result wraps its payload in
        {"status": "success", "value": {linesAdded, linesRemoved,
        diffString}} and carries the path ONLY in the call's args.
        Payloads in the fixture were captured verbatim from the
        reproduction (model gpt-5.4-nano)."""
        msgs = [
            json.loads(line)
            for line in SDK_EDIT_FIXTURE.read_text().splitlines()
            if line.strip()
        ]
        norm = self._norm()
        envs = []
        for msg in msgs:
            envs.extend(norm.normalize("cloud.message", msg))

        diffs = [e for e in envs if e["kind"] == "file_diff"]
        assert len(diffs) == 1
        assert diffs[0]["path"] == "/private/tmp/gc-probe/repo/hello.txt"
        assert diffs[0]["status"] == "A"  # "--- /dev/null" diff header
        assert "+hello from sdk probe" in diffs[0]["diff"]
        assert diffs[0]["added"] == 1  # linesAdded from the payload

        edit_result = next(
            e for e in envs
            if e["kind"] == "tool_result" and e["id"] == msgs[0]["call_id"]
        )
        assert edit_result["additions"] == 1

        # The shell result rides the same {"status", "value"} envelope:
        # output and the non-zero exit code must still be mined from it.
        shell_result = [e for e in envs if e["kind"] == "tool_result"][-1]
        assert shell_result is not edit_result
        assert shell_result["status"] == "error"  # exitCode 128 in "value"
        assert "not a git repository" in shell_result["output"]

    def test_model_warning_maps_to_lifecycle(self):
        envs = self._norm().normalize("cloud.model_warning", {
            "warning": "requested model 'x[y' has an unparseable bracket "
                       "suffix — falling back to 'claude-fable-5'",
            "requested": "x[y", "using": "claude-fable-5",
        })
        assert envs[0]["kind"] == "lifecycle"
        assert envs[0]["event"] == "model.warning"
        assert envs[0]["requested"] == "x[y"
        assert envs[0]["using"] == "claude-fable-5"

    def test_terminal_tool_call_without_start_synthesizes_tool_use(self):
        envs = self._norm().normalize("cloud.message", {
            "type": "tool_call", "call_id": "fast", "name": "shell",
            "status": "completed", "args": {"command": "true"},
            "result": {"exitCode": 0, "stdout": ""},
        })
        assert [e["kind"] for e in envs] == ["tool_use", "tool_result"]
        assert envs[0]["status"] == "running"
        assert envs[1]["status"] == "done"

    def test_unstable_tool_payload_shapes_never_crash(self):
        norm = self._norm()
        for weird in (None, 42, "text", ["a", 1], {"nested": {"deep": object()}}):
            envs = norm.normalize("cloud.message", {
                "type": "tool_call", "call_id": f"w-{id(weird)}",
                "name": "mystery", "status": "completed", "result": weird,
            })
            assert envs, "terminal tool_call must always render"
            assert envs[-1]["kind"] == "tool_result"

    def test_usage_maps_to_log_only_lifecycle(self):
        envs = self._norm().normalize("cloud.message", {
            "type": "usage", "usage": {"total_tokens": 1234},
        })
        assert envs[0]["event"] == "usage"
        assert envs[0]["usage"]["total_tokens"] == 1234

    def test_unknown_message_type_passes_through(self):
        envs = self._norm().normalize("cloud.message", {"type": "mystery", "x": 1})
        assert envs[0]["event"] == "passthrough"
        assert envs[0]["name"] == "cloud.mystery"

    def test_unknown_runner_event_passes_through(self):
        envs = self._norm().normalize("cloud.wat", {"x": 1})
        assert envs[0]["event"] == "passthrough"
        assert envs[0]["name"] == "cloud.wat"


# ---------------------------------------------------------------------------
# Session supervisor — durable supervision record (RFC §1)
# ---------------------------------------------------------------------------

class TestSupervisionRecord:
    def test_legacy_entry_reads_as_unsupervised(self, clean_state):
        gc_handles.record("s-legacy", repo="/r", status="running")
        entry = gc_handles.get("s-legacy")
        sup = gc_handles.supervision_of(entry)
        assert sup["phase"] == ""
        assert sup["current_attempt_id"] == ""
        assert sup["last_seq_delivered"] == {}
        assert not gc_handles.supervision_is_live(entry)

    def test_record_supervision_merges_and_persists(self, clean_state):
        gc_handles.record("s-sup", repo="/r", status="running")
        gc_handles.record_supervision(
            "s-sup", phase="streaming", current_attempt_id="att-1", attempt_n=1
        )
        gc_handles.record_supervision("s-sup", phase="retrying", attempt_n=2)
        sup = gc_handles.supervision_of(gc_handles.get("s-sup"))
        assert sup["phase"] == "retrying"
        assert sup["current_attempt_id"] == "att-1"  # untouched by the merge
        assert sup["attempt_n"] == 2
        assert gc_handles.supervision_is_live(gc_handles.get("s-sup"))
        # The handle's own fields are untouched.
        assert gc_handles.get("s-sup")["repo"] == "/r"

    def test_delivery_cursor_is_advance_only_per_subscriber(self, clean_state):
        gc_handles.record("s-cur", repo="/r", status="running")
        gc_handles.advance_delivery_cursor("s-cur", "alice", 5)
        gc_handles.advance_delivery_cursor("s-cur", "alice", 3)  # stale writer
        gc_handles.advance_delivery_cursor("s-cur", "bob", 2)
        sup = gc_handles.supervision_of(gc_handles.get("s-cur"))
        assert sup["last_seq_delivered"] == {"alice": 5, "bob": 2}

    def test_cursor_is_not_writable_through_record_supervision(self, clean_state):
        gc_handles.record("s-cur2", repo="/r", status="running")
        gc_handles.advance_delivery_cursor("s-cur2", "alice", 5)
        gc_handles.record_supervision(
            "s-cur2", phase="streaming", last_seq_delivered={"alice": 0}
        )
        sup = gc_handles.supervision_of(gc_handles.get("s-cur2"))
        assert sup["last_seq_delivered"] == {"alice": 5}

    def test_transition_is_the_exactly_once_settle_gate(self, clean_state):
        gc_handles.record("s-gate", repo="/r", status="running")
        gc_handles.record_supervision("s-gate", phase="streaming")
        assert gc_handles.transition_supervision("s-gate", "completed") is True
        # Second writer loses: the phase is already terminal.
        assert gc_handles.transition_supervision("s-gate", "failed") is False
        assert (
            gc_handles.supervision_of(gc_handles.get("s-gate"))["phase"]
            == "completed"
        )

    def test_never_supervised_entry_refuses_the_transition(self, clean_state):
        gc_handles.record("s-none", repo="/r", status="running")
        assert gc_handles.transition_supervision("s-none", "failed") is False


# ---------------------------------------------------------------------------
# Session supervisor — in-process dispatch lifecycle (attempt identity,
# derived events, retry policy per RFC §4/§5)
# ---------------------------------------------------------------------------

class TestSupervisedDispatch:
    def _fast_retries(self, monkeypatch):
        monkeypatch.setattr(gc, "_AUTO_RETRY_BACKOFF_S", (0.0, 0.0, 0.0))

    def test_every_spilled_event_carries_the_attempt_id(
        self, clean_state, monkeypatch, tmp_path
    ):
        release = threading.Event()
        release.set()
        monkeypatch.setattr(
            gc_cloud, "run_cloud", _gated_replay_factory(release, sid="a-att")
        )
        _start_run("t", repo=str(tmp_path))
        job = _job_for("a-att")
        assert job.done_event.wait(10)

        lines = _log_lines(job.session_name)
        assert lines, "the run must have spilled events"
        attempt_ids = {l.get("attemptId") for l in lines}
        assert attempt_ids == {job.current_attempt_id}
        assert job.current_attempt_id.startswith("att-")
        # The durable record carries the same attempt identity.
        sup = gc_handles.supervision_of(gc_handles.get(job.session_name))
        assert sup["current_attempt_id"] == job.current_attempt_id
        assert sup["attempt_n"] == 1

    def test_dispatch_marks_streaming_and_finalize_settles_the_phase(
        self, clean_state, monkeypatch, tmp_path
    ):
        release = threading.Event()
        monkeypatch.setattr(
            gc_cloud, "run_cloud", _gated_replay_factory(release, sid="a-ph")
        )
        _start_run("t", repo=str(tmp_path))
        job = _job_for("a-ph")
        assert _wait_until(
            lambda: gc_handles.supervision_of(
                gc_handles.get(job.session_name)
            )["phase"] == "streaming"
        )
        release.set()
        assert job.done_event.wait(10)
        sup = gc_handles.supervision_of(gc_handles.get(job.session_name))
        assert sup["phase"] == "completed"
        # The supervisor-derived settled event, stamped, no death shape on
        # a completed run.
        settled = [e for n, e in _lifecycle_trail(job.session_name)
                   if n == "session.settled"]
        assert len(settled) == 1
        assert settled[0]["status"] == "completed"
        assert settled[0]["attemptId"] == job.current_attempt_id
        assert "death_shape" not in settled[0]

    def test_durable_progress_is_derived_once_per_attempt(
        self, clean_state, monkeypatch, tmp_path
    ):
        release = threading.Event()
        release.set()
        # Two completed edits in one attempt — one derived marker only.
        monkeypatch.setattr(
            gc_cloud, "run_cloud",
            _gated_replay_factory(release, sid="a-dur",
                                  early_edit=True, late_edit=True),
        )
        _start_run("t", repo=str(tmp_path))
        job = _job_for("a-dur")
        assert job.done_event.wait(10)
        durable = [e for n, e in _lifecycle_trail(job.session_name)
                   if n == "durable_progress"]
        assert len(durable) == 1
        assert durable[0]["evidence"] in ("tool_result", "file_diff")
        assert durable[0]["attemptId"] == job.current_attempt_id

    def test_retry_mints_a_new_attempt_and_emits_retry_started(
        self, clean_state, monkeypatch, tmp_path
    ):
        self._fast_retries(monkeypatch)
        release = threading.Event()
        release.set()
        seq = _SdkSequence(
            _terminal_error_replay(sid="a-rt"),
            _gated_replay_factory(release, sid="a-rt"),
        )
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)
        _start_run("t", repo=str(tmp_path))
        job = _job_for("a-rt")
        assert job.done_event.wait(10)

        trail = _lifecycle_trail(job.session_name)
        first_attempt = next(
            e["attemptId"] for n, e in trail if n == "run.started"
        )
        started = [e for n, e in trail if n == "retry_started"]
        assert len(started) == 1
        assert started[0]["attempt_n"] == 2
        # The retry event carries the NEW attemptId (RFC §5).
        assert started[0]["attemptId"] != first_attempt
        assert started[0]["attemptId"] == job.current_attempt_id
        # The legacy alias stays during migration, same attempt identity.
        alias = [e for n, e in trail if n == "cloud.autoretry"]
        assert len(alias) == 1
        assert alias[0]["attemptId"] == started[0]["attemptId"]
        # The second run's events are stamped with the new attempt.
        second_started = [e for n, e in trail if n == "run.started"][1]
        assert second_started["attemptId"] == job.current_attempt_id
        sup = gc_handles.supervision_of(gc_handles.get(job.session_name))
        assert sup["attempt_n"] == 2
        assert sup["phase"] == "completed"

    def test_durable_progress_suppresses_the_retry_with_a_marker(
        self, clean_state, monkeypatch, tmp_path
    ):
        self._fast_retries(monkeypatch)
        seq = _SdkSequence(
            _terminal_error_replay(sid="a-sup", meaningful=True),
        )
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)
        _start_run("t", repo=str(tmp_path))
        job = _job_for("a-sup")
        assert job.done_event.wait(10)

        assert len(seq.calls) == 1  # never re-prompted
        trail = _lifecycle_trail(job.session_name)
        assert not [n for n, _ in trail if n == "retry_started"]
        suppressed = [e for n, e in trail if n == "retry_suppressed"]
        assert len(suppressed) == 1
        assert "double-applying" in suppressed[0]["reason"]

    def test_death_shape_fast_fail_vs_mid_flight(
        self, clean_state, monkeypatch, tmp_path
    ):
        self._fast_retries(monkeypatch)
        # Zero-progress lifecycle-only failure (non-retryable so it
        # settles on the first attempt) → fast_fail.
        seq = _SdkSequence(
            _terminal_error_replay(sid="a-ff", retryable=False),
        )
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)
        _start_run("t", repo=str(tmp_path))
        job = _job_for("a-ff")
        assert job.done_event.wait(10)
        settled = [e for n, e in _lifecycle_trail(job.session_name)
                   if n == "session.settled"]
        assert settled[0]["death_shape"] == "fast_fail"

        # A failure after real streamed events → mid_flight.
        gc_jobs.registry._reset_for_tests()
        seq2 = _SdkSequence(
            _terminal_error_replay(sid="a-mf", meaningful=True),
        )
        monkeypatch.setattr(gc_cloud, "run_cloud", seq2)
        _start_run("t", repo=str(tmp_path))
        job2 = _job_for("a-mf")
        assert job2.done_event.wait(10)
        settled2 = [e for n, e in _lifecycle_trail(job2.session_name)
                    if n == "session.settled"]
        assert settled2[0]["death_shape"] == "mid_flight"

    def test_interrupt_reprompt_leaves_the_supervisor_event_trace(
        self, clean_state, monkeypatch, tmp_path
    ):
        release1, release2 = threading.Event(), threading.Event()
        release2.set()
        seq = _SdkSequence(
            _gated_replay_factory(release1, sid="a-int"),
            _gated_replay_factory(release2, sid="a-int", early_edit=False),
        )
        monkeypatch.setattr(gc_cloud, "run_cloud", seq)
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        _assert_running_ack(cursor_send_message(name, "first"))
        ack = cursor_send_message(name, "steer")  # interrupts the live run
        assert "interrupted" in ack
        job = _job_for("a-int")
        assert job.done_event.wait(10)
        names = [n for n, _ in _lifecycle_trail(name)]
        assert "interrupt_requested" in names
        assert "interrupted" in names
        # Ordering: requested strictly before the interrupted ack.
        assert names.index("interrupt_requested") < names.index("interrupted")


# ---------------------------------------------------------------------------
# Session supervisor — reconciler + re-attach (RFC §1/§2/§3/§6)
# ---------------------------------------------------------------------------

def _seed_reattachable(name="lost-run", repo="/tmp/r", agent="bc-lost",
                       run="run-9", session_key=""):
    """A handle left behind by a dead process: status running, supervision
    phase streaming, remote agent + run ids recorded."""
    gc_handles.record(
        name, repo=repo, status="running", task="finish the thing",
        session_key=session_key, cursor_session_id=agent, runtime="local",
        latest_run_id=run, model="m",
    )
    gc_handles.record_supervision(
        name, phase="streaming", current_attempt_id="att-prior", attempt_n=1
    )
    return name


def _install_supervisor_client(monkeypatch, client):
    monkeypatch.setattr(gc_cloud, "make_client", lambda: client)


def _wait_settled(name, status, timeout=10.0):
    assert _wait_until(
        lambda: (gc_handles.get(name) or {}).get("status") == status,
        timeout=timeout,
    ), (
        f"session {name!r} did not settle to {status!r}; "
        f"entry={gc_handles.get(name)}"
    )


class TestCreateIntentAndRecovery:
    """Cursor's create is NOT idempotent (no idempotency key in the API):
    a durable intent is persisted before the POST, the returned identity
    is persisted at the producer boundary, and a crashed create is
    recovered by bounded newest-title/time matching — ambiguity is
    reported, never auto-cancelled."""

    def test_intent_fires_before_create_and_identity_lands_at_producer(
        self, tmp_path, monkeypatch
    ):
        client = _FakeRestClient(agent_id="bc-intent-1", run_id="run-intent-1")
        _install_fake_rest(monkeypatch, client)
        order = []
        list(gc_cloud.run_cloud(
            "do it", str(tmp_path),
            inactivity_timeout_s=30.0,
            cancel_check=lambda: False,
            on_create_intent=lambda: order.append(
                ("intent", len(client.create_calls))
            ),
            on_created=lambda agent_id, run_id, resumed: order.append(
                ("created", agent_id, run_id, resumed)
            ),
        ))
        # Intent persisted BEFORE the non-idempotent POST left the box…
        assert order[0] == ("intent", 0)
        # …and the returned identity persisted at the producer boundary.
        assert order[1] == ("created", "bc-intent-1", "run-intent-1", False)

    def test_send_persists_intent_then_agent_identity_on_the_handle(
        self, clean_state, monkeypatch, tmp_path
    ):
        client = _FakeRestClient(agent_id="bc-intent-2", run_id="run-intent-2")
        _install_fake_rest(monkeypatch, client)
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        cursor_send_message(name, "build the thing")
        job = _job_for("bc-intent-2")
        assert job.done_event.wait(10)
        entry = gc_handles.get(name)
        assert entry["cursor_session_id"] == "bc-intent-2"
        assert entry["latest_run_id"] == "run-intent-2"
        # The intent settled to "recorded" — recovery has nothing to do.
        assert entry["create_intent"]["state"] == "recorded"

    def _stale_intent_entry(self, name, ts):
        gc_handles.record(
            name, repo="/w", status="created", session_key="",
            create_intent={"ts": ts, "state": "creating"},
        )

    @staticmethod
    def _iso(t):
        import datetime as _dt

        return _dt.datetime.fromtimestamp(
            t, _dt.timezone.utc
        ).isoformat().replace("+00:00", "Z")

    def test_reconciler_adopts_a_unique_matching_agent(
        self, clean_state, monkeypatch
    ):
        intent_ts = time.time() - 600
        self._stale_intent_entry("Fix the flux capacitor", intent_ts)
        client = _FakeRestClient()
        client.list_agents = lambda limit=20: {"items": [
            {"id": "bc-match", "name": "Fix the flux capacitor",
             "createdAt": self._iso(intent_ts + 40), "latestRunId": "run-n"},
            {"id": "bc-unrelated", "name": "Other work",
             "createdAt": self._iso(intent_ts + 20)},
        ]}
        monkeypatch.setattr(gc_cloud, "make_client", lambda: client)
        monkeypatch.setattr(gc_supervisor, "ensure_supervisor", lambda name: None)

        gc_supervisor.reconcile_once()
        entry = gc_handles.get("Fix the flux capacitor")
        assert entry["cursor_session_id"] == "bc-match"
        assert entry["latest_run_id"] == "run-n"
        assert entry["create_intent"]["state"] == "recovered"

    def test_ambiguous_recovery_stays_unresolved_and_adopts_none(
        self, clean_state, monkeypatch
    ):
        """Several title/time matches: adopting the newest would guess —
        the intent stays UNRESOLVED for operator review, nothing is
        attached, nothing is auto-cancelled."""
        intent_ts = time.time() - 600
        self._stale_intent_entry("Fix the flux capacitor", intent_ts)
        client = _FakeRestClient()
        client.list_agents = lambda limit=20: {"items": [
            {"id": "bc-newer", "name": "Fix the flux capacitor",
             "createdAt": self._iso(intent_ts + 40), "latestRunId": "run-n"},
            {"id": "bc-older", "name": "Fix the flux capacitor",
             "createdAt": self._iso(intent_ts + 10), "latestRunId": "run-o"},
        ]}
        monkeypatch.setattr(gc_cloud, "make_client", lambda: client)
        monkeypatch.setattr(gc_supervisor, "ensure_supervisor", lambda name: None)

        gc_supervisor.reconcile_once()
        entry = gc_handles.get("Fix the flux capacitor")
        assert not entry.get("cursor_session_id")
        assert entry["create_intent"]["state"] == "unresolved"
        note = str(entry.get("status_note") or "")
        assert "bc-newer" in note and "bc-older" in note
        assert "cursor.com/agents" in note

    def test_recovery_runs_for_the_real_crash_shape_spawning_phase(
        self, clean_state, monkeypatch
    ):
        """The actual crash shape: begin_attempt left supervision phase
        'spawning' (LIVE), the intent says 'creating', no agent id was
        ever recorded. Recovery must run BEFORE ordinary live-phase
        attachment — a supervisor attached here would settle the session
        failed ('no remote agent id') and strand the real agent."""
        intent_ts = time.time() - 600
        gc_handles.record(
            "Crashed mid create", repo="/w", status="created",
            session_key="",
            create_intent={"ts": intent_ts, "state": "creating"},
        )
        gc_handles.record_supervision(
            "Crashed mid create", phase="spawning",
            current_attempt_id="att-crash", attempt_n=1,
        )
        client = _FakeRestClient()
        client.list_agents = lambda limit=20: {"items": [
            {"id": "bc-crashed", "name": "Crashed mid create",
             "createdAt": self._iso(intent_ts + 30), "latestRunId": "run-c"},
        ]}
        monkeypatch.setattr(gc_cloud, "make_client", lambda: client)
        monkeypatch.setattr(gc_supervisor, "ensure_supervisor", lambda name: None)

        gc_supervisor.reconcile_once()
        entry = gc_handles.get("Crashed mid create")
        assert entry["cursor_session_id"] == "bc-crashed"
        assert entry["create_intent"]["state"] == "recovered"
        assert entry["status"] != "failed"

    def test_pending_create_never_attaches_a_false_settling_supervisor(
        self, clean_state
    ):
        """A live-phase handle whose create is still pending (creating
        intent, no agent id) must not get a supervisor: it would settle
        'orphaned: no remote agent id' while the create may have landed."""
        gc_handles.record(
            "Pending create", repo="/w", status="created", session_key="",
            create_intent={"ts": time.time(), "state": "creating"},
        )
        gc_handles.record_supervision(
            "Pending create", phase="spawning",
            current_attempt_id="att-p", attempt_n=1,
        )
        assert gc_supervisor.ensure_supervisor("Pending create") is None
        assert str(gc_handles.get("Pending create").get("status")) != "failed"

    def test_create_is_refused_when_intent_cannot_be_persisted(
        self, clean_state, monkeypatch, tmp_path
    ):
        """The POST is non-idempotent: with no durable intent there is no
        recovery path, so an unpersistable intent must abort BEFORE the
        create leaves the box."""
        client = _FakeRestClient()
        _install_fake_rest(monkeypatch, client)
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        monkeypatch.setattr(gc_handles, "_save_locked", lambda: False)

        out = cursor_send_message(name, "build the thing")
        job = gc_jobs.registry.get_by_name(name)
        if job is not None:
            assert job.done_event.wait(10)
        assert client.create_calls == []   # the POST never left the box
        assert "intent" in out or "failed" in out

    def test_reconciler_reports_unresolved_when_listing_truncates(
        self, clean_state, monkeypatch
    ):
        self._stale_intent_entry("Needle session", time.time() - 600)
        client = _FakeRestClient()
        client.list_agents = lambda limit=20: {
            "items": [
                {"id": f"bc-{i}", "name": "Other", "createdAt": "2026-01-01T00:00:00Z"}
                for i in range(limit)
            ],
            "nextCursor": "bc-more",
        }
        monkeypatch.setattr(gc_cloud, "make_client", lambda: client)
        monkeypatch.setattr(gc_supervisor, "ensure_supervisor", lambda name: None)

        gc_supervisor.reconcile_once()
        entry = gc_handles.get("Needle session")
        assert entry["create_intent"]["state"] == "unresolved"
        assert not entry.get("cursor_session_id")
        assert "cursor.com/agents" in str(entry.get("status_note") or "")

    def test_reconciler_settles_failed_when_create_never_landed(
        self, clean_state, monkeypatch
    ):
        self._stale_intent_entry("Ghost create", time.time() - 600)
        client = _FakeRestClient()
        client.list_agents = lambda limit=20: {"items": [
            {"id": "bc-x", "name": "Other", "createdAt": "2026-01-01T00:00:00Z"},
        ]}
        monkeypatch.setattr(gc_cloud, "make_client", lambda: client)
        monkeypatch.setattr(gc_supervisor, "ensure_supervisor", lambda name: None)

        gc_supervisor.reconcile_once()
        entry = gc_handles.get("Ghost create")
        assert entry["create_intent"]["state"] == "failed"
        assert entry["status"] == "failed"


class TestPreflightOrder:
    def test_bad_model_never_spawns_or_leases_a_worker(
        self, tmp_path, monkeypatch
    ):
        client = _FakeRestClient(models=["real-model"])
        _install_fake_rest(monkeypatch, client)
        monkeypatch.setattr(
            gc_workers, "ensure_worker",
            lambda repo: pytest.fail(
                "a failed pure preflight must never spawn a worker"
            ),
        )
        with pytest.raises(gc_cloud.CloudRunnerError) as err:
            _run_cloud_events(tmp_path, model="bogus-model")
        assert "not in the cursor model catalog" in str(err.value)


class TestRunLeases:
    """A local run holds a per-RUN worker lease from dispatch until the
    observed remote-terminal settle — the reaper can never take a worker
    mid-run — and the reaper is actually wired into the reconciler."""

    def test_local_run_holds_lease_until_settled(self, tmp_path, monkeypatch):
        record = gc_workers.WorkerRecord(
            name="lease-w", repo_path=str(tmp_path), pid=4242,
            log_path="/dev/null", started_at=time.time(), verified=True,
            generation="gen-lease-1", state="ready",
        )
        gc_workers._write_record(record)
        seen = {}

        def capture_mid_stream():
            rec = gc_workers._read_record("lease-w")
            seen["leases"] = dict(rec.leases) if rec else None
            return None

        client = _FakeRestClient(streams=[[
            _sse("status", {"status": "RUNNING"}),
            capture_mid_stream,
            _sse("assistant", {"text": "done"}, id="1"),
            _sse("result", {"status": "FINISHED"}, id="2"),
            _sse("done", {}),
        ]])
        _install_fake_rest(monkeypatch, client, worker=record)
        _run_cloud_events(tmp_path)

        assert seen["leases"], "no lease held while the run streamed"
        after = gc_workers._read_record("lease-w")
        assert after.leases == {}, "lease not released after settle"

    def test_supervisor_reconciler_runs_the_worker_reaper(self, monkeypatch, clean_state):
        calls = []
        monkeypatch.setattr(
            gc_workers, "reconcile", lambda: calls.append(1) or []
        )
        gc_supervisor.reconcile_once()
        assert calls, "reconcile_once did not run the worker reaper"

    def test_lease_binds_to_remote_identity_at_the_producer(
        self, tmp_path, monkeypatch
    ):
        record = gc_workers.WorkerRecord(
            name="bind-w", repo_path=str(tmp_path), pid=4242,
            log_path="/dev/null", started_at=time.time(), verified=True,
            generation="gen-bind-1", state="ready",
        )
        gc_workers._write_record(record)
        seen = {}

        def capture():
            rec = gc_workers._read_record("bind-w")
            seen["lease"] = dict(next(iter(rec.leases.values()))) if rec.leases else None
            return None

        client = _FakeRestClient(agent_id="bc-bind", run_id="run-bind")
        client_streams = [[
            _sse("status", {"status": "RUNNING"}),
            capture,
            _sse("result", {"status": "FINISHED"}, id="1"),
            _sse("done", {}),
        ]]
        client.streams = [list(s) for s in client_streams]
        _install_fake_rest(monkeypatch, client, worker=record)
        _run_cloud_events(tmp_path)

        assert seen["lease"] is not None
        assert seen["lease"]["agent_id"] == "bc-bind"
        assert seen["lease"]["run_id"] == "run-bind"

    def test_lease_survives_an_unconfirmed_executor_exit(
        self, tmp_path, monkeypatch
    ):
        """Exhausted stream reconnects with the remote run still RUNNING:
        the local executor dies, but the run may be alive — the lease
        must survive until someone OBSERVES a remote terminal."""
        record = gc_workers.WorkerRecord(
            name="keep-w", repo_path=str(tmp_path), pid=4242,
            log_path="/dev/null", started_at=time.time(), verified=True,
            generation="gen-keep-1", state="ready",
        )
        gc_workers._write_record(record)
        drop = gc_rest.RestNetworkError("stream dropped")
        client = _FakeRestClient(
            streams=[[drop]] * 10,
            statuses=("RUNNING",),   # never terminal — never confirmed
        )
        monkeypatch.setattr(gc_cloud, "MAX_STREAM_REATTACHES", 1)
        monkeypatch.setattr(gc_cloud, "_REATTACH_BACKOFF_S", 0.01)
        _install_fake_rest(monkeypatch, client, worker=record)
        events = _run_cloud_events(tmp_path)

        assert any(k == "cloud.error" for k, _ in events)
        after = gc_workers._read_record("keep-w")
        assert after.leases, "lease must survive an unconfirmed executor exit"

    def test_supervisor_settle_releases_the_sessions_leases(
        self, clean_state, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(gc_workers, "_pid_alive", lambda pid: True)
        record = gc_workers.WorkerRecord(
            name="settle-w", repo_path=str(tmp_path), pid=4242,
            log_path="/dev/null", started_at=time.time(), verified=True,
            generation="gen-settle-1", state="ready",
        )
        gc_workers._write_record(record)
        gc_workers.acquire_lease(record.name, "run-s", session="Settle me")
        gc_workers.bind_lease(record.name, "run-s", "bc-s", "run-s-1")
        gc_handles.record(
            "Settle me", repo=str(tmp_path), status="running",
            cursor_session_id="bc-s", session_key="",
        )
        gc_handles.record_supervision(
            "Settle me", phase="streaming",
            current_attempt_id="att-x", attempt_n=1,
        )
        sup = gc_supervisor.SessionSupervisor("Settle me")
        sup._settle("completed")

        assert gc_workers._read_record(record.name).leases == {}

    def test_reconciler_probes_and_releases_settled_bound_leases(
        self, clean_state, monkeypatch, tmp_path
    ):
        """A bound lease with no live supervision anywhere (crashed
        dispatcher, settled remote run) is released by the reconciler's
        authoritative GET probe."""
        monkeypatch.setattr(gc_workers, "_pid_alive", lambda pid: True)
        record = gc_workers.WorkerRecord(
            name="probe-w", repo_path=str(tmp_path), pid=4242,
            log_path="/dev/null", started_at=time.time(), verified=True,
            generation="gen-probe-1", state="ready",
        )
        gc_workers._write_record(record)
        gc_workers.acquire_lease(record.name, "run-p", session="Probed session")
        gc_workers.bind_lease(record.name, "run-p", "bc-p", "run-p-1")
        client = _FakeRestClient(statuses=("FINISHED",))
        monkeypatch.setattr(gc_cloud, "make_client", lambda: client)

        gc_supervisor.reconcile_once()
        assert gc_workers._read_record(record.name).leases == {}


class TestSupervisorReattach:
    def test_reconciler_reattaches_ingests_and_settles_from_the_get(
        self, clean_state, monkeypatch
    ):
        name = _seed_reattachable()
        client = _FakeRestClient(
            streams=[_happy_stream()], statuses=("FINISHED",), run_id="run-9"
        )
        _install_supervisor_client(monkeypatch, client)

        attached = gc_supervisor.reconcile_once()
        assert attached == [name]
        _wait_settled(name, "completed")

        # Settlement came from the GET authority, not the replay.
        assert client.get_run_calls >= 1
        sup = gc_handles.supervision_of(gc_handles.get(name))
        assert sup["phase"] == "completed"

        # Ingested events landed in the jsonl with the attempt identity.
        lines = _log_lines(name)
        kinds = [l["kind"] for l in lines]
        assert "content" in kinds and "tool_result" in kinds
        assert all(l.get("attemptId") == "att-prior" for l in lines)
        trail = [l.get("event") for l in lines if l["kind"] == "lifecycle"]
        assert "supervisor.reattached" in trail
        assert "durable_progress" in trail  # derived from the completed tool
        assert "session.settled" in trail

        # Exactly one completion, delivered to the dispatching session.
        completions = _completion_events(_drain_completion_queue())
        assert len(completions) == 1
        evt = completions[0]
        assert evt["status"] == "completed"
        assert evt["session_key"] == ""
        assert evt["delegation_id"] == name
        assert "all done" in evt["summary"]

    def test_replayed_finished_never_beats_the_gets_cancelled(
        self, clean_state, monkeypatch
    ):
        """Terminal precedence (RFC §2): the replay of a cancelled run says
        FINISHED; the GET says CANCELLED; the GET wins."""
        name = _seed_reattachable(name="lost-cancelled")
        client = _FakeRestClient(
            streams=[_happy_stream()], statuses=("CANCELLED",), run_id="run-9"
        )
        _install_supervisor_client(monkeypatch, client)

        gc_supervisor.reconcile_once()
        _wait_settled(name, "cancelled")
        completions = _completion_events(_drain_completion_queue())
        assert len(completions) == 1
        assert completions[0]["status"] == "cancelled"

    def test_replayed_twins_are_deduped_by_provider_event_id(
        self, clean_state, monkeypatch
    ):
        name = _seed_reattachable(name="lost-dupes")
        stream = [
            _sse("assistant", {"text": "once"}, id="e1"),
            _sse("assistant", {"text": "once"}, id="e1"),  # replayed twin
            _sse("assistant", {"text": "twice"}, id="e2"),
        ]
        client = _FakeRestClient(
            streams=[stream], statuses=("FINISHED",), run_id="run-9"
        )
        _install_supervisor_client(monkeypatch, client)
        gc_supervisor.reconcile_once()
        _wait_settled(name, "completed")
        content = [l for l in _log_lines(name) if l["kind"] == "content"]
        assert [c["delta"] for c in content] == ["once", "twice"]

    def test_completion_fans_out_exactly_once_per_subscriber(
        self, clean_state, monkeypatch
    ):
        name = _seed_reattachable(name="lost-fanout")
        gc_handles.set_subscriber(name, "alice", 300.0)
        client = _FakeRestClient(
            streams=[_happy_stream()], statuses=("FINISHED",), run_id="run-9"
        )
        _install_supervisor_client(monkeypatch, client)
        gc_supervisor.reconcile_once()
        _wait_settled(name, "completed")
        completions = _completion_events(_drain_completion_queue())
        assert len(completions) == 2
        by_key = {e["session_key"]: e for e in completions}
        assert set(by_key) == {"", "alice"}
        assert by_key[""]["delegation_id"] == name
        assert by_key["alice"]["delegation_id"] != name  # suffixed copy

        # A second reconcile pass finds nothing live: settled is settled.
        assert gc_supervisor.reconcile_once() == []
        assert _completion_events(_drain_completion_queue()) == []

    def test_orphaned_handle_without_remote_agent_settles_failed(
        self, clean_state, monkeypatch
    ):
        gc_handles.record("lost-orphan", repo="/tmp/r", status="running",
                          session_key="", runtime="local")
        gc_handles.record_supervision("lost-orphan", phase="streaming")
        _install_supervisor_client(
            monkeypatch, _FakeRestClient(statuses=("FINISHED",))
        )
        gc_supervisor.reconcile_once()
        _wait_settled("lost-orphan", "failed")
        entry = gc_handles.get("lost-orphan")
        assert "orphaned" in str(entry.get("status_note") or "")
        completions = _completion_events(_drain_completion_queue())
        assert len(completions) == 1
        assert completions[0]["status"] == "failed"

    def test_reconciler_skips_sessions_with_a_live_in_process_job(
        self, clean_state, monkeypatch, tmp_path
    ):
        release = threading.Event()
        monkeypatch.setattr(
            gc_cloud, "run_cloud", _gated_replay_factory(release, sid="a-live")
        )
        _start_run("t", repo=str(tmp_path))
        job = _job_for("a-live")
        # The handle is in a live supervision phase, but the running job
        # IS its supervision — no re-attach task may be spawned.
        assert gc_handles.supervision_is_live(gc_handles.get(job.session_name))
        assert gc_supervisor.reconcile_once() == []
        assert not gc_supervisor.has_live(job.session_name)
        release.set()
        assert job.done_event.wait(10)

    def test_adopted_legacy_handle_without_agent_id_settles_orphaned(
        self, clean_state, monkeypatch
    ):
        """A pre-supervisor running record is ADOPTED by the reconciler
        (not skipped); with no remote agent id there is nothing to
        re-attach to, so the adopted supervisor settles it honestly."""
        gc_handles.record("s-old", repo="/tmp/r", status="running")
        _install_supervisor_client(
            monkeypatch, _FakeRestClient(statuses=("FINISHED",))
        )
        attached = gc_supervisor.reconcile_once()
        assert attached == ["s-old"]
        _wait_settled("s-old", "failed")
        entry = gc_handles.get("s-old")
        assert "orphaned" in str(entry.get("status_note") or "")
        completions = _completion_events(_drain_completion_queue())
        assert len(completions) == 1
        assert completions[0]["status"] == "failed"

    def test_supervised_running_handle_is_not_declared_dead_at_read_time(
        self, clean_state, monkeypatch
    ):
        """A live-phase handle read before the supervisor settles it stays
        running — the read re-attaches instead of flipping to failed."""
        name = _seed_reattachable(name="lost-read")
        gate = threading.Event()
        stream = [
            _sse("assistant", {"text": "still going"}, id="e1"),
            lambda: (gate.wait(10), None)[1],
        ]
        client = _FakeRestClient(
            streams=[stream], statuses=("RUNNING", "FINISHED"), run_id="run-9"
        )
        _install_supervisor_client(monkeypatch, client)

        out = cursor_status(name)
        assert "orphaned" not in out
        assert (gc_handles.get(name) or {}).get("status") == "running"
        assert _wait_until(lambda: gc_supervisor.has_live(name))
        gate.set()
        _wait_settled(name, "completed")
        _drain_completion_queue()

    def test_digests_resume_from_the_persisted_cursor(
        self, clean_state, monkeypatch
    ):
        name = _seed_reattachable(name="lost-digest")
        gc_handles.set_subscriber(name, "", 0.05)
        # Pre-crash history: 3 events already in the log, 2 delivered.
        for i in range(3):
            gc_eventlog.append(name, {"kind": "content", "delta": f"pre{i}"})
        gc_handles.advance_delivery_cursor(name, "", 2)

        gate = threading.Event()
        stream = [
            _sse("assistant", {"text": "fresh"}, id="e1"),
            lambda: (gate.wait(10), None)[1],
        ]
        client = _FakeRestClient(
            streams=[stream], statuses=("RUNNING", "FINISHED"), run_id="run-9"
        )
        _install_supervisor_client(monkeypatch, client)
        gc_supervisor.reconcile_once()

        # A digest arrives while the run is still live, covering
        # everything after the persisted cursor.
        digests = []
        assert _wait_until(
            lambda: any(
                e.get("cursor_progress_update")
                for e in (digests.extend(_drain_completion_queue()) or digests)
            ),
            timeout=10.0,
        ), f"no digest delivered; got {digests}"
        digest = next(e for e in digests if e.get("cursor_progress_update"))
        assert digest["status"] == "running"
        assert "progress update 1" in digest["summary"]

        # The cursor advanced to the log total at delivery time (the write
        # lands just after the enqueue — poll for it) — a later re-attach
        # would resume from here, not from zero.
        assert _wait_until(
            lambda: gc_handles.supervision_of(
                gc_handles.get(name)
            )["last_seq_delivered"].get("", 0) > 2
        )
        total = gc_eventlog.stats(name)["total_events"]
        assert gc_handles.supervision_of(
            gc_handles.get(name)
        )["last_seq_delivered"][""] <= total

        gate.set()
        _wait_settled(name, "completed")
        _drain_completion_queue()

    def test_cursor_stop_requests_the_transition_and_the_supervisor_settles(
        self, clean_state, monkeypatch
    ):
        name = _seed_reattachable(name="lost-stop")
        client = _FakeRestClient(streams=[[
            _sse("assistant", {"text": "grinding"}, id="e1"),
        ]], statuses=("CANCELLED",), run_id="run-9")
        # Keep the stream open until the fake observes the cancel.
        client.streams[0].append(lambda: (client.cancelled.wait(10), None)[1])
        _install_supervisor_client(monkeypatch, client)
        gc_supervisor.reconcile_once()
        assert _wait_until(lambda: gc_supervisor.has_live(name))

        out = cursor_stop(name)
        assert "cancelled" in out
        assert client.cancel_calls == [("bc-lost", "run-9")]
        assert (gc_handles.get(name) or {}).get("status") == "cancelled"
        trail = [l.get("event") for l in _log_lines(name)
                 if l.get("kind") == "lifecycle"]
        assert "interrupt_requested" in trail
        assert "interrupted" in trail
        _drain_completion_queue()

    def test_crashed_supervisor_leaves_the_phase_live_for_the_next_pass(
        self, clean_state, monkeypatch
    ):
        """An attach that cannot even build a client must NOT settle the
        session — supervision stays live and the next pass retries."""
        name = _seed_reattachable(name="lost-nokey")

        def _boom():
            raise gc_cloud.CloudRunnerError("CURSOR_API_KEY is not set")

        monkeypatch.setattr(gc_cloud, "make_client", _boom)
        attached = gc_supervisor.reconcile_once()
        assert attached == [name]
        assert _wait_until(lambda: not gc_supervisor.has_live(name))
        assert (gc_handles.get(name) or {}).get("status") == "running"
        assert gc_handles.supervision_is_live(gc_handles.get(name))
        assert _completion_events(_drain_completion_queue()) == []


# ---------------------------------------------------------------------------
# Reconciler adoption of legacy/pre-supervisor handles + false-settle repair
# (incident: gateway restart left two healthy pre-supervisor cloud runs
# un-adopted, and a bounced follow-up send falsely settled one as failed)
# ---------------------------------------------------------------------------

def _seed_legacy_running(name="legacy-run", agent="bc-legacy", run=None,
                         supervision=None, repo="/tmp/r"):
    """A pre-supervisor handle: top-level status running, NO (or empty)
    supervision record — exactly what predates the supervisor deploy."""
    fields = dict(
        repo=repo, status="running", task="keep going", session_key="",
        cursor_session_id=agent, runtime="local",
    )
    if run:
        fields["latest_run_id"] = run
    if supervision is not None:
        fields["supervision"] = supervision
    gc_handles.record(name, **fields)
    return name


class TestReconcilerLegacyAdoption:
    def test_adopts_running_handle_with_no_supervision_record(
        self, clean_state, monkeypatch
    ):
        """A running handle with NO supervision record is adopted: record
        seeded (live phase, fresh attempt id), supervisor spawned, digests
        flow, and the run settles from the GET authority."""
        name = _seed_legacy_running()  # no latest_run_id: list_runs resolves
        gc_handles.set_subscriber(name, "", 0.05)
        gate = threading.Event()
        stream = [
            _sse("assistant", {"text": "still going"}, id="e1"),
            lambda: (gate.wait(10), None)[1],
        ]
        client = _FakeRestClient(
            streams=[stream], statuses=("RUNNING", "FINISHED"), run_id="run-9"
        )
        _install_supervisor_client(monkeypatch, client)

        attached = gc_supervisor.reconcile_once()
        assert attached == [name]

        # The supervision record was seeded: live phase, fresh attempt id.
        sup = gc_handles.supervision_of(gc_handles.get(name))
        assert sup["phase"] == "streaming"
        assert sup["current_attempt_id"].startswith("att-")
        assert sup["attempt_n"] == 1
        assert sup["last_seq_delivered"] == {}
        assert _wait_until(lambda: gc_supervisor.has_live(name))

        # Digests flow while the adopted run is live.
        digests = []
        assert _wait_until(
            lambda: any(
                e.get("cursor_progress_update")
                for e in (digests.extend(_drain_completion_queue()) or digests)
            ),
            timeout=10.0,
        ), f"no digest delivered; got {digests}"

        gate.set()
        _wait_settled(name, "completed")
        trail = [l.get("event") for l in _log_lines(name)
                 if l.get("kind") == "lifecycle"]
        assert "supervision.adopted" in trail
        assert "session.settled" in trail
        _drain_completion_queue()

    def test_adopts_running_handle_with_legacy_empty_phase_dict(
        self, clean_state, monkeypatch
    ):
        """Same adoption for a handle carrying a legacy supervision dict
        with an EMPTY phase (supervision: null / {} both read as '')."""
        name = _seed_legacy_running(
            name="legacy-empty-phase", run="run-9",
            supervision={"phase": "", "last_seq_delivered": {}},
        )
        client = _FakeRestClient(
            streams=[_happy_stream()], statuses=("FINISHED",), run_id="run-9"
        )
        _install_supervisor_client(monkeypatch, client)

        attached = gc_supervisor.reconcile_once()
        assert attached == [name]
        _wait_settled(name, "completed")
        sup = gc_handles.supervision_of(gc_handles.get(name))
        assert sup["phase"] == "completed"
        assert sup["current_attempt_id"].startswith("att-")
        completions = _completion_events(_drain_completion_queue())
        assert len(completions) == 1
        assert completions[0]["status"] == "completed"

    def test_adopted_handle_with_terminal_remote_settles_exactly_once(
        self, clean_state, monkeypatch
    ):
        """An adopted handle whose remote GET is already terminal settles
        ONCE with the real terminal status (the GET authority — the
        replayed stream's FINISHED never beats it)."""
        name = _seed_legacy_running(name="legacy-done", run="run-9")
        client = _FakeRestClient(
            streams=[_happy_stream()], statuses=("CANCELLED",), run_id="run-9"
        )
        _install_supervisor_client(monkeypatch, client)

        attached = gc_supervisor.reconcile_once()
        assert attached == [name]
        _wait_settled(name, "cancelled")
        completions = _completion_events(_drain_completion_queue())
        assert len(completions) == 1
        assert completions[0]["status"] == "cancelled"

        # Settled is settled: a second pass adopts/attaches nothing and
        # delivers nothing (exactly-once completion).
        assert gc_supervisor.reconcile_once() == []
        assert _completion_events(_drain_completion_queue()) == []
        settled = [l for l in _log_lines(name)
                   if l.get("kind") == "lifecycle"
                   and l.get("event") == "session.settled"]
        assert len(settled) == 1

    def test_reconciler_never_adopts_terminal_or_created_handles(
        self, clean_state, monkeypatch
    ):
        """Adoption is for RUNNING handles only: settled records and lazy
        never-run sessions stay untouched."""
        gc_handles.record("legacy-finished", repo="/tmp/r", status="completed",
                          cursor_session_id="bc-f", runtime="local")
        # Outside the false-settle repair window → not probed either.
        with gc_handles._lock:
            gc_handles._table["legacy-finished"]["updated_at"] = (
                time.time() - 2 * gc_supervisor.FALSE_SETTLE_REPAIR_WINDOW_S
            )
        gc_handles.record("lazy-created", repo="/tmp/r", status="created",
                          runtime="local")
        _install_supervisor_client(
            monkeypatch, _FakeRestClient(statuses=("RUNNING",))
        )
        assert gc_supervisor.reconcile_once() == []
        assert gc_handles.get("legacy-finished")["status"] == "completed"
        assert gc_handles.get("lazy-created")["status"] == "created"
        assert not gc_handles.supervision_of(
            gc_handles.get("legacy-finished")
        )["phase"]


class TestSendBounceDoesNotSettle:
    def test_followup_409_agent_busy_keeps_the_handle_running(
        self, clean_state, monkeypatch, tmp_path
    ):
        """A follow-up send bounced by 409 agent_busy (= the remote run is
        ACTIVE) must not settle the session failed: the handle stays
        running and the caller gets an honest 'still active' message."""
        name = _seed_legacy_running(
            name="busy-run", agent="bc-busy", repo=str(tmp_path)
        )
        client = _FakeRestClient(followup_error=gc_rest.RestApiError(
            "cursor api POST /v1/agents/bc-busy/followup -> 409 "
            "agent_busy: agent has an active run",
            status_code=409, code="agent_busy",
        ))
        _install_fake_rest(monkeypatch, client)

        out = cursor_send_message(name, "one more thing")

        # Honest surface: the run is still active, NOT failed.
        assert "active" in out
        assert "NOT failed" in out
        # The follow-up was attempted on the recorded agent (no silent fork).
        assert client.followup_calls == [
            {"agent_id": "bc-busy", "prompt": "one more thing"}
        ]
        assert client.create_calls == []
        # No false settlement anywhere: handle running, supervision
        # non-terminal, nothing delivered.
        entry = gc_handles.get(name)
        assert entry["status"] == "running"
        assert gc_handles.supervision_of(entry)["phase"] not in (
            gc_handles.SUPERVISION_TERMINAL_PHASES
        )
        assert _completion_events(_drain_completion_queue()) == []

    def test_fresh_session_preflight_failure_still_settles(
        self, clean_state, monkeypatch, tmp_path
    ):
        """The guard is scoped to already-running handles: a FIRST send
        whose create bounces still settles the (never-run) session."""
        client = _FakeRestClient(create_error=gc_rest.RestApiError(
            "cursor api POST /v1/agents -> 400 invalid_model: bad model",
            status_code=400, code="invalid_model",
        ))
        _install_fake_rest(monkeypatch, client)
        name = _created_name(cursor_create_session(repo=str(tmp_path)))
        out = cursor_send_message(name, "go")
        assert "failed" in out
        assert gc_handles.get(name)["status"] == "failed"


class TestFalseSettleRepair:
    def test_local_failed_remote_running_is_unsettled_and_reattached(
        self, clean_state, monkeypatch
    ):
        """Remote authority wins in BOTH directions: a handle falsely
        settled failed while the remote run is RUNNING is un-settled
        (running/streaming), re-attached, and supervised to its real end."""
        name = "falsely-failed"
        gc_handles.record(
            name, repo="/tmp/r", status="failed",
            status_note="cursor agent create failed (... 409 agent_busy ...)",
            task="t", session_key="", cursor_session_id="bc-lost",
            latest_run_id="run-9", runtime="local",
        )
        gc_handles.record_supervision(
            name, phase="failed", current_attempt_id="att-prior", attempt_n=1
        )
        gate = threading.Event()
        stream = [
            _sse("assistant", {"text": "never stopped"}, id="e1"),
            lambda: (gate.wait(10), None)[1],
        ]
        client = _FakeRestClient(
            streams=[stream], statuses=("RUNNING", "FINISHED"), run_id="run-9"
        )
        _install_supervisor_client(monkeypatch, client)

        attached = gc_supervisor.reconcile_once()
        assert attached == [name]

        # Un-settled: running again, live supervision phase, repair event.
        entry = gc_handles.get(name)
        assert entry["status"] == "running"
        assert gc_handles.supervision_of(entry)["phase"] == "streaming"
        unsettled = [l for l in _log_lines(name)
                     if l.get("kind") == "lifecycle"
                     and l.get("event") == "session.unsettled"]
        assert len(unsettled) == 1
        assert unsettled[0]["was"] == "failed"
        assert unsettled[0]["remote_status"] == "RUNNING"
        assert _wait_until(lambda: gc_supervisor.has_live(name))

        # ...and supervised to its REAL terminal state.
        gate.set()
        _wait_settled(name, "completed")
        completions = _completion_events(_drain_completion_queue())
        assert len(completions) == 1
        assert completions[0]["status"] == "completed"

    def test_terminal_local_with_terminal_remote_stays_settled(
        self, clean_state, monkeypatch
    ):
        """The repair only fires on a live remote: a genuinely-finished
        run's terminal record is left alone (no un-settle churn)."""
        gc_handles.record(
            "really-done", repo="/tmp/r", status="completed", task="t",
            session_key="", cursor_session_id="bc-done",
            latest_run_id="run-9", runtime="local",
        )
        client = _FakeRestClient(statuses=("FINISHED",), run_id="run-9")
        _install_supervisor_client(monkeypatch, client)
        assert gc_supervisor.reconcile_once() == []
        assert client.get_run_calls == 1  # probed once, left settled
        assert gc_handles.get("really-done")["status"] == "completed"
        assert _completion_events(_drain_completion_queue()) == []
