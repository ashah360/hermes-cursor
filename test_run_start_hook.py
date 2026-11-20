"""Tests for the optional ``plugins.ghost_cursor.on_run_start`` hook: the
config read, the argv substitution + launch, the ``on_run_start_users``
filter with the requesting user captured at dispatch, and the
``cloud.session`` call site in ``_run_attempt``. The launcher
(``subprocess.Popen``) is faked everywhere except the one test that proves
the reap path."""

import contextvars
import logging
import sys
import threading
import types

import pytest

import plugins.ghost_cursor as gc
from plugins.ghost_cursor import cloud_runner as gc_cloud
from plugins.ghost_cursor import eventlog as gc_eventlog
from plugins.ghost_cursor import handles as gc_handles
from plugins.ghost_cursor import jobs as gc_jobs
from plugins.ghost_cursor import supervisor as gc_supervisor

_UNSET = object()

EXAMPLE = [
    "rex", "window", "new", "-s", "work", "{session}", "--focus=false",
    "--", "cursor-tui", "run", "{agent_id}",
]


@pytest.fixture
def clean_state(monkeypatch):
    gc_supervisor._reset_for_tests()
    monkeypatch.setattr(gc_handles, "_table", {})
    monkeypatch.setattr(gc_handles, "_loaded", False)
    gc_eventlog._reset_for_tests()
    yield
    gc_supervisor._reset_for_tests()
    gc_eventlog._reset_for_tests()


@pytest.fixture(autouse=True)
def raw_config(monkeypatch):
    """The dict ``hermes_cli.config.read_raw_config()`` yields. In a Hermes
    tree only that reader is patched; standalone, where Hermes core is not
    importable, a stand-in ``hermes_cli.config`` is installed."""
    raw = {}
    try:
        import hermes_cli.config as module
    except ImportError:

        def cfg_get(cfg, *keys, default=None):
            for key in keys:
                if not isinstance(cfg, dict) or key not in cfg:
                    return default
                cfg = cfg[key]
            return cfg

        module = types.ModuleType("hermes_cli.config")
        module.cfg_get = cfg_get
        package = types.ModuleType("hermes_cli")
        package.config = module
        monkeypatch.setitem(sys.modules, "hermes_cli", package)
        monkeypatch.setitem(sys.modules, "hermes_cli.config", module)
    monkeypatch.setattr(module, "read_raw_config", lambda: raw, raising=False)
    return raw


class _FakeProc:
    def wait(self):
        return 0


@pytest.fixture
def popen_calls(monkeypatch):
    """Replace ``subprocess.Popen`` with a recorder of (argv, kwargs)."""
    calls = []

    def fake_popen(argv, **kwargs):
        calls.append((argv, kwargs))
        return _FakeProc()

    monkeypatch.setattr(gc.subprocess, "Popen", fake_popen)
    return calls


@pytest.fixture
def session_user(monkeypatch):
    """Stand in for ``gateway.session_context``: the returned ContextVar
    backs ``get_session_env("HERMES_SESSION_USER_ID")``."""
    var = contextvars.ContextVar("HERMES_SESSION_USER_ID", default="")

    def get_session_env(name, default=""):
        assert name == "HERMES_SESSION_USER_ID"
        return var.get() or default

    try:
        import gateway  # noqa: F401
    except ImportError:
        monkeypatch.setitem(sys.modules, "gateway", types.ModuleType("gateway"))
    module = types.ModuleType("gateway.session_context")
    module.get_session_env = get_session_env
    monkeypatch.setitem(sys.modules, "gateway.session_context", module)
    return var


def _job(name="Fix webhook retries", user_id=""):
    return gc_jobs.CursorJob(
        job_id="job-1",
        task="do the thing",
        repo="/srv/repo",
        inactivity_timeout_s=600.0,
        session_name=name,
        user_id=user_id,
        cursor_session_id="bc-123",
        latest_run_id="run-9",
        agents_ui_url="https://cursor.com/agents/bc-123",
    )


def _sessions(*run_ids):
    """A fake ``run_cloud`` emitting one ``cloud.session`` per run id."""

    def replay(task, workdir, **_kw):
        for run_id in run_ids:
            yield ("cloud.session", {
                "agentId": "bc-123",
                "run_id": run_id,
                "cwd": str(workdir),
                "model": "fake-model",
                "agents_ui_url": "https://cursor.com/agents/bc-123",
            })
        yield ("cloud.result", {"status": "finished"})

    return replay


class TestConfiguredOnRunStart:
    def test_reads_the_configured_argv(self, raw_config):
        raw_config["plugins"] = {"ghost_cursor": {"on_run_start": EXAMPLE}}
        assert gc._configured_on_run_start() == EXAMPLE

    @pytest.mark.parametrize("plugins", [
        None,
        {},
        {"ghost_cursor": {}},
        {"ghost_cursor": {"on_run_start": None}},
        {"ghost_cursor": {"on_run_start": []}},
        {"ghost_cursor": {"on_run_start": ""}},
    ])
    def test_off_when_unset_or_empty(self, raw_config, caplog, plugins):
        if plugins is not None:
            raw_config["plugins"] = plugins
        with caplog.at_level(logging.WARNING):
            assert gc._configured_on_run_start() is None
        assert caplog.records == []

    @pytest.mark.parametrize("value", [
        "rex window new",
        ["rex", 5],
        ["rex", None],
        [["rex"]],
        {"cmd": "rex"},
        7,
    ])
    def test_off_with_a_warning_when_malformed(self, raw_config, caplog, value):
        raw_config["plugins"] = {"ghost_cursor": {"on_run_start": value}}
        with caplog.at_level(logging.WARNING):
            assert gc._configured_on_run_start() is None
        assert "on_run_start" in caplog.text

    def test_off_when_the_config_cannot_be_read(self, raw_config, monkeypatch):
        def boom():
            raise OSError("unreadable")

        monkeypatch.setattr(
            sys.modules["hermes_cli.config"], "read_raw_config", boom
        )
        assert gc._configured_on_run_start() is None


class TestLaunchRunStartHook:
    def test_substitutes_placeholders_per_element(self, monkeypatch, popen_calls):
        monkeypatch.setattr(gc, "_configured_on_run_start", lambda: [
            "open", "{agent_id}", "{run_id}", "{session}", "{repo}", "{url}",
            "{user_id}", "--title={session} ({agent_id}/{run_id})",
        ])
        gc._launch_run_start_hook(_job(user_id="U123"), "/srv/repo")
        (argv, _kwargs), = popen_calls
        assert argv == [
            "open", "bc-123", "run-9", "Fix webhook retries", "/srv/repo",
            "https://cursor.com/agents/bc-123", "U123",
            "--title=Fix webhook retries (bc-123/run-9)",
        ]

    def test_user_id_placeholder_is_empty_without_a_requesting_user(
        self, monkeypatch, popen_calls
    ):
        monkeypatch.setattr(
            gc, "_configured_on_run_start", lambda: ["open", "--user={user_id}"]
        )
        gc._launch_run_start_hook(_job(), "/srv/repo")
        (argv, _kwargs), = popen_calls
        assert argv == ["open", "--user="]

    def test_readme_example(self, monkeypatch, popen_calls):
        monkeypatch.setattr(gc, "_configured_on_run_start", lambda: EXAMPLE)
        gc._launch_run_start_hook(_job(), "/srv/repo")
        (argv, _kwargs), = popen_calls
        assert argv == [
            "rex", "window", "new", "-s", "work", "Fix webhook retries",
            "--focus=false", "--", "cursor-tui", "run", "bc-123",
        ]

    def test_stray_and_unknown_braces_pass_through(self, monkeypatch, popen_calls):
        template = [
            "sh-like{", "}", "{", "{}", "{unknown}", "{0}", "{{agent_id}}",
            "{agent_id", "awk '{print $1}'", "{run_id}}{",
        ]
        monkeypatch.setattr(gc, "_configured_on_run_start", lambda: template)
        gc._launch_run_start_hook(_job(), "/srv/repo")
        (argv, _kwargs), = popen_calls
        assert argv == [
            "sh-like{", "}", "{", "{}", "{unknown}", "{0}", "{bc-123}",
            "{agent_id", "awk '{print $1}'", "run-9}{",
        ]

    def test_substituted_values_are_not_expanded_again(
        self, monkeypatch, popen_calls
    ):
        monkeypatch.setattr(
            gc, "_configured_on_run_start", lambda: ["open", "{session}"]
        )
        gc._launch_run_start_hook(_job(name="Document {url} and \\1"), "/srv/repo")
        (argv, _kwargs), = popen_calls
        assert argv == ["open", "Document {url} and \\1"]

    def test_launches_detached_without_a_shell(self, monkeypatch, popen_calls):
        monkeypatch.setattr(gc, "_configured_on_run_start", lambda: ["open"])
        monkeypatch.setenv("PYTHONPATH", "/gateway/venv")
        gc._launch_run_start_hook(_job(), "/srv/repo")
        (_argv, kwargs), = popen_calls
        assert kwargs["stdin"] is gc.subprocess.DEVNULL
        assert kwargs["stdout"] is gc.subprocess.DEVNULL
        assert kwargs["stderr"] is gc.subprocess.DEVNULL
        assert kwargs["start_new_session"] is True
        assert not kwargs.get("shell")
        assert "PATH" in kwargs["env"]
        assert "PYTHONPATH" not in kwargs["env"]

    def test_off_launches_nothing(self, monkeypatch, popen_calls):
        monkeypatch.setattr(gc, "_configured_on_run_start", lambda: None)
        gc._launch_run_start_hook(_job(), "/srv/repo")
        assert popen_calls == []

    def test_launcher_that_raises_is_logged_and_swallowed(
        self, monkeypatch, caplog
    ):
        def boom(argv, **kwargs):
            raise FileNotFoundError(2, "No such file or directory", argv[0])

        monkeypatch.setattr(gc, "_configured_on_run_start", lambda: ["nope"])
        monkeypatch.setattr(gc.subprocess, "Popen", boom)
        with caplog.at_level(logging.WARNING):
            assert gc._launch_run_start_hook(_job(), "/srv/repo") is None
        (record,) = caplog.records
        assert record.levelno == logging.WARNING
        assert "on_run_start" in record.getMessage()

    def test_real_child_is_reaped_and_nonzero_exit_logged(
        self, monkeypatch, caplog
    ):
        spawned = []
        real_popen = gc.subprocess.Popen

        def recording_popen(argv, **kwargs):
            proc = real_popen(argv, **kwargs)
            spawned.append(proc)
            return proc

        monkeypatch.setattr(gc, "_configured_on_run_start", lambda: [
            sys.executable, "-c", "import sys; sys.exit(3)",
        ])
        monkeypatch.setattr(gc.subprocess, "Popen", recording_popen)
        with caplog.at_level(logging.WARNING):
            gc._launch_run_start_hook(_job(), "/srv/repo")
            for thread in threading.enumerate():
                if thread.name == "ghost-cursor-on-run-start":
                    thread.join(30)
        (proc,) = spawned
        assert proc.returncode == 3
        assert "exited with code 3" in caplog.text


class TestRunStartUsers:
    def _fired(self, raw_config, popen_calls, users, user_id):
        section = {"on_run_start": ["open", "{user_id}"]}
        if users is not _UNSET:
            section["on_run_start_users"] = users
        raw_config["plugins"] = {"ghost_cursor": section}
        del popen_calls[:]
        gc._launch_run_start_hook(_job(user_id=user_id), "/srv/repo")
        return len(popen_calls)

    @pytest.mark.parametrize("users", [_UNSET, None, []])
    @pytest.mark.parametrize("user_id", ["U123", "U999", ""])
    def test_unset_or_empty_fires_for_everyone(
        self, raw_config, popen_calls, caplog, users, user_id
    ):
        with caplog.at_level(logging.WARNING):
            assert self._fired(raw_config, popen_calls, users, user_id) == 1
        assert caplog.records == []

    def test_set_and_matching_fires(self, raw_config, popen_calls):
        assert self._fired(raw_config, popen_calls, ["U123"], "U123") == 1
        assert self._fired(raw_config, popen_calls, ["U999", "U123"], "U123") == 1

    def test_set_and_not_matching_does_not_fire(
        self, raw_config, popen_calls, caplog
    ):
        with caplog.at_level(logging.WARNING):
            assert self._fired(raw_config, popen_calls, ["U123"], "U999") == 0
            assert self._fired(raw_config, popen_calls, ["U123"], "U12") == 0
            assert self._fired(raw_config, popen_calls, ["U123"], "u123") == 0
        assert caplog.records == []

    def test_no_requesting_user_matches_local_and_nothing_else(
        self, raw_config, popen_calls
    ):
        assert self._fired(raw_config, popen_calls, ["local"], "") == 1
        assert self._fired(raw_config, popen_calls, ["U123", "local"], "") == 1
        assert self._fired(raw_config, popen_calls, ["U123", "local"], "U123") == 1
        assert self._fired(raw_config, popen_calls, ["local"], "U123") == 0
        assert self._fired(raw_config, popen_calls, ["U123"], "") == 0
        assert self._fired(raw_config, popen_calls, [""], "") == 0
        assert self._fired(raw_config, popen_calls, ["", "U123"], "") == 0

    @pytest.mark.parametrize("users", [
        "U123",
        "local",
        ["U123", 5],
        [123456789],
        ["U123", None],
        [["U123"]],
        {"U123": True},
        "",
        0,
        False,
        True,
    ])
    @pytest.mark.parametrize("user_id", ["U123", "U999", ""])
    def test_malformed_disables_the_hook_with_a_warning(
        self, raw_config, popen_calls, caplog, users, user_id
    ):
        with caplog.at_level(logging.WARNING):
            assert self._fired(raw_config, popen_calls, users, user_id) == 0
        (record,) = caplog.records
        assert record.levelno == logging.WARNING
        assert "on_run_start_users" in record.getMessage()

    def test_unreadable_users_disables_the_hook_with_a_warning(
        self, monkeypatch, popen_calls, caplog
    ):
        def boom():
            raise OSError("unreadable")

        monkeypatch.setattr(gc, "_configured_on_run_start", lambda: ["open"])
        monkeypatch.setattr(
            sys.modules["hermes_cli.config"], "read_raw_config", boom
        )
        with caplog.at_level(logging.WARNING):
            gc._launch_run_start_hook(_job(user_id="U123"), "/srv/repo")
        assert popen_calls == []
        assert "on_run_start_users" in caplog.text

    def test_users_are_not_consulted_when_the_hook_is_off(
        self, raw_config, popen_calls, caplog
    ):
        raw_config["plugins"] = {"ghost_cursor": {"on_run_start_users": "oops"}}
        with caplog.at_level(logging.WARNING):
            gc._launch_run_start_hook(_job(user_id="U123"), "/srv/repo")
        assert popen_calls == []
        assert caplog.records == []


class TestRequestingUser:
    def test_reads_the_gateway_user_id(self, session_user):
        assert gc._resolve_session_user_id() == ""
        session_user.set("U123")
        assert gc._resolve_session_user_id() == "U123"

    def test_empty_when_the_gateway_context_is_unavailable(
        self, session_user, monkeypatch
    ):
        def boom(name, default=""):
            raise RuntimeError("no session context")

        session_user.set("U123")
        monkeypatch.setattr(
            sys.modules["gateway.session_context"], "get_session_env", boom
        )
        assert gc._resolve_session_user_id() == ""

    def test_dispatch_captures_the_user_on_the_calling_thread(
        self, clean_state, session_user, raw_config, popen_calls, monkeypatch,
        tmp_path,
    ):
        seen = {}

        def runner(job):
            seen["thread"] = threading.current_thread()
            gc._run_attempt(job, job.repo)
            return {"success": True, "session_id": job.cursor_session_id}

        raw_config["plugins"] = {"ghost_cursor": {
            "on_run_start": ["open", "{user_id}"],
            "on_run_start_users": ["U123"],
        }}
        monkeypatch.setattr(gc, "_execute_cursor_run", runner)
        monkeypatch.setattr(gc, "_await_handle", lambda job: {})
        monkeypatch.setattr(gc_cloud, "run_cloud", _sessions("run-1"))
        session_user.set("U123")
        gc_jobs.registry._reset_for_tests()
        try:
            gc._dispatch_run(
                "do the thing", str(tmp_path), "Fix webhook retries", None,
                None, 600.0, 0.0, runtime="cloud",
            )
            (job,) = gc_jobs.registry.list_jobs()
            assert job.done_event.wait(30)
        finally:
            gc_jobs.registry._reset_for_tests()
        assert seen["thread"] is not threading.current_thread()
        assert job.user_id == "U123"
        assert [argv for argv, _kwargs in popen_calls] == [["open", "U123"]]


class TestRunStartCallSite:
    def _run(self, monkeypatch, job, *run_ids):
        monkeypatch.setattr(gc_cloud, "run_cloud", _sessions(*run_ids))
        return gc._run_attempt(job, "/srv/repo")

    def _fresh_job(self):
        return gc_jobs.CursorJob(
            job_id="job-1",
            task="do the thing",
            repo="/srv/repo",
            inactivity_timeout_s=600.0,
            session_name="Fix webhook retries",
        )

    def test_fires_once_per_run_id(self, clean_state, monkeypatch):
        fired = []
        monkeypatch.setattr(
            gc, "_launch_run_start_hook",
            lambda job, workdir: fired.append(
                (job.cursor_session_id, job.latest_run_id, workdir)
            ),
        )
        job = self._fresh_job()
        self._run(monkeypatch, job, "run-1", "run-1")
        assert fired == [("bc-123", "run-1", "/srv/repo")]

        self._run(monkeypatch, job, "run-1")
        assert fired == [("bc-123", "run-1", "/srv/repo")]

        self._run(monkeypatch, job, "run-2")
        assert fired == [
            ("bc-123", "run-1", "/srv/repo"),
            ("bc-123", "run-2", "/srv/repo"),
        ]

    def test_a_later_job_in_the_session_fires_again(
        self, clean_state, monkeypatch
    ):
        fired = []
        monkeypatch.setattr(
            gc, "_launch_run_start_hook",
            lambda job, workdir: fired.append(job.latest_run_id),
        )
        self._run(monkeypatch, self._fresh_job(), "run-1")
        self._run(monkeypatch, self._fresh_job(), "run-2")
        assert fired == ["run-1", "run-2"]

    def test_fires_after_the_handle_is_persisted(self, clean_state, monkeypatch):
        seen = []

        def record(job, workdir):
            entry = gc_handles.get(job.session_name) or {}
            seen.append((
                entry.get("cursor_session_id"),
                entry.get("latest_run_id"),
                job.session_event.is_set(),
            ))

        monkeypatch.setattr(gc, "_launch_run_start_hook", record)
        self._run(monkeypatch, self._fresh_job(), "run-1")
        assert seen == [("bc-123", "run-1", True)]

    def test_substituted_argv_reaches_the_launcher(
        self, clean_state, monkeypatch, popen_calls
    ):
        monkeypatch.setattr(gc, "_configured_on_run_start", lambda: EXAMPLE)
        self._run(monkeypatch, self._fresh_job(), "run-1")
        (argv, _kwargs), = popen_calls
        assert argv == [
            "rex", "window", "new", "-s", "work", "Fix webhook retries",
            "--focus=false", "--", "cursor-tui", "run", "bc-123",
        ]

    def test_unconfigured_run_launches_nothing(
        self, clean_state, monkeypatch, raw_config, popen_calls
    ):
        job = self._fresh_job()
        state = self._run(monkeypatch, job, "run-1")
        assert popen_calls == []
        assert state["preflight"] is None
        assert job.run_error is None

    def test_launcher_that_raises_does_not_affect_the_run(
        self, clean_state, monkeypatch, caplog
    ):
        def boom(argv, **kwargs):
            raise FileNotFoundError(2, "No such file or directory", argv[0])

        monkeypatch.setattr(gc, "_configured_on_run_start", lambda: ["nope"])
        monkeypatch.setattr(gc.subprocess, "Popen", boom)
        job = self._fresh_job()
        with caplog.at_level(logging.WARNING):
            state = self._run(monkeypatch, job, "run-1")
        assert state == {
            "preflight": None,
            "terminal_error": False,
            "retryable": None,
            "retry_after": None,
        }
        assert job.run_error is None
        assert job.session_event.is_set()
        assert job.latest_run_id == "run-1"
        assert (gc_handles.get(job.session_name) or {}).get("status") == "running"
        assert "on_run_start hook failed" in caplog.text
