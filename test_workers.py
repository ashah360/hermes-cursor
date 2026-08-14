"""Tests for workers.py — the worker controller, with a faked process
table (``_pid_alive``) and a faked spawner (``_spawn_worker``)."""

import json
import os
import subprocess
from pathlib import Path

import pytest

from plugins.ghost_cursor import workers


def _git(*args, cwd):
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", *args],
        cwd=str(cwd), check=True, capture_output=True,
    )


def _make_repo(path):
    """A real git repo with one commit (worktrees need a commit)."""
    path.mkdir(parents=True, exist_ok=True)
    _git("init", "-q", "-b", "main", cwd=path)
    (path / "seed.txt").write_text("seed\n")
    _git("add", ".", cwd=path)
    _git("commit", "-q", "-m", "seed", cwd=path)
    return path


@pytest.fixture
def fake_procs(monkeypatch):
    """A fake process table: pids in the set are alive. Kills and group
    probes operate on the fake table (never signal real processes)."""
    alive = set()
    monkeypatch.setattr(workers, "_pid_alive", lambda pid: int(pid) in alive)
    monkeypatch.setattr(
        workers, "_kill_detached",
        lambda record: alive.discard(int(record.pid)),
    )
    monkeypatch.setattr(
        workers, "_pgid_empty", lambda pgid: int(pgid) not in alive,
    )
    return alive


@pytest.fixture
def fake_spawn(monkeypatch, fake_procs):
    """A fake spawner that 'starts' pids 1000, 1001, ... and (by default)
    writes the ready line to the log immediately."""
    calls = []

    def spawn(name, repo_path, log_path, **kw):
        pid = 1000 + len(calls)
        calls.append({
            "name": name, "repo_path": repo_path, "log_path": log_path, **kw,
        })
        fake_procs.add(pid)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(f"registering...\n{workers.READY_LINE}\n")
        return pid

    monkeypatch.setattr(workers, "_spawn_worker", spawn)
    return calls


@pytest.fixture(autouse=True)
def fast_ready(monkeypatch):
    monkeypatch.setattr(workers, "READY_TIMEOUT_S", 1.0)
    monkeypatch.setattr(workers, "_READY_POLL_S", 0.01)


@pytest.fixture(autouse=True)
def no_systemd(monkeypatch):
    """Tests never touch the host's real user systemd manager; the
    systemd path is exercised through explicit fakes."""
    monkeypatch.setattr(workers, "_systemd_available", lambda: False)


@pytest.fixture(autouse=True)
def reachable_management(monkeypatch):
    """Default test-world management endpoint: reachable-and-connected
    once a generation reached READY, silent while it is still spawning
    (so readiness tests stay log-driven). Tests that exercise
    disconnected/unreachable endpoints override this."""
    monkeypatch.setattr(
        workers, "_probe_management",
        lambda record: (
            {"status": "ok", "connected": True, "claimed": False}
            if record.state == workers.STATE_READY
            else None
        ),
    )


@pytest.fixture
def fake_systemd(monkeypatch, fake_procs):
    """A fake user systemd manager: records started units, assigns
    MainPIDs 9000..., mints invocation ids, tracks stops."""
    state = {"started": [], "stopped": [], "units": {}}

    def start(unit, argv, env, cwd, log_path, env_file=""):
        pid = 9000 + len(state["started"])
        state["started"].append({
            "unit": unit, "argv": list(argv), "env": dict(env),
            "cwd": cwd, "log_path": log_path, "env_file": env_file,
        })
        state["units"][unit] = {
            "ActiveState": "active",
            "MainPID": str(pid),
            "InvocationID": f"inv-{len(state['started']):04d}",
        }
        fake_procs.add(pid)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, "a") as fh:
            fh.write(f"registering...\n{workers.READY_LINE}\n")

    def show(unit):
        return dict(state["units"].get(unit) or {
            "ActiveState": "inactive", "MainPID": "0", "InvocationID": "",
        })

    def stop(unit):
        state["stopped"].append(unit)
        info = state["units"].pop(unit, None)
        if info:
            fake_procs.discard(int(info["MainPID"]))
        return True  # verified stop succeeded

    monkeypatch.setattr(workers, "_systemd_available", lambda: True)
    # CI intentionally has no Cursor CLI installed. This fixture exercises
    # systemd supervision, not binary discovery, so provide the executable
    # identity that the fake manager receives.
    monkeypatch.setattr(workers, "_agent_cli_path", lambda: "/usr/bin/agent")
    monkeypatch.setattr(workers, "_systemd_start", start)
    monkeypatch.setattr(workers, "_systemd_show", show)
    monkeypatch.setattr(workers, "_systemd_stop", stop)
    monkeypatch.setattr(workers, "_linger_enabled", lambda: True)
    return state


class TestCanonicalIdentity:
    """Canonical identity = realpath(git rev-parse --show-toplevel).

    Governing invariants: a subdirectory keys the SAME worker as its
    worktree root; sibling linked worktrees of one repository stay
    DISTINCT; non-git directories degrade to their realpath.
    """

    def test_subdir_resolves_to_worktree_toplevel(self, tmp_path):
        repo = _make_repo(tmp_path / "repo")
        sub = repo / "src" / "nested"
        sub.mkdir(parents=True)
        assert workers.canonical_repo_path(str(sub)) == os.path.realpath(repo)
        assert workers.worker_name_for(str(sub)) == workers.worker_name_for(
            str(repo)
        )

    def test_sibling_worktrees_stay_distinct(self, tmp_path):
        repo = _make_repo(tmp_path / "repo")
        wt = tmp_path / "wt-feature"
        _git("worktree", "add", "-q", str(wt), "-b", "feature", cwd=repo)
        assert workers.canonical_repo_path(str(wt)) == os.path.realpath(wt)
        assert workers.canonical_repo_path(str(wt)) != (
            workers.canonical_repo_path(str(repo))
        )
        # A subdir of the linked worktree keys the WORKTREE, not the repo.
        sub = wt / "sub"
        sub.mkdir()
        assert workers.canonical_repo_path(str(sub)) == os.path.realpath(wt)

    def test_non_git_dir_falls_back_to_realpath(self, tmp_path):
        plain = tmp_path / "plain"
        plain.mkdir()
        assert workers.canonical_repo_path(str(plain)) == os.path.realpath(
            plain
        )

    def test_symlink_to_worktree_resolves(self, tmp_path):
        repo = _make_repo(tmp_path / "repo")
        link = tmp_path / "link"
        link.symlink_to(repo)
        assert workers.canonical_repo_path(str(link)) == os.path.realpath(
            repo
        )


class TestAuthSecrecy:
    """The governing secret-handoff invariant: CURSOR_API_KEY reaches a
    supervised worker ONLY through a 0600 env file referenced by PATH —
    the key VALUE never appears in systemd-run argv (process inspection
    would leak it), unit metadata, or logs. The detached fallback passes
    it via the subprocess environment (never argv)."""

    SECRET = "sk-cursor-super-secret-0123456789"

    def test_key_value_absent_from_systemd_run_argv(self, tmp_path):
        cmd = workers._systemd_run_command(
            "cursor-worker-w.service",
            ["/usr/bin/agent", "worker", "start", "--name", "w"],
            {"CURSOR_DATA_DIR": "/state/data/w", "CURSOR_API_KEY": self.SECRET,
             "PATH": "/usr/bin", "HOME": "/home/u"},
            "/repo",
            Path("/state/w-gen.log"),
            env_file="/state/secrets/w.env",
        )
        joined = " ".join(cmd)
        assert self.SECRET not in joined
        assert "--setenv=CURSOR_API_KEY" not in joined
        # The unit reads the secret by PATH only.
        assert "--property=EnvironmentFile=/state/secrets/w.env" in cmd
        # Non-secret allowlist still crosses via --setenv.
        assert "--setenv=CURSOR_DATA_DIR=/state/data/w" in cmd
        assert "--setenv=PATH=/usr/bin" in cmd
        assert "--setenv=HOME=/home/u" in cmd

    def test_env_file_written_0600_outside_worktree(self, monkeypatch):
        monkeypatch.setenv("CURSOR_API_KEY", self.SECRET)
        path = workers._write_auth_env(
            "w-auth", {"CURSOR_API_KEY": self.SECRET}
        )
        assert path
        env_path = Path(path)
        # Machine-global state dir, never the worktree.
        assert str(workers.state_dir()) in path
        assert oct(env_path.stat().st_mode & 0o777) == "0o600"
        assert oct(env_path.parent.stat().st_mode & 0o777) == "0o700"
        assert env_path.read_text() == f'CURSOR_API_KEY="{self.SECRET}"\n'

    def test_env_file_escapes_quotes_and_backslashes(self):
        tricky = 'k"ey\\with"specials'
        path = workers._write_auth_env("w-esc", {"CURSOR_API_KEY": tricky})
        body = Path(path).read_text()
        assert body == 'CURSOR_API_KEY="k\\"ey\\\\with\\"specials"\n'

    def test_no_key_no_env_file(self):
        assert workers._write_auth_env("w-nokey", {"PATH": "/usr/bin"}) == ""

    def test_systemd_spawn_hands_over_path_not_value(
        self, tmp_path, fake_systemd, monkeypatch
    ):
        monkeypatch.setenv("CURSOR_API_KEY", self.SECRET)
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo))
        started = fake_systemd["started"][0]
        assert started["env_file"] == str(
            workers._auth_env_path(record.name)
        )
        assert self.SECRET not in " ".join(started["argv"])
        assert Path(started["env_file"]).read_text() == (
            f'CURSOR_API_KEY="{self.SECRET}"\n'
        )

    def test_detached_spawn_env_carries_the_key(self, monkeypatch):
        monkeypatch.setenv("CURSOR_API_KEY", self.SECRET)
        argv, env = workers._spawn_command(
            "/usr/bin/agent", "w", "/repo", data_dir="/state/data/w",
        )
        assert env["CURSOR_API_KEY"] == self.SECRET
        assert self.SECRET not in " ".join(argv)


class TestWorkerNames:
    def test_deterministic_per_realpath(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()
        assert workers.worker_name_for(str(repo)) == workers.worker_name_for(
            str(repo)
        )

    def test_symlink_resolves_to_same_worker(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()
        link = tmp_path / "link"
        link.symlink_to(repo)
        assert workers.worker_name_for(str(link)) == workers.worker_name_for(
            str(repo)
        )

    def test_different_checkouts_different_names(self, tmp_path):
        a, b = tmp_path / "a", tmp_path / "b"
        a.mkdir(), b.mkdir()
        assert workers.worker_name_for(str(a)) != workers.worker_name_for(str(b))

    def test_name_shape(self, tmp_path):
        name = workers.worker_name_for(str(tmp_path))
        slug, digest = name.rsplit("-", 1)
        assert len(digest) == 8 and slug


class TestEnsureWorker:
    def test_fresh_spawn(self, tmp_path, fake_spawn):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo))
        assert record.pid == 1000
        assert record.repo_path == os.path.realpath(str(repo))
        assert not record.verified
        assert len(fake_spawn) == 1
        # State json persisted
        saved = json.loads(
            (workers.state_dir() / f"{record.name}.json").read_text()
        )
        assert saved["pid"] == 1000

    def test_reuses_live_worker(self, tmp_path, fake_spawn):
        repo = tmp_path / "repo"
        repo.mkdir()
        first = workers.ensure_worker(str(repo))
        second = workers.ensure_worker(str(repo))
        assert second.pid == first.pid
        assert len(fake_spawn) == 1  # no second spawn

    def test_dead_record_respawns(self, tmp_path, fake_spawn, fake_procs):
        repo = tmp_path / "repo"
        repo.mkdir()
        first = workers.ensure_worker(str(repo))
        fake_procs.discard(first.pid)  # the worker died
        second = workers.ensure_worker(str(repo))
        assert second.pid != first.pid
        assert len(fake_spawn) == 2

    def test_never_ready_raises_with_log_tail(
        self, tmp_path, monkeypatch, fake_procs
    ):
        repo = tmp_path / "repo"
        repo.mkdir()

        def spawn_silent(name, repo_path, log_path, **kw):
            fake_procs.add(2000)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_path.write_text("registering forever...\n")
            return 2000

        monkeypatch.setattr(workers, "_spawn_worker", spawn_silent)
        with pytest.raises(workers.WorkerError) as err:
            workers.ensure_worker(str(repo))
        assert "did not report ready" in str(err.value)
        assert "registering forever" in str(err.value)

    def test_spawn_death_raises(self, tmp_path, monkeypatch, fake_procs):
        repo = tmp_path / "repo"
        repo.mkdir()

        def spawn_dying(name, repo_path, log_path, **kw):
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_path.write_text("fatal: not logged in\n")
            return 3000  # never added to fake_procs — already dead

        monkeypatch.setattr(workers, "_spawn_worker", spawn_dying)
        with pytest.raises(workers.WorkerError) as err:
            workers.ensure_worker(str(repo))
        assert "exited during startup" in str(err.value)
        assert "not logged in" in str(err.value)

    def test_single_dead_reading_is_not_death(self, tmp_path, monkeypatch):
        """One dead probe (the pre-exec cmdline window) must not be treated
        as startup death — that needs two consecutive readings."""
        repo = tmp_path / "repo"
        repo.mkdir()
        reads = iter([False])  # dead once, then alive forever
        monkeypatch.setattr(workers, "_pid_alive", lambda pid: next(reads, True))

        def spawn_slow(name, repo_path, log_path, **kw):
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_path.write_text("registering...\n")
            return 4000

        monkeypatch.setattr(workers, "_spawn_worker", spawn_slow)
        with pytest.raises(workers.WorkerError) as err:
            workers.ensure_worker(str(repo))
        assert "did not report ready" in str(err.value)
        assert "exited during startup" not in str(err.value)

    def test_missing_agent_cli(self, tmp_path, monkeypatch):
        repo = tmp_path / "repo"
        repo.mkdir()
        monkeypatch.setattr(workers, "_agent_cli_path", lambda: None)
        with pytest.raises(workers.WorkerError) as err:
            workers.ensure_worker(str(repo))
        assert "agent" in str(err.value)


class TestSpawnEnv:
    def test_strips_loader_and_interpreter_vars(self, monkeypatch):
        monkeypatch.setenv("LD_LIBRARY_PATH", "/opt/python/lib")
        monkeypatch.setenv("LD_PRELOAD", "/x.so")
        monkeypatch.setenv("DYLD_LIBRARY_PATH", "/y")
        monkeypatch.setenv("PYTHONPATH", "/z")
        monkeypatch.setenv("PYTHONHOME", "/w")
        monkeypatch.setenv("PATH", "/usr/bin")
        env = workers._spawn_env()
        assert "LD_LIBRARY_PATH" not in env
        assert "LD_PRELOAD" not in env
        assert "DYLD_LIBRARY_PATH" not in env
        assert "PYTHONPATH" not in env
        assert "PYTHONHOME" not in env
        assert env["PATH"] == "/usr/bin"


class TestLiveWorkersAndCleanup:
    def test_lists_only_live_and_cleans_dead(self, tmp_path, fake_spawn, fake_procs):
        repo_a, repo_b = tmp_path / "a", tmp_path / "b"
        repo_a.mkdir(), repo_b.mkdir()
        rec_a = workers.ensure_worker(str(repo_a))
        rec_b = workers.ensure_worker(str(repo_b))
        fake_procs.discard(rec_a.pid)  # a died

        live = workers.live_workers()
        assert [r.name for r in live] == [rec_b.name]
        # the dead record's pidfile was lazily removed
        assert not (workers.state_dir() / f"{rec_a.name}.json").exists()

    def test_corrupt_record_removed(self, tmp_path, fake_spawn):
        directory = workers.state_dir()
        directory.mkdir(parents=True, exist_ok=True)
        bad = directory / "corrupt-record.json"
        bad.write_text("{not json")
        assert workers.live_workers() == []
        assert not bad.exists()

    def test_mark_verified_persists(self, tmp_path, fake_spawn):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo))
        assert not record.verified
        workers.mark_verified(record.name)
        assert workers._read_record(record.name).verified
        # idempotent
        workers.mark_verified(record.name)
        assert workers._read_record(record.name).verified


class TestUnroutableHint:
    def test_names_conflicting_managed_worker(self, tmp_path, fake_spawn, monkeypatch):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo))
        # A second managed record on the SAME checkout (simulated — the
        # deterministic name normally prevents this, but records written
        # by other profiles/boxes sharing state must still be named).
        other = workers.WorkerRecord(
            name="other-worker",
            repo_path=str(repo),
            pid=record.pid,  # alive in the fake table
            log_path=str(workers.state_dir() / "other-worker.log"),
            started_at=0.0,
        )
        workers._write_record(other)
        hint = workers.unroutable_hint(record.name, str(repo))
        assert "not routable" in hint
        assert "other-worker" in hint

    def test_no_conflict_mentions_manual_workers(self, tmp_path, fake_spawn):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo))
        hint = workers.unroutable_hint(record.name, str(repo))
        assert "manually-started" in hint
