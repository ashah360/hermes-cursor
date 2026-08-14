"""Tests for workers.py — the worker controller, with a faked process
table (``_pid_alive``) and a faked spawner (``_spawn_worker``)."""

import hashlib
import json
import os
import subprocess
import time
from pathlib import Path

import pytest

from plugins.ghost_cursor import workers


def _raw_name(path):
    """The PRE-canonical naming scheme: a hash of the raw dispatch path
    (subdir included), same host slug as today's worker_name_for."""
    digest = hashlib.sha256(
        os.path.realpath(str(path)).encode("utf-8")
    ).hexdigest()[:8]
    host_slug = workers.worker_name_for(str(path)).rsplit("-", 1)[0]
    return f"{host_slug}-{digest}"


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


class _FakeProcTable(set):
    """A set of live pids, plus per-pid births (default 1) so the
    pid-reuse fence is exercised deterministically."""

    def __init__(self):
        super().__init__()
        self.births = {}


@pytest.fixture
def fake_procs(monkeypatch):
    """A fake process table: pids in the set are alive. Kills, group
    probes, existence and birth reads all operate on the fake table
    (never on real processes)."""
    alive = _FakeProcTable()
    monkeypatch.setattr(workers, "_pid_alive", lambda pid: int(pid) in alive)
    monkeypatch.setattr(workers, "_pid_exists", lambda pid: int(pid) in alive)
    monkeypatch.setattr(
        workers, "_pid_birth", lambda pid: alive.births.get(int(pid), 1)
    )
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


class TestMachineGlobalControlDir:
    """Worker records/locks/data roots are MACHINE-global (unit names and
    the one-worker-per-worktree invariant are machine-global); session
    handles stay profile-local. Pre-v2 profile-local records migrate by
    adoption — never by killing live workers."""

    def test_state_dir_is_independent_of_hermes_profile(self, monkeypatch, tmp_path):
        monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "xdg"))
        monkeypatch.setenv("HERMES_HOME", str(tmp_path / "profile-a"))
        dir_a = workers.state_dir()
        monkeypatch.setenv("HERMES_HOME", str(tmp_path / "profile-b"))
        dir_b = workers.state_dir()
        assert dir_a == dir_b
        assert str(tmp_path / "xdg") in str(dir_a)

    def test_profile_local_records_migrate_by_adoption(
        self, tmp_path, fake_procs, fake_spawn
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        name = workers.worker_name_for(str(repo))
        # A pre-v2 record left in the PROFILE-local location.
        legacy_dir = workers._profile_state_dir()
        legacy_dir.mkdir(parents=True, exist_ok=True)
        (legacy_dir / f"{name}.json").write_text(json.dumps({
            "name": name,
            "repo_path": os.path.realpath(str(repo)),
            "pid": 8811,
            "log_path": str(legacy_dir / f"{name}.log"),
            "started_at": 1000.0,
            "verified": True,
        }))
        fake_procs.add(8811)
        workers._reset_migration_for_tests()

        record = workers.ensure_worker(str(repo))
        assert record.pid == 8811          # adopted from the profile dir
        assert len(fake_spawn) == 0        # never killed, never respawned
        assert record.supervision == "legacy"
        # The record now lives in the machine-global control dir.
        assert (workers.state_dir() / f"{name}.json").exists()


class TestMultiProfileMigration:
    def _seed(self, directory, name, repo, pid):
        directory.mkdir(parents=True, exist_ok=True)
        (directory / f"{name}.json").write_text(json.dumps({
            "name": name, "repo_path": os.path.realpath(str(repo)),
            "pid": pid, "log_path": str(directory / f"{name}.log"),
            "started_at": 1000.0, "verified": True,
        }))

    def test_discovers_default_profile_and_named_profiles(
        self, tmp_path, fake_procs
    ):
        hermes_home = Path(os.environ["HERMES_HOME"])
        repo_a, repo_b = tmp_path / "a", tmp_path / "b"
        repo_a.mkdir(), repo_b.mkdir()
        name_a = workers.worker_name_for(str(repo_a))
        name_b = workers.worker_name_for(str(repo_b))
        self._seed(
            hermes_home / "state" / "ghost_cursor" / "workers",
            name_a, repo_a, 9911,
        )
        self._seed(
            hermes_home / "profiles" / "beta" / "state" / "ghost_cursor" / "workers",
            name_b, repo_b, 9912,
        )
        fake_procs.update({9911, 9912})
        workers._reset_migration_for_tests()

        names = {r.name for r in workers.live_workers()}
        assert {name_a, name_b} <= names  # both profiles adopted

    def test_live_live_collision_is_an_explicit_conflict_with_no_winner(
        self, tmp_path, fake_procs, fake_spawn, monkeypatch
    ):
        """Two profiles hold the SAME worker name with DIFFERENT live
        pids: first-record-wins would leave one live worker untracked
        and later permit duplication. The collision must become an
        explicit machine-global CONFLICT — no winner, no stop, no
        overwrite, no spawn — until one candidate is provably dead."""
        hermes_home = Path(os.environ["HERMES_HOME"])
        repo = tmp_path / "repo"
        repo.mkdir()
        name = workers.worker_name_for(str(repo))
        self._seed(
            hermes_home / "state" / "ghost_cursor" / "workers",
            name, repo, 9921,
        )
        self._seed(
            hermes_home / "profiles" / "beta" / "state" / "ghost_cursor" / "workers",
            name, repo, 9922,
        )
        fake_procs.update({9921, 9922})  # BOTH candidates are alive
        kills = []
        monkeypatch.setattr(
            workers, "_kill_detached", lambda record: kills.append(record.pid)
        )
        stops = []
        monkeypatch.setattr(
            workers, "_systemd_stop", lambda unit: stops.append(unit) or True
        )
        workers._reset_migration_for_tests()

        with pytest.raises(workers.WorkerError) as err:
            workers.ensure_worker(str(repo), lease_id="run-x")
        assert "conflict" in str(err.value).lower()
        assert "9921" in str(err.value) and "9922" in str(err.value)
        assert kills == [] and stops == []      # neither candidate stopped
        assert len(fake_spawn) == 0             # no duplicate spawn
        record = workers._read_record(name)
        assert record.state == workers.STATE_CONFLICT
        # Ownership evidence for BOTH candidates is preserved.
        candidates = workers._read_conflict_candidates(name)
        assert {c["pid"] for c in candidates} == {9921, 9922}

        # Resolution: one candidate provably dead -> the survivor is
        # adopted safely and dispatch works again.
        fake_procs.discard(9922)
        resolved = workers.ensure_worker(str(repo), lease_id="run-x")
        assert resolved.pid == 9921
        assert resolved.state != workers.STATE_CONFLICT
        assert kills == [] and stops == []      # still nothing stopped
        assert workers._read_conflict_candidates(name) is None


class TestLegacyProtection:
    """A legacy (generation-0) worker may be serving a pre-v2 run this
    controller cannot see (no leases, possibly another profile) — it is
    never TTL-reaped and never capacity-reclaimed while alive."""

    def _legacy_record(self, repo, pid, fake_procs):
        name = workers.worker_name_for(str(repo))
        directory = workers.state_dir()
        directory.mkdir(parents=True, exist_ok=True)
        (directory / f"{name}.json").write_text(json.dumps({
            "name": name,
            "repo_path": os.path.realpath(str(repo)),
            "pid": pid,
            "log_path": str(directory / f"{name}.log"),
            "started_at": 5.0,  # ancient — far past any TTL
            "verified": True,
        }))
        fake_procs.add(pid)
        return name

    def test_active_legacy_worker_survives_the_reaper(
        self, tmp_path, fake_procs
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        name = self._legacy_record(repo, 8821, fake_procs)
        assert workers.reconcile() == []
        assert workers._read_record(name) is not None

    def test_dead_legacy_worker_is_still_cleaned(self, tmp_path, fake_procs):
        repo = tmp_path / "repo"
        repo.mkdir()
        name = self._legacy_record(repo, 8822, fake_procs)
        fake_procs.discard(8822)
        assert workers.reconcile() == [name]

    def test_capacity_never_reclaims_a_live_legacy_worker(
        self, tmp_path, fake_procs, fake_spawn, monkeypatch
    ):
        monkeypatch.setattr(workers, "_max_workers", lambda: 1)
        repo_a, repo_b = tmp_path / "a", tmp_path / "b"
        repo_a.mkdir(), repo_b.mkdir()
        name_a = self._legacy_record(repo_a, 8823, fake_procs)
        with pytest.raises(workers.WorkerError):
            workers.ensure_worker(str(repo_b))
        assert workers._read_record(name_a) is not None


class TestRecordV2AndFencing:
    """Durable versioned records: legacy adoption, generation fencing,
    cross-process (flock) spawn serialization."""

    def test_legacy_v1_record_is_adopted_not_replaced(
        self, tmp_path, fake_procs, fake_spawn
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        name = workers.worker_name_for(str(repo))
        directory = workers.state_dir()
        directory.mkdir(parents=True, exist_ok=True)
        (directory / f"{name}.json").write_text(json.dumps({
            "name": name,
            "repo_path": os.path.realpath(str(repo)),
            "pid": 7777,
            "log_path": str(directory / f"{name}.log"),
            "started_at": 12345.0,
            "verified": True,
        }))
        fake_procs.add(7777)

        record = workers.ensure_worker(str(repo))
        assert record.pid == 7777              # adopted, not killed
        assert len(fake_spawn) == 0            # no respawn
        assert record.supervision == "legacy"
        assert record.generation == "legacy-7777"
        assert record.verified                 # v1 proof carries over

    def test_stale_generation_cannot_overwrite_current_record(
        self, tmp_path, fake_spawn
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo))
        current_gen = record.generation
        assert current_gen

        with workers._worker_lock(record.name):
            ok = workers._update_record_locked(
                record.name, "gen-someone-else", last_error="stale writer"
            )
        assert ok is False
        persisted = workers._read_record(record.name)
        assert persisted.generation == current_gen
        assert persisted.last_error != "stale writer"

    def test_concurrent_ensure_spawns_exactly_one_worker(
        self, tmp_path, monkeypatch, fake_procs
    ):
        import threading as _threading
        import time as _time

        repo = tmp_path / "repo"
        repo.mkdir()
        calls = []

        def slow_spawn(name, repo_path, log_path, **kw):
            _time.sleep(0.2)  # hold the lock long enough for a real race
            pid = 5000 + len(calls)
            calls.append(name)
            fake_procs.add(pid)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(log_path, "a") as fh:
                fh.write(f"registering...\n{workers.READY_LINE}\n")
            return pid

        monkeypatch.setattr(workers, "_spawn_worker", slow_spawn)
        results, errors = [], []

        def ensure():
            try:
                results.append(workers.ensure_worker(str(repo)))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [_threading.Thread(target=ensure) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        assert not errors
        assert len(calls) == 1                     # exactly one spawn
        assert {r.pid for r in results} == {5000}  # both got the winner


class TestGenerationScopedReadiness:
    def test_respawn_rejects_previous_generations_ready_line(
        self, tmp_path, monkeypatch, fake_procs
    ):
        """A dead worker's log keeps its old 'Worker is now running' line;
        the respawned generation must NOT be declared ready by it."""
        repo = tmp_path / "repo"
        repo.mkdir()
        name = workers.worker_name_for(str(repo))
        log = workers.state_dir() / f"{name}.log"
        log.parent.mkdir(parents=True, exist_ok=True)
        # Evidence from a PREVIOUS incarnation, within tail range.
        log.write_text(f"old boot...\n{workers.READY_LINE}\nStopping worker\n")

        def spawn_never_ready(name, repo_path, log_path, **kw):
            fake_procs.add(6001)
            with open(log_path, "a") as fh:
                fh.write("registering fresh generation...\n")
            return 6001

        monkeypatch.setattr(workers, "_spawn_worker", spawn_never_ready)
        with pytest.raises(workers.WorkerError) as err:
            workers.ensure_worker(str(repo))
        assert "did not report ready" in str(err.value)

    def test_respawn_accepts_own_generations_ready_line(
        self, tmp_path, monkeypatch, fake_procs
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        name = workers.worker_name_for(str(repo))
        log = workers.state_dir() / f"{name}.log"
        log.parent.mkdir(parents=True, exist_ok=True)
        log.write_text(f"old boot...\n{workers.READY_LINE}\n")

        def spawn_ready(name, repo_path, log_path, **kw):
            fake_procs.add(6002)
            with open(log_path, "a") as fh:
                fh.write(f"fresh boot...\n{workers.READY_LINE}\n")
            return 6002

        monkeypatch.setattr(workers, "_spawn_worker", spawn_ready)
        record = workers.ensure_worker(str(repo))
        assert record.pid == 6002
        assert record.state == "ready"


class TestDataRootIsolation:
    """Every worker gets its own deterministic CURSOR_DATA_DIR and its
    own management endpoint — no two workers share either."""

    def test_distinct_worktrees_get_distinct_data_roots_and_ports(
        self, tmp_path, fake_spawn
    ):
        repo_a, repo_b = tmp_path / "a", tmp_path / "b"
        repo_a.mkdir(), repo_b.mkdir()
        rec_a = workers.ensure_worker(str(repo_a))
        rec_b = workers.ensure_worker(str(repo_b))
        assert rec_a.data_dir and rec_b.data_dir
        assert rec_a.data_dir != rec_b.data_dir
        assert rec_a.data_dir == str(workers.state_dir() / "data" / rec_a.name)
        assert rec_a.management_addr and rec_b.management_addr
        assert rec_a.management_addr != rec_b.management_addr
        assert rec_a.management_addr.startswith("127.0.0.1:")
        # The spawner was handed the SAME isolation parameters.
        assert fake_spawn[0]["data_dir"] == rec_a.data_dir
        assert fake_spawn[0]["management_addr"] == rec_a.management_addr

    def test_spawn_command_isolates_data_dir_and_management(self):
        argv, env = workers._spawn_command(
            "/usr/bin/agent", "w-name", "/repo",
            data_dir="/state/data/w-name",
            management_addr="127.0.0.1:42700",
        )
        assert argv[:4] == ["/usr/bin/agent", "worker", "start", "--name"]
        assert "--worker-dir" in argv and "/repo" in argv
        assert "--management-addr" in argv
        assert argv[argv.index("--management-addr") + 1] == "127.0.0.1:42700"
        assert env["CURSOR_DATA_DIR"] == "/state/data/w-name"
        assert "LD_LIBRARY_PATH" not in env  # loader vars still stripped


class TestSystemdSupervision:
    def test_spawns_transient_service_with_deterministic_identity(
        self, tmp_path, fake_systemd
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo))
        assert record.supervision == "systemd"
        assert record.unit == f"cursor-worker-{record.name}.service"
        assert record.pid == 9000                # MainPID is authoritative
        assert record.generation == "inv-0001"   # InvocationID = generation
        assert record.state == workers.STATE_READY
        started = fake_systemd["started"][0]
        assert started["unit"] == record.unit
        assert started["env"]["CURSOR_DATA_DIR"] == record.data_dir

    def test_adopted_after_restart_without_respawn(
        self, tmp_path, fake_systemd
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        first = workers.ensure_worker(str(repo))
        # A fresh plugin process (nothing in memory) re-ensures: the
        # persisted record + live unit are adopted, not respawned.
        second = workers.ensure_worker(str(repo))
        assert second.pid == first.pid
        assert second.generation == first.generation
        assert len(fake_systemd["started"]) == 1

    def test_stop_generation_stops_the_unit(self, tmp_path, fake_systemd):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo))
        stops_before = len(fake_systemd["stopped"])
        with workers._worker_lock(record.name):
            workers._stop_generation(record)
        # Full-cgroup teardown goes through the unit — exactly one stop.
        assert fake_systemd["stopped"][stops_before:] == [record.unit]

    def test_dead_unit_respawns_new_generation(self, tmp_path, fake_systemd, fake_procs):
        repo = tmp_path / "repo"
        repo.mkdir()
        first = workers.ensure_worker(str(repo))
        fake_procs.discard(first.pid)  # the service died
        fake_systemd["units"].pop(first.unit, None)
        second = workers.ensure_worker(str(repo))
        assert second.generation != first.generation
        assert second.pid != first.pid
        assert not second.verified  # routing proof does NOT carry over


class TestSystemdOwnershipVerification:
    def test_systemd_run_command_construction(self):
        cmd = workers._systemd_run_command(
            "cursor-worker-w.service",
            ["/usr/bin/agent", "worker", "start", "--name", "w"],
            {"CURSOR_DATA_DIR": "/state/data/w", "CURSOR_API_KEY": "cursor-key",
             "PATH": "/usr/bin", "HOME": "/home/u", "IRRELEVANT": "x"},
            "/repo",
            Path("/state/w-gen.log"),
        )
        assert cmd[:2] == ["systemd-run", "--user"]
        assert "--unit=cursor-worker-w.service" in cmd
        assert "--collect" in cmd
        assert "--service-type=exec" in cmd
        assert "--property=KillMode=control-group" in cmd
        assert f"--property=TimeoutStopSec={workers.STOP_TIMEOUT_S}" in cmd
        assert "--property=Restart=no" in cmd
        assert "--property=StandardOutput=append:/state/w-gen.log" in cmd
        assert "--working-directory=/repo" in cmd
        assert "--setenv=CURSOR_DATA_DIR=/state/data/w" in cmd
        assert "--setenv=IRRELEVANT=x" not in cmd  # only the allowlist
        # The secret NEVER rides in argv (see TestAuthSecrecy).
        assert not any("cursor-key" in part for part in cmd)
        # The payload argv rides verbatim after the `--` separator.
        assert cmd[cmd.index("--"):] == [
            "--", "/usr/bin/agent", "worker", "start", "--name", "w",
        ]

    def test_identity_mismatch_fails_closed_without_stopping_foreign_unit(
        self, tmp_path, fake_systemd
    ):
        """A live unit whose InvocationID differs from the persisted
        generation may belong to ANOTHER generation/controller: it is
        never adopted AND never stopped — the record turns conflict and
        the dispatch fails closed for operator/reconciler resolution."""
        repo = tmp_path / "repo"
        repo.mkdir()
        first = workers.ensure_worker(str(repo))
        fake_systemd["units"][first.unit] = {
            "ActiveState": "active",
            "MainPID": str(first.pid),        # keep pid: only invocation
            "InvocationID": "inv-imposter",   # identity differs
        }
        stops_before = len(fake_systemd["stopped"])
        with pytest.raises(workers.WorkerError) as err:
            workers.ensure_worker(str(repo))
        assert "conflict" in str(err.value)
        assert len(fake_systemd["stopped"]) == stops_before  # NOT stopped
        retained = workers._read_record(first.name)
        assert retained is not None
        assert retained.state == workers.STATE_CONFLICT

    def test_missing_invocation_id_is_not_valid_fencing(
        self, tmp_path, fake_systemd
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        first = workers.ensure_worker(str(repo))
        fake_systemd["units"][first.unit] = {
            "ActiveState": "active",
            "MainPID": str(first.pid),
            "InvocationID": "",               # no identity — no fencing
        }
        stops_before = len(fake_systemd["stopped"])
        with pytest.raises(workers.WorkerError):
            workers.ensure_worker(str(repo))
        assert len(fake_systemd["stopped"]) == stops_before

    def test_stop_path_refuses_active_unit_with_missing_invocation(
        self, tmp_path, fake_systemd, fake_procs
    ):
        """Missing live InvocationID is UNKNOWN ownership, exactly like a
        mismatch: the stop path must never issue systemctl stop against
        an active unit it cannot positively prove it owns."""
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo))
        fake_procs.discard(record.pid)  # our leader died…
        # …and the active unit exposes NO invocation id (unprovable).
        fake_systemd["units"][record.unit] = {
            "ActiveState": "active",
            "MainPID": "424242",
            "InvocationID": "",
        }
        stops_before = len(fake_systemd["stopped"])
        with workers._worker_lock(record.name):
            assert workers._stop_generation(record) is False
        assert len(fake_systemd["stopped"]) == stops_before  # NOT stopped
        # The ensure path fails closed the same way: record retained.
        with pytest.raises(workers.WorkerError):
            workers.ensure_worker(str(repo))
        assert workers._read_record(record.name) is not None

    def test_unverified_stop_fails_closed_and_retains_the_record(
        self, tmp_path, fake_systemd, fake_procs, monkeypatch
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo))
        fake_procs.discard(record.pid)  # leader died…
        # …but the stop CANNOT be verified (cgroup still has members).
        monkeypatch.setattr(workers, "_systemd_stop", lambda unit: False)

        with pytest.raises(workers.WorkerError) as err:
            workers.ensure_worker(str(repo))
        assert "confirmed stopped" in str(err.value)
        retained = workers._read_record(record.name)
        assert retained is not None          # never an untracked remnant
        assert retained.state == workers.STATE_DRAINING
        assert "stop unverified" in retained.last_error
        assert len(fake_systemd["started"]) == 1  # no duplicate spawn


class TestVerifiedStopEvidence:
    """_unit_stopped succeeds only on POSITIVE inactive/not-found
    evidence from the real systemctl seam; unknown show output is
    UNKNOWN, never 'stopped'."""

    @staticmethod
    def _proc(rc, stdout="", stderr=""):
        from types import SimpleNamespace

        return SimpleNamespace(returncode=rc, stdout=stdout, stderr=stderr)

    def test_failed_show_is_unknown_not_stopped(self, monkeypatch):
        monkeypatch.setattr(
            workers, "_run_systemctl",
            lambda args, timeout=None: self._proc(1, "", "dbus gone"),
        )
        assert workers._unit_stopped("cursor-worker-x.service") is False

    def test_positive_evidence_accepts_inactive_and_not_found(self, monkeypatch):
        monkeypatch.setattr(
            workers, "_run_systemctl",
            lambda args, timeout=None: self._proc(
                0, "ActiveState=inactive\nLoadState=loaded\nMainPID=0\n"
            ),
        )
        assert workers._unit_stopped("u.service") is True
        monkeypatch.setattr(
            workers, "_run_systemctl",
            lambda args, timeout=None: self._proc(
                0, "ActiveState=inactive\nLoadState=not-found\nMainPID=0\n"
            ),
        )
        assert workers._unit_stopped("u.service") is True

    def test_active_show_is_not_stopped(self, monkeypatch):
        monkeypatch.setattr(
            workers, "_run_systemctl",
            lambda args, timeout=None: self._proc(
                0, "ActiveState=active\nLoadState=loaded\nMainPID=42\n"
            ),
        )
        assert workers._unit_stopped("u.service") is False

    def test_systemd_stop_unconfirmed_on_nonzero_stop_and_unknown_show(
        self, monkeypatch
    ):
        def run(args, timeout=None):
            if "stop" in args:
                return self._proc(1, "", "Job for u.service failed")
            if "show" in args:
                return self._proc(1, "", "Failed to get properties")
            return self._proc(0)

        monkeypatch.setattr(workers, "_run_systemctl", run)
        assert workers._systemd_stop("u.service") is False


class TestDeadLeaderTeardown:
    def test_dead_leader_with_surviving_descendants_fails_closed(
        self, tmp_path, fake_spawn, fake_procs, monkeypatch
    ):
        """A dead detached leader whose process GROUP still has members
        (tmux/MCP children) must go through full teardown; the record is
        never deleted over live descendants."""
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo))
        fake_procs.discard(record.pid)  # leader died…
        # …but descendants keep the process group alive and unkillable.
        monkeypatch.setattr(workers, "_kill_detached", lambda record: None)
        monkeypatch.setattr(workers, "_pgid_empty", lambda pgid: False)

        with pytest.raises(workers.WorkerError):
            workers.ensure_worker(str(repo))
        retained = workers._read_record(record.name)
        assert retained is not None
        assert retained.state == workers.STATE_DRAINING

    def test_dead_systemd_leader_still_stops_through_the_unit(
        self, tmp_path, fake_systemd, fake_procs
    ):
        """Even with the leader pid gone, the unit's cgroup may hold
        descendants — cleanup must route through systemctl stop."""
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo))
        fake_procs.discard(record.pid)
        stops_before = len(fake_systemd["stopped"])
        assert workers.reconcile() == [record.name]
        assert record.unit in fake_systemd["stopped"][stops_before:]


class TestReadyReuseRevalidation:
    def test_disconnected_ready_worker_is_replaced(
        self, tmp_path, fake_spawn, monkeypatch
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        first = workers.ensure_worker(str(repo))
        monkeypatch.setattr(
            workers, "_probe_management",
            lambda record: {"status": "not_ready", "connected": False,
                            "claimed": False},
        )
        second = workers.ensure_worker(str(repo))
        assert second.generation != first.generation  # replaced
        assert len(fake_spawn) == 2

    def test_claimed_but_connected_worker_is_still_handed_out(
        self, tmp_path, fake_spawn, monkeypatch
    ):
        """claimed=true means BUSY, not unhealthy — the claimed-vs-
        connected distinction must be preserved."""
        repo = tmp_path / "repo"
        repo.mkdir()
        first = workers.ensure_worker(str(repo))
        monkeypatch.setattr(
            workers, "_probe_management",
            lambda record: {"status": "not_ready", "connected": True,
                            "claimed": True},
        )
        second = workers.ensure_worker(str(repo))
        assert second.generation == first.generation  # reused
        assert len(fake_spawn) == 1

    def test_unreachable_endpoint_is_not_proven_connected(
        self, tmp_path, fake_spawn, monkeypatch
    ):
        """A READY record whose management endpoint is unreachable is
        NOT proven connected: the dispatch fails with an actionable
        retry — the worker is kept, never handed out, never killed."""
        repo = tmp_path / "repo"
        repo.mkdir()
        first = workers.ensure_worker(str(repo))
        monkeypatch.setattr(workers, "_probe_management", lambda record: None)

        with pytest.raises(workers.WorkerError) as err:
            workers.ensure_worker(str(repo))
        assert "unreachable" in str(err.value) or "not proven" in str(err.value)
        retained = workers._read_record(first.name)
        assert retained is not None
        assert retained.generation == first.generation  # kept, not killed

    def test_legacy_record_without_endpoint_is_still_adoptable(
        self, tmp_path, fake_procs, monkeypatch
    ):
        """Legacy generation-0 records predate the management endpoint:
        adoption stands on process liveness (the migration promise)."""
        monkeypatch.setattr(workers, "_probe_management", lambda record: None)
        repo = tmp_path / "repo"
        repo.mkdir()
        name = workers.worker_name_for(str(repo))
        directory = workers.state_dir()
        directory.mkdir(parents=True, exist_ok=True)
        (directory / f"{name}.json").write_text(json.dumps({
            "name": name, "repo_path": os.path.realpath(str(repo)),
            "pid": 8899, "log_path": str(directory / f"{name}.log"),
            "started_at": 1000.0, "verified": True,
        }))
        fake_procs.add(8899)
        record = workers.ensure_worker(str(repo))
        assert record.pid == 8899
        assert record.supervision == "legacy"


class TestDegradedFallback:
    def test_no_user_systemd_falls_back_to_detached_and_says_so(
        self, tmp_path, fake_spawn
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo))
        assert record.supervision == "detached"
        assert record.unit == ""
        # Isolation still applies on the degraded path.
        assert record.data_dir
        assert record.management_addr


class TestForeignUnitProtection:
    """An ACTIVE deterministic unit with no managed record is UNKNOWN
    ownership (another controller/state-dir may own it): it is NEVER
    stopped — the ensure fails closed with explicit conflict state, and
    self-heals only once the unit is positively gone."""

    def _seed_foreign_unit(self, repo, fake_systemd, fake_procs):
        name = workers.worker_name_for(str(repo))
        unit = workers._unit_name(name)
        fake_systemd["units"][unit] = {
            "ActiveState": "active",
            "MainPID": "31337",
            "InvocationID": "inv-foreign",
        }
        fake_procs.add(31337)
        return name, unit

    def test_active_unit_with_no_record_is_never_stopped(
        self, tmp_path, fake_systemd, fake_procs
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        name, unit = self._seed_foreign_unit(repo, fake_systemd, fake_procs)
        with pytest.raises(workers.WorkerError) as err:
            workers.ensure_worker(str(repo))
        assert unit in str(err.value)
        assert fake_systemd["stopped"] == []            # NEVER stopped
        record = workers._read_record(name)
        assert record is not None
        assert record.state == workers.STATE_CONFLICT   # explicit conflict

    def test_foreign_unit_conflict_self_heals_once_the_unit_is_gone(
        self, tmp_path, fake_systemd, fake_procs
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        name, unit = self._seed_foreign_unit(repo, fake_systemd, fake_procs)
        with pytest.raises(workers.WorkerError):
            workers.ensure_worker(str(repo))
        # While the foreign unit lives, every ensure keeps failing closed.
        with pytest.raises(workers.WorkerError):
            workers.ensure_worker(str(repo))
        assert fake_systemd["stopped"] == []
        # The foreign unit disappears (its owner stopped it).
        fake_systemd["units"].pop(unit)
        fake_procs.discard(31337)
        record = workers.ensure_worker(str(repo))
        assert record.state == workers.STATE_READY      # fresh spawn


class TestDetachedPidReuse:
    """Positive pid+birth ownership immediately before any detached
    signal: a reused pid (same number, different birth) may lead a
    FOREIGN process group — never signalled, record retained."""

    def test_reused_pid_is_never_signalled(
        self, tmp_path, fake_spawn, fake_procs, monkeypatch
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo))
        assert record.pid_birth == 1
        # The leader died and the kernel reused its pid for a FOREIGN
        # process (different birth) that started its own group.
        fake_procs.births[record.pid] = 2
        kills = []
        monkeypatch.setattr(
            workers, "_kill_detached", lambda r: kills.append(r.pid)
        )
        with workers._worker_lock(record.name):
            workers._update_record_locked(
                record.name, record.generation,
                last_active_at=time.time() - workers.IDLE_TTL_S - 60,
            )
        assert workers.reconcile() == []     # nothing reaped
        assert kills == []                   # NO signal at a foreign pgid
        retained = workers._read_record(record.name)
        assert retained is not None          # never silently dropped
        assert retained.state == workers.STATE_DRAINING
        assert record.pid in fake_procs      # the foreign process lives on

    def test_matching_birth_still_stops_normally(
        self, tmp_path, fake_spawn, fake_procs
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo))
        with workers._worker_lock(record.name):
            assert workers._stop_generation(
                workers._read_record(record.name)
            ) is True
        assert record.pid not in fake_procs  # ours, verified stopped


class TestDeadLeaderLeaseProtection:
    """A bound lease NEVER disappears because the worker leader died:
    reconcile and live_workers retain the record untouched; a new
    dispatch may replace the dead generation but CARRIES the surviving
    leases forward."""

    def test_dead_leader_with_bound_lease_is_retained_untouched(
        self, tmp_path, fake_spawn, fake_procs, monkeypatch
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo), lease_id="run-x")
        assert workers.bind_lease(record.name, "run-x", "agent-1", "r-1")
        fake_procs.discard(record.pid)  # the leader died
        kills = []
        monkeypatch.setattr(
            workers, "_kill_detached", lambda r: kills.append(r.pid)
        )
        assert workers.reconcile() == []
        assert record.name not in {r.name for r in workers.live_workers()}
        persisted = workers._read_record(record.name)
        assert persisted is not None
        assert "run-x" in persisted.leases   # the lease evidence survives
        assert kills == []                   # remnants untouched

    def test_new_dispatch_replaces_dead_leader_but_carries_leases(
        self, tmp_path, fake_spawn, fake_procs
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        first = workers.ensure_worker(str(repo), lease_id="run-x")
        assert workers.bind_lease(first.name, "run-x", "agent-1", "r-1")
        fake_procs.discard(first.pid)
        second = workers.ensure_worker(str(repo), lease_id="run-y")
        assert second.generation != first.generation
        assert {"run-x", "run-y"} <= set(second.leases)
        lease = second.leases["run-x"]
        assert lease["agent_id"] == "agent-1"  # binding carried verbatim

    def test_live_workers_routes_dead_cleanup_through_verified_stop(
        self, tmp_path, fake_spawn, fake_procs, monkeypatch
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo))
        fake_procs.discard(record.pid)
        monkeypatch.setattr(workers, "_stop_generation", lambda r: False)
        assert workers.live_workers() == []
        retained = workers._read_record(record.name)
        assert retained is not None          # unverified stop: retained
        assert retained.state == workers.STATE_DRAINING

    def test_replacing_a_live_leased_worker_fails_closed(
        self, tmp_path, fake_spawn, monkeypatch
    ):
        """A LIVE worker holding a lease that stopped being adoptable
        (lost registration) is never replaced under an active lease —
        replacing would kill the run's processes."""
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo), lease_id="run-x")
        assert workers.bind_lease(record.name, "run-x", "agent-1", "r-1")
        monkeypatch.setattr(
            workers, "_probe_management",
            lambda r: {"status": "not_ready", "connected": False,
                       "claimed": False},
        )
        with pytest.raises(workers.WorkerError) as err:
            workers.ensure_worker(str(repo), lease_id="run-y")
        assert "lease" in str(err.value)
        assert workers._read_record(record.name) is not None


class TestAtomicEnsureAndLease:
    def test_ensure_with_lease_id_installs_the_lease_atomically(
        self, tmp_path, fake_spawn
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(
            str(repo), lease_id="run-1", lease_session="sess-a"
        )
        assert "run-1" in record.leases
        persisted = workers._read_record(record.name)
        assert "run-1" in persisted.leases
        assert persisted.leases["run-1"]["session"] == "sess-a"
        assert persisted.leases["run-1"]["holder_pid"] == os.getpid()
        assert persisted.leases["run-1"]["agent_id"] == ""  # not yet bound

    def test_bind_lease_attaches_remote_identity(self, tmp_path, fake_spawn):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo), lease_id="run-2")
        assert workers.bind_lease(record.name, "run-2", "agent-9", "r-9")
        lease = workers._read_record(record.name).leases["run-2"]
        assert lease["agent_id"] == "agent-9"
        assert lease["run_id"] == "r-9"

    def test_bind_lease_is_authoritative_about_failure(
        self, tmp_path, fake_spawn
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo), lease_id="run-3")
        assert workers.bind_lease(record.name, "run-GONE", "a", "r") is False
        assert workers.bind_lease("no-such-worker", "run-3", "a", "r") is False

    def test_bind_lease_rejects_empty_identity(self, tmp_path, fake_spawn):
        """A bound lease MEANS real remote identity: binding with an
        empty agent_id or run_id is rejected (the lease stays unbound —
        which now protects just as hard — rather than recording a lie)."""
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo), lease_id="run-4")
        assert workers.bind_lease(record.name, "run-4", "", "r-1") is False
        assert workers.bind_lease(record.name, "run-4", "agent-1", "") is False
        lease = workers._read_record(record.name).leases["run-4"]
        assert lease["agent_id"] == "" and lease["run_id"] == ""

    def test_release_settles_dead_holder_siblings_of_the_same_agent(
        self, tmp_path, fake_spawn, monkeypatch
    ):
        """Runs on ONE agent are sequential: this run's observed terminal
        is terminal proof for every EARLIER run of the same agent. A
        bound lease left by a crashed gateway is settled when a later
        run on the same agent releases — but a sibling with a LIVE
        holder (still executing) is untouched."""
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo), lease_id="run-old")
        assert workers.bind_lease(record.name, "run-old", "agent-X", "r-1")
        assert workers.acquire_lease(record.name, "run-live", session="s")
        assert workers.bind_lease(record.name, "run-live", "agent-X", "r-2")
        assert workers.acquire_lease(record.name, "run-new", session="s")
        assert workers.bind_lease(record.name, "run-new", "agent-X", "r-3")
        # run-old's holder died (crashed gateway); run-live's holder lives.
        dead = {"run-old"}
        monkeypatch.setattr(
            workers, "_holder_alive",
            lambda lease: lease.get("session_marker") not in dead,
        )
        with workers._worker_lock(record.name):
            current = workers._read_record(record.name)
            leases = dict(current.leases)
            for key in leases:
                leases[key] = {**leases[key], "session_marker": key}
            workers._update_record_locked(
                record.name, current.generation, leases=leases
            )

        workers.release_lease(record.name, "run-new", agent_id="agent-X")
        remaining = set(workers._read_record(record.name).leases)
        assert remaining == {"run-live"}  # stale settled, live retained


class TestLeasesAndReaping:
    def _age(self, name, seconds):
        with workers._worker_lock(name):
            record = workers._read_record(name)
            workers._update_record_locked(
                name, record.generation,
                last_active_at=time.time() - seconds,
            )

    def test_idle_leaseless_worker_is_reaped_after_ttl(
        self, tmp_path, fake_spawn, fake_procs
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo))
        self._age(record.name, workers.IDLE_TTL_S + 60)
        assert workers.reconcile() == [record.name]
        assert workers._read_record(record.name) is None
        assert record.pid not in fake_procs  # actually stopped

    def test_recent_leaseless_worker_is_not_reaped(
        self, tmp_path, fake_spawn
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo))
        assert workers.reconcile() == []
        assert workers._read_record(record.name) is not None

    def test_bound_lease_shields_from_ttl_even_with_dead_holder(
        self, tmp_path, fake_spawn, monkeypatch
    ):
        """The governing invariant: a lease BOUND to agent/run identity
        NEVER expires because the gateway pid died — the run lives
        server-side and only observed terminal proof releases it."""
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo), lease_id="run-b")
        assert workers.bind_lease(record.name, "run-b", "agent-1", "r-1")
        monkeypatch.setattr(workers, "_holder_alive", lambda lease: False)
        self._age(record.name, workers.IDLE_TTL_S + 3600)
        assert workers.reconcile() == []
        persisted = workers._read_record(record.name)
        assert persisted is not None
        assert "run-b" in persisted.leases  # conservatively retained

    def test_unbound_lease_with_dead_holder_still_protects(
        self, tmp_path, fake_spawn, monkeypatch
    ):
        """Every installed run lease is CREATE-UNCERTAIN: model
        validation happens before ensure, so by the time a lease exists
        the create POST may already have succeeded server-side even if
        the holder crashed before binding (or before reading the
        response). A dead holder therefore NEVER time-expires a lease —
        removal is explicit release or authoritative terminal proof
        only (the documented conservative orphan leak)."""
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo), lease_id="run-u")
        monkeypatch.setattr(workers, "_holder_alive", lambda lease: False)
        # Age acquisition and activity far past every historical window.
        with workers._worker_lock(record.name):
            current = workers._read_record(record.name)
            aged = {
                key: {**lease, "acquired_at": time.time() - 90000}
                for key, lease in current.leases.items()
            }
            workers._update_record_locked(
                record.name, current.generation,
                leases=aged,
                last_active_at=time.time() - workers.IDLE_TTL_S - 90000,
            )
        assert workers.reconcile() == []     # still protecting
        persisted = workers._read_record(record.name)
        assert persisted is not None
        assert "run-u" in persisted.leases
        # Explicit release settles it; the worker becomes reapable only
        # after a fresh full idle TTL.
        workers.release_lease(record.name, "run-u")
        assert workers.reconcile() == []
        with workers._worker_lock(record.name):
            current = workers._read_record(record.name)
            workers._update_record_locked(
                record.name, current.generation,
                last_active_at=time.time() - workers.IDLE_TTL_S - 60,
            )
        assert workers.reconcile() == [record.name]

    def test_release_bumps_the_idle_clock(self, tmp_path, fake_spawn):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo), lease_id="run-r")
        self._age(record.name, workers.IDLE_TTL_S + 60)
        workers.release_lease(record.name, "run-r")
        # Just-released -> full TTL of idle patience again.
        assert workers.reconcile() == []
        assert workers._read_record(record.name) is not None


class TestLazyReconcileHook:
    def test_ensure_reaps_idle_workers_before_capacity_evaluation(
        self, tmp_path, fake_spawn, monkeypatch
    ):
        """The lazy-cleanup guarantee: with NO timer, the next dispatch
        itself must clean positively-owned idle/leaseless workers before
        capacity is evaluated — an idle-past-TTL worker never costs a
        fresh dispatch its slot."""
        monkeypatch.setattr(workers, "_max_workers", lambda: 1)
        repo_a, repo_b = tmp_path / "a", tmp_path / "b"
        repo_a.mkdir(), repo_b.mkdir()
        rec_a = workers.ensure_worker(str(repo_a))
        with workers._worker_lock(rec_a.name):
            workers._update_record_locked(
                rec_a.name, rec_a.generation,
                last_active_at=time.time() - workers.IDLE_TTL_S - 60,
            )
        rec_b = workers.ensure_worker(str(repo_b), lease_id="run-n")
        assert rec_b.name != rec_a.name
        assert workers._read_record(rec_a.name) is None  # lazily reaped


class TestCapacity:
    def test_at_cap_reclaims_least_recently_active_leaseless_worker(
        self, tmp_path, fake_spawn, monkeypatch
    ):
        monkeypatch.setattr(workers, "_max_workers", lambda: 2)
        repos = []
        for key in ("a", "b", "c"):
            repo = tmp_path / key
            repo.mkdir()
            repos.append(repo)
        rec_a = workers.ensure_worker(str(repos[0]))
        rec_b = workers.ensure_worker(str(repos[1]))
        # Make A the least recently active (but NOT idle-past-TTL: this
        # is capacity pressure, not the TTL reaper).
        with workers._worker_lock(rec_a.name):
            workers._update_record_locked(
                rec_a.name, rec_a.generation,
                last_active_at=time.time() - 120,
            )
        rec_c = workers.ensure_worker(str(repos[2]))
        assert workers._read_record(rec_a.name) is None      # reclaimed
        assert workers._read_record(rec_b.name) is not None  # kept
        assert workers._read_record(rec_c.name) is not None

    def test_at_cap_with_all_slots_leased_fails_honestly(
        self, tmp_path, fake_spawn, monkeypatch
    ):
        # Config-driven cap (exercises the _plugin_config -> _max_workers
        # parsing path rather than patching _max_workers directly).
        monkeypatch.setattr(
            workers, "_plugin_config",
            lambda key: 1 if key == "max_workers" else None,
        )
        repo_a, repo_b = tmp_path / "a", tmp_path / "b"
        repo_a.mkdir(), repo_b.mkdir()
        rec_a = workers.ensure_worker(str(repo_a), lease_id="run-hold")
        with pytest.raises(workers.WorkerError) as err:
            workers.ensure_worker(str(repo_b), lease_id="run-want")
        message = str(err.value)
        assert "capacity" in message
        assert rec_a.name in message           # names the busy worker
        assert "max_workers" in message        # actionable remedy
        assert workers._read_record(rec_a.name) is not None

    def test_max_workers_rejects_junk_config(self, monkeypatch):
        for junk in ("lots", float("inf"), float("nan"), 0, -3, None):
            monkeypatch.setattr(
                workers, "_plugin_config", lambda key, junk=junk: junk
            )
            assert workers._max_workers() == 10


class TestSpawningReservationCapacity:
    """Admission startup accounting: a generation being spawned by
    ANOTHER process (reservation record on disk, its worker flock held)
    claims a capacity slot BEFORE its process exists — a concurrent
    ensure can never over-admit past the cap. Stale reservations (flock
    free: the spawner crashed) claim nothing and are lazily retired."""

    def _seed_reservation(self, repo):
        name = workers.worker_name_for(str(repo))
        directory = workers.state_dir()
        directory.mkdir(parents=True, exist_ok=True)
        (directory / f"{name}.json").write_text(json.dumps({
            "version": workers.RECORD_VERSION,
            "name": name,
            "repo_path": os.path.realpath(str(repo)),
            "pid": 0,                      # no process yet — reserving
            "log_path": str(directory / f"{name}-det-res.log"),
            "started_at": time.time(),
            "generation": "det-reserved",
            "state": workers.STATE_SPAWNING,
            "last_active_at": time.time(),
        }))
        return name

    def test_busy_spawning_reservation_claims_its_capacity_slot(
        self, tmp_path, fake_spawn, monkeypatch
    ):
        import fcntl

        monkeypatch.setattr(workers, "_max_workers", lambda: 1)
        repo_a, repo_b = tmp_path / "a", tmp_path / "b"
        repo_a.mkdir(), repo_b.mkdir()
        name_a = self._seed_reservation(repo_a)
        lock_path = workers.state_dir() / "locks" / f"{name_a}.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)  # the in-flight spawner's hold
            with pytest.raises(workers.WorkerError) as err:
                workers.ensure_worker(str(repo_b))
            assert "capacity" in str(err.value)
            assert workers._read_record(name_a) is not None  # untouched
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
        # The spawner crashed (flock free): the stale reservation claims
        # nothing and the next dispatch lazily retires it.
        record_b = workers.ensure_worker(str(repo_b))
        assert record_b.state == workers.STATE_READY
        assert workers._read_record(name_a) is None

    def test_spawn_runs_outside_the_global_admission_lock(
        self, tmp_path, monkeypatch, fake_procs
    ):
        """The (slow) spawn must not serialize every other worktree's
        admission behind it: during the spawn the machine-global
        admission lock is FREE."""
        import fcntl

        observed = {}

        def probing_spawn(name, repo_path, log_path, **kw):
            path = workers.state_dir() / "locks" / "admission.lock"
            path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o644)
            try:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    fcntl.flock(fd, fcntl.LOCK_UN)
                    observed["admission_free"] = True
                except OSError:
                    observed["admission_free"] = False
            finally:
                os.close(fd)
            fake_procs.add(4321)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_path.write_text(f"registering...\n{workers.READY_LINE}\n")
            return 4321

        monkeypatch.setattr(workers, "_spawn_worker", probing_spawn)
        repo = tmp_path / "repo"
        repo.mkdir()
        workers.ensure_worker(str(repo))
        assert observed["admission_free"] is True


class TestCanonicalAliasAdoption:
    """Migration from pre-canonical naming: a live worker recorded under
    a SUBDIRECTORY-derived name must be adopted for the canonical
    worktree — never duplicated, never killed. Multiple live candidates
    for one canonical checkout fail closed."""

    def _alias_record(self, repo, alias_name, pid, fake_procs):
        directory = workers.state_dir()
        directory.mkdir(parents=True, exist_ok=True)
        (directory / f"{alias_name}.json").write_text(json.dumps({
            "name": alias_name,
            "repo_path": os.path.realpath(str(repo)),  # canonical target
            "pid": pid,
            "log_path": str(directory / f"{alias_name}.log"),
            "started_at": 1000.0,
            "verified": True,
        }))
        fake_procs.add(pid)

    def test_live_subdir_named_worker_is_adopted_not_duplicated(
        self, tmp_path, fake_procs, fake_spawn
    ):
        repo = _make_repo(tmp_path / "repo")
        sub = repo / "src"
        sub.mkdir()
        # Old naming keyed the SUBDIR the user happened to dispatch from.
        alias = _raw_name(str(sub))
        assert alias != workers.worker_name_for(str(repo))
        self._alias_record(repo, alias, 8611, fake_procs)

        record = workers.ensure_worker(str(sub), lease_id="run-m")
        assert record.pid == 8611          # the live alias was adopted
        assert record.name == alias        # registered name preserved
        assert len(fake_spawn) == 0        # never duplicated
        assert "run-m" in record.leases    # lease installed atomically

    def test_two_live_candidates_for_one_checkout_fail_closed(
        self, tmp_path, fake_procs, fake_spawn
    ):
        repo = _make_repo(tmp_path / "repo")
        sub_a, sub_b = repo / "a", repo / "b"
        sub_a.mkdir(), sub_b.mkdir()
        alias_a = _raw_name(str(sub_a))
        alias_b = _raw_name(str(sub_b))
        self._alias_record(repo, alias_a, 8612, fake_procs)
        self._alias_record(repo, alias_b, 8613, fake_procs)

        with pytest.raises(workers.WorkerError) as err:
            workers.ensure_worker(str(repo))
        message = str(err.value).lower()
        assert "conflict" in message or "multiple" in message
        assert len(fake_spawn) == 0                    # no spawn
        assert 8612 in fake_procs and 8613 in fake_procs  # nothing killed

    def test_dead_alias_does_not_block_canonical_spawn(
        self, tmp_path, fake_procs, fake_spawn
    ):
        repo = _make_repo(tmp_path / "repo")
        sub = repo / "src"
        sub.mkdir()
        alias = _raw_name(str(sub))
        self._alias_record(repo, alias, 8614, fake_procs)
        fake_procs.discard(8614)  # the alias worker is dead

        record = workers.ensure_worker(str(repo))
        assert record.name == workers.worker_name_for(str(repo))
        assert len(fake_spawn) == 1  # fresh canonical spawn


def _mp_ensure_child(repo, xdg, alive_dir, spawn_log, cap, barrier, result_q):
    """Child body for the REAL multi-process admission tests (fork start
    method: the plugin module is inherited; seams are re-pointed at
    filesystem-backed fakes shared by every process)."""
    import time as _time

    os.environ["XDG_STATE_HOME"] = xdg
    workers._reset_migration_for_tests()
    workers.READY_TIMEOUT_S = 10.0
    workers._READY_POLL_S = 0.01
    workers._systemd_available = lambda: False
    workers._probe_management = lambda record: (
        {"status": "ok", "connected": True, "claimed": False}
        if record.state == workers.STATE_READY
        else None
    )
    workers._pid_alive = (
        lambda pid: (Path(alive_dir) / str(int(pid))).exists()
    )
    workers._pid_exists = (
        lambda pid: (Path(alive_dir) / str(int(pid))).exists()
    )
    workers._pid_birth = lambda pid: 1
    workers._kill_detached = lambda record: (
        (Path(alive_dir) / str(int(record.pid))).unlink(missing_ok=True)
    )
    workers._pgid_empty = (
        lambda pgid: not (Path(alive_dir) / str(int(pgid))).exists()
    )
    workers._max_workers = lambda: int(cap)

    def spawn(name, repo_path, log_path, **kw):
        _time.sleep(0.3)  # hold the locks long enough for a real race
        with open(spawn_log, "a") as fh:
            fh.write(f"{name}\n")
        with open(spawn_log) as fh:
            pid = 50000 + sum(1 for _ in fh)
        (Path(alive_dir) / str(pid)).touch()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, "a") as fh:
            fh.write(f"registering...\n{workers.READY_LINE}\n")
        return pid

    workers._spawn_worker = spawn
    barrier.wait(timeout=10)  # both processes race the SAME instant
    try:
        record = workers.ensure_worker(str(repo), lease_id=f"run-{os.getpid()}")
        result_q.put(("ok", record.pid, record.generation))
    except workers.WorkerError as exc:
        result_q.put(("err", str(exc), ""))


def _mp_migrate_child(xdg, hermes_home, alive_dir, barrier, result_q):
    """Child body for the concurrent-migration proof (fork start
    method): re-runs the pre-v2 profile migration from scratch in this
    process, racing its sibling."""
    os.environ["XDG_STATE_HOME"] = xdg
    os.environ["HERMES_HOME"] = hermes_home
    workers._reset_migration_for_tests()
    workers._pid_alive = (
        lambda pid: (Path(alive_dir) / str(int(pid))).exists()
    )
    workers._pid_exists = (
        lambda pid: (Path(alive_dir) / str(int(pid))).exists()
    )
    workers._pid_birth = lambda pid: 1
    barrier.wait(timeout=10)
    try:
        names = sorted(r.name for r in workers.live_workers())
        result_q.put(("ok", names))
    except Exception as exc:  # noqa: BLE001
        result_q.put(("err", repr(exc)))


class TestReleaseBoundLease:
    """The restart bridge's workers API: release strictly by POSITIVE
    worker + agent_id + run_id match — never by session text, never by
    timer, never an unbound (create-uncertain) lease. Idempotent and
    generation-safe under the worker flock."""

    def _leased(self, tmp_path, agent="agent-1", run="r-1"):
        repo = tmp_path / "repo"
        repo.mkdir(exist_ok=True)
        record = workers.ensure_worker(str(repo), lease_id="lease-1")
        assert workers.bind_lease(record.name, "lease-1", agent, run)
        return record.name

    def test_releases_the_exact_agent_run_match(self, tmp_path, fake_spawn):
        name = self._leased(tmp_path)
        assert workers.release_bound_lease(
            name, agent_id="agent-1", run_id="r-1"
        ) is True
        assert workers._read_record(name).leases == {}

    def test_mismatched_run_or_agent_releases_nothing(
        self, tmp_path, fake_spawn
    ):
        name = self._leased(tmp_path)
        assert not workers.release_bound_lease(
            name, agent_id="agent-1", run_id="r-OTHER"
        )
        assert not workers.release_bound_lease(
            name, agent_id="agent-OTHER", run_id="r-1"
        )
        assert "lease-1" in workers._read_record(name).leases

    def test_unbound_lease_is_never_matched(self, tmp_path, fake_spawn):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo), lease_id="lease-u")
        # Empty identity must never wildcard onto the unbound lease.
        assert not workers.release_bound_lease(
            record.name, agent_id="", run_id=""
        )
        assert not workers.release_bound_lease(
            record.name, agent_id="agent-1", run_id="r-1"
        )
        assert "lease-u" in workers._read_record(record.name).leases

    def test_idempotent_and_safe_on_missing_worker(
        self, tmp_path, fake_spawn
    ):
        name = self._leased(tmp_path)
        assert workers.release_bound_lease(name, "agent-1", "r-1")
        assert workers.release_bound_lease(name, "agent-1", "r-1") is False
        assert workers.release_bound_lease("no-such-worker", "a", "r") is False

    def test_sibling_leases_on_other_runs_survive(
        self, tmp_path, fake_spawn
    ):
        name = self._leased(tmp_path)
        # A follow-up run's lease on the same worker, bound elsewhere.
        assert workers.acquire_lease(name, "lease-2", "sess")
        assert workers.bind_lease(name, "lease-2", "agent-1", "r-2")
        assert workers.release_bound_lease(name, "agent-1", "r-1")
        remaining = workers._read_record(name).leases
        assert set(remaining) == {"lease-2"}


class TestMigrationLockNonRecursive:
    """Regression: the migration flock must never be re-entered through
    state_dir from helpers running UNDER it (collision noting writes the
    conflict marker record via _write_record → _record_path, which must
    not resolve state_dir into a second migration attempt — flock
    conflicts across open-file-descriptions even within one process)."""

    def test_collision_migration_does_not_self_deadlock(
        self, tmp_path, fake_procs
    ):
        import threading

        hermes_home = Path(os.environ["HERMES_HOME"])
        repo = tmp_path / "repo"
        repo.mkdir()
        name = workers.worker_name_for(str(repo))
        for directory, pid in (
            (hermes_home / "state" / "ghost_cursor" / "workers", 9941),
            (
                hermes_home / "profiles" / "beta" / "state"
                / "ghost_cursor" / "workers",
                9942,
            ),
        ):
            directory.mkdir(parents=True, exist_ok=True)
            (directory / f"{name}.json").write_text(json.dumps({
                "name": name,
                "repo_path": os.path.realpath(str(repo)),
                "pid": pid,
                "log_path": str(directory / f"{name}.log"),
                "started_at": 1000.0,
                "verified": True,
            }))
        fake_procs.update({9941, 9942})
        workers._reset_migration_for_tests()

        done = threading.Event()

        def migrate():
            # The collision path runs during the FIRST state_dir
            # resolution of this process — live_workers triggers it.
            workers.live_workers()
            done.set()

        thread = threading.Thread(target=migrate, daemon=True)
        thread.start()
        assert done.wait(timeout=15), (
            "migration deadlocked on its own migration lock "
            "(state_dir re-entry under the held flock)"
        )
        # And the collision was still recorded correctly.
        candidates = workers._read_conflict_candidates(name)
        assert candidates is not None
        assert {int(c["pid"]) for c in candidates} == {9941, 9942}


class TestMigrationThreadCoordination:
    """In-process coordination around the one-shot migration pass: a
    second thread must never see the target before legacy ownership
    migration completed — it WAITS, then observes the outcome. Failure
    leaves the target retryable (next call attempts migration again);
    success is recorded only when the pass actually succeeded."""

    def test_second_thread_blocks_until_the_active_migration_finishes(
        self, tmp_path, monkeypatch
    ):
        import threading

        entered = threading.Event()
        release = threading.Event()
        passes = []
        real = workers._migrate_profile_records_serialized

        def slow(target):
            passes.append(threading.get_ident())
            entered.set()
            assert release.wait(10)
            return real(target)

        monkeypatch.setattr(
            workers, "_migrate_profile_records_serialized", slow
        )
        workers._reset_migration_for_tests()

        results = {}
        thread_a = threading.Thread(
            target=lambda: results.__setitem__("a", workers.state_dir()),
            daemon=True,
        )
        thread_a.start()
        assert entered.wait(10)
        thread_b = threading.Thread(
            target=lambda: results.__setitem__("b", workers.state_dir()),
            daemon=True,
        )
        thread_b.start()
        # B must be BLOCKED behind A's in-flight migration — not handed
        # the target while legacy ownership is still being migrated.
        time.sleep(0.3)
        assert "b" not in results
        release.set()
        thread_a.join(timeout=10)
        thread_b.join(timeout=10)
        assert not thread_a.is_alive() and not thread_b.is_alive()
        # Both observed the SAME migrated target; the pass ran ONCE.
        assert results["a"] == results["b"]
        assert len(passes) == 1

    def test_reset_refuses_while_a_migration_is_running(
        self, tmp_path, monkeypatch
    ):
        """Clearing an ACTIVE gate would let a new caller create a second
        gate for the same target and run a concurrent migration — reset
        must fail loudly instead, and late callers must keep
        coordinating on the ORIGINAL gate."""
        import threading

        entered = threading.Event()
        release = threading.Event()
        calls = []

        def gated(target):
            calls.append(1)
            entered.set()
            assert release.wait(10)
            return True

        monkeypatch.setattr(
            workers, "_migrate_profile_records_serialized", gated
        )
        workers._reset_migration_for_tests()

        results = {}
        thread_a = threading.Thread(
            target=lambda: results.__setitem__("a", workers.state_dir()),
            daemon=True,
        )
        thread_a.start()
        assert entered.wait(10)

        with pytest.raises(RuntimeError):
            workers._reset_migration_for_tests()

        # A second caller still coordinates on the original gate: it
        # BLOCKS behind the active pass rather than starting its own.
        thread_b = threading.Thread(
            target=lambda: results.__setitem__("b", workers.state_dir()),
            daemon=True,
        )
        thread_b.start()
        time.sleep(0.3)
        assert "b" not in results

        release.set()
        thread_a.join(timeout=10)
        thread_b.join(timeout=10)
        assert results["a"] == results["b"]
        assert len(calls) == 1  # ONE pass — no concurrent migration

        # Quiescent now — reset is allowed again.
        workers._reset_migration_for_tests()

    def test_failed_pass_degrades_every_queued_waiter_without_a_convoy(
        self, tmp_path, monkeypatch
    ):
        """Attempt-outcome semantics: waiters queued behind a FAILING
        pass observe that attempt's failure and degrade for their own
        call — they never serially re-run the migration (3 waiters must
        not mean 3 passes). Only a later fresh call retries, once."""
        import threading

        entered = threading.Event()
        release = threading.Event()
        calls = []

        def flaky(target):
            calls.append(1)
            if len(calls) == 1:
                entered.set()
                assert release.wait(10)
                return False  # the whole wave observes ONE failure
            return True

        monkeypatch.setattr(
            workers, "_migrate_profile_records_serialized", flaky
        )
        workers._reset_migration_for_tests()

        expected = (
            Path(os.environ["XDG_STATE_HOME"]) / "ghost_cursor" / "workers"
        )
        gate = workers._migration_gate(str(expected))
        results = []
        owner = threading.Thread(
            target=lambda: results.append(workers.state_dir()), daemon=True
        )
        owner.start()
        assert entered.wait(10)
        waiters = [
            threading.Thread(
                target=lambda: results.append(workers.state_dir()),
                daemon=True,
            )
            for _ in range(3)
        ]
        for thread in waiters:
            thread.start()
        # All three must be queued on THIS attempt before it fails —
        # otherwise a slow starter would legitimately count as a fresh
        # call and retry.
        deadline = time.time() + 10
        while len(gate.cond._waiters) < 3:
            assert time.time() < deadline, "waiters never queued"
            time.sleep(0.01)

        release.set()
        for thread in (owner, *waiters):
            thread.join(timeout=10)
            assert not thread.is_alive()
        # Every caller of the wave returned after the SAME settlement,
        # from exactly ONE (failed) pass.
        assert len(results) == 4
        assert all(path == expected for path in results)
        assert len(calls) == 1

        # A separate later call — one that never waited on the failed
        # attempt — retries exactly once and succeeds.
        assert workers.state_dir() == expected
        assert len(calls) == 2
        workers.state_dir()
        assert len(calls) == 2  # one-shot after success

    def test_failed_migration_is_retried_on_the_next_call(
        self, tmp_path, monkeypatch, fake_procs
    ):
        hermes_home = Path(os.environ["HERMES_HOME"])
        repo = tmp_path / "repo"
        repo.mkdir()
        name = workers.worker_name_for(str(repo))
        legacy = hermes_home / "state" / "ghost_cursor" / "workers"
        legacy.mkdir(parents=True, exist_ok=True)
        (legacy / f"{name}.json").write_text(json.dumps({
            "name": name,
            "repo_path": os.path.realpath(str(repo)),
            "pid": 9951,
            "log_path": str(legacy / f"{name}.log"),
            "started_at": 1000.0,
            "verified": True,
        }))
        fake_procs.add(9951)

        calls = []
        real_inner = workers._migrate_profile_records

        def flaky(target):
            calls.append(1)
            if len(calls) == 1:
                raise OSError("disk exploded mid-migration")
            return real_inner(target)

        monkeypatch.setattr(workers, "_migrate_profile_records", flaky)
        workers._reset_migration_for_tests()

        # First call: the pass fails — state_dir still returns a usable
        # target (degraded), but migration is NOT marked complete and
        # the legacy record was not adopted.
        target = workers.state_dir()
        assert len(calls) == 1
        assert not (target / f"{name}.json").exists()

        # Next call: the migration is ATTEMPTED AGAIN and succeeds —
        # the legacy record is adopted.
        assert workers.state_dir() == target
        assert len(calls) == 2
        assert (target / f"{name}.json").exists()

        # And now it is genuinely one-shot: no further passes.
        workers.state_dir()
        assert len(calls) == 2


class TestConcurrentMigration:
    """Legacy migration is cross-process serialized and atomic: two
    processes migrating the same profiles concurrently can neither lose
    collision evidence nor adopt/overwrite past each other."""

    def test_concurrent_collision_migration_preserves_both_candidates(
        self, tmp_path, fake_procs
    ):
        import multiprocessing

        hermes_home = Path(os.environ["HERMES_HOME"])
        repo = tmp_path / "repo"
        repo.mkdir()
        name = workers.worker_name_for(str(repo))
        # Two profiles claim the SAME worker name with DIFFERENT live
        # pids — the collision every process must preserve, not race.
        seeds = (
            (hermes_home / "state" / "ghost_cursor" / "workers", 9931),
            (
                hermes_home / "profiles" / "beta" / "state"
                / "ghost_cursor" / "workers",
                9932,
            ),
        )
        for directory, pid in seeds:
            directory.mkdir(parents=True, exist_ok=True)
            (directory / f"{name}.json").write_text(json.dumps({
                "name": name,
                "repo_path": os.path.realpath(str(repo)),
                "pid": pid,
                "log_path": str(directory / f"{name}.log"),
                "started_at": 1000.0,
                "verified": True,
            }))
        alive_dir = tmp_path / "alive"
        alive_dir.mkdir()
        (alive_dir / "9931").touch()
        (alive_dir / "9932").touch()
        fake_procs.update({9931, 9932})

        ctx = multiprocessing.get_context("fork")
        barrier = ctx.Barrier(2)
        result_q = ctx.Queue()
        children = [
            ctx.Process(
                target=_mp_migrate_child,
                args=(
                    os.environ["XDG_STATE_HOME"],
                    str(hermes_home), str(alive_dir), barrier, result_q,
                ),
            )
            for _ in range(2)
        ]
        for child in children:
            child.start()
        for child in children:
            child.join(timeout=30)
            assert child.exitcode == 0
        results = [result_q.get(timeout=5) for _ in children]
        assert all(kind == "ok" for kind, _ in results), results

        # The collision survived the race: BOTH candidates preserved,
        # the record an explicit conflict, nothing adopted or lost.
        candidates = workers._read_conflict_candidates(name)
        assert candidates is not None
        assert {int(c["pid"]) for c in candidates} == {9931, 9932}
        record = workers._read_record(name)
        assert record is not None
        assert record.state == workers.STATE_CONFLICT
        # Both source profile records still exist (adoption never
        # deletes originals; nothing was overwritten past the lock).
        for directory, _pid in seeds:
            assert (directory / f"{name}.json").exists()


class TestMultiProcessAdmission:
    """Cross-PROCESS admission: flock + the machine-global admission lock
    must hold between real interpreters, not just threads."""

    def _run_children(self, tmp_path, repos, cap):
        import multiprocessing

        ctx = multiprocessing.get_context("fork")
        alive_dir = tmp_path / "alive"
        alive_dir.mkdir()
        spawn_log = tmp_path / "spawns.log"
        spawn_log.touch()
        barrier = ctx.Barrier(len(repos))
        result_q = ctx.Queue()
        children = [
            ctx.Process(
                target=_mp_ensure_child,
                args=(
                    str(repo), os.environ["XDG_STATE_HOME"], str(alive_dir),
                    str(spawn_log), cap, barrier, result_q,
                ),
            )
            for repo in repos
        ]
        for child in children:
            child.start()
        for child in children:
            child.join(timeout=30)
            assert child.exitcode == 0
        results = [result_q.get(timeout=5) for _ in repos]
        spawns = spawn_log.read_text().splitlines()
        return results, spawns

    def test_two_processes_same_worktree_spawn_exactly_one_generation(
        self, tmp_path
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        results, spawns = self._run_children(tmp_path, [repo, repo], cap=10)
        assert len(spawns) == 1, "exactly one spawn may win the flock"
        oks = [r for r in results if r[0] == "ok"]
        assert len(oks) == 2, f"both processes must get the worker: {results}"
        assert len({(pid, gen) for _, pid, gen in oks}) == 1, (
            "both processes must see ONE generation"
        )

    def test_two_processes_distinct_worktrees_respect_the_global_cap(
        self, tmp_path
    ):
        repo_a, repo_b = tmp_path / "a", tmp_path / "b"
        repo_a.mkdir(), repo_b.mkdir()
        results, spawns = self._run_children(
            tmp_path, [repo_a, repo_b], cap=1
        )
        assert len(spawns) == 1, "the admission lock must gate BOTH processes"
        oks = [r for r in results if r[0] == "ok"]
        errs = [r for r in results if r[0] == "err"]
        assert len(oks) == 1 and len(errs) == 1, results
        assert "capacity" in errs[0][1]


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
