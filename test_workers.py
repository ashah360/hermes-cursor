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


@pytest.fixture
def fake_procs(monkeypatch):
    """A fake process table: pids in the set are alive. Kills operate on
    the fake table (never signal real processes)."""
    alive = set()
    monkeypatch.setattr(workers, "_pid_alive", lambda pid: int(pid) in alive)
    monkeypatch.setattr(
        workers, "_kill_detached",
        lambda record: alive.discard(int(record.pid)),
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

    def start(unit, argv, env, cwd, log_path):
        pid = 9000 + len(state["started"])
        state["started"].append({
            "unit": unit, "argv": list(argv), "env": dict(env),
            "cwd": cwd, "log_path": log_path,
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
    monkeypatch.setattr(workers, "_systemd_start", start)
    monkeypatch.setattr(workers, "_systemd_show", show)
    monkeypatch.setattr(workers, "_systemd_stop", stop)
    monkeypatch.setattr(workers, "_linger_enabled", lambda: True)
    return state


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
            {"CURSOR_DATA_DIR": "/state/data/w", "PATH": "/usr/bin",
             "HOME": "/home/u", "IRRELEVANT": "x"},
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
        assert cmd[-5:] == ["--", "/usr/bin/agent", "worker", "start",
                            "--name", "w"][-5:]

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


class TestLeasesAndReaping:
    """Per-RUN leases protect a worker from every cleanup path; the idle
    reaper takes only leaseless workers past the TTL."""

    def _backdate(self, name, seconds):
        record = workers._read_record(name)
        with workers._worker_lock(name):
            workers._update_record_locked(
                name, record.generation,
                last_active_at=record.last_active_at - seconds,
            )

    def test_reaper_never_evicts_a_leased_worker(self, tmp_path, fake_spawn):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo))
        assert workers.acquire_lease(record.name, "run-1", session="s")
        self._backdate(record.name, workers.IDLE_TTL_S * 10)

        reaped = workers.reconcile()
        assert reaped == []
        assert workers._read_record(record.name) is not None

    def test_idle_leaseless_worker_reaped_after_ttl(self, tmp_path, fake_spawn):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo))
        self._backdate(record.name, workers.IDLE_TTL_S * 10)

        reaped = workers.reconcile()
        assert reaped == [record.name]
        assert workers._read_record(record.name) is None

    def test_release_clears_lease_and_bumps_activity(self, tmp_path, fake_spawn):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo))
        workers.acquire_lease(record.name, "run-1")
        assert workers._read_record(record.name).leases
        workers.release_lease(record.name, "run-1")
        after = workers._read_record(record.name)
        assert after.leases == {}
        # A fresh idle clock: the just-released worker is not reap-bait.
        assert workers.reconcile() == []

    def test_stale_lease_of_dead_holder_expires(self, tmp_path, fake_spawn, monkeypatch):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo))
        # A lease whose holder process died (crash) and whose grace has
        # passed no longer protects the worker.
        workers.acquire_lease(record.name, "run-crashed")
        rec = workers._read_record(record.name)
        stale = dict(rec.leases["run-crashed"])
        stale["holder_pid"] = 999999999  # never alive
        stale["acquired_at"] = stale["acquired_at"] - workers.LEASE_STALE_GRACE_S * 10
        with workers._worker_lock(record.name):
            workers._update_record_locked(
                record.name, rec.generation, leases={"run-crashed": stale},
            )
        self._backdate(record.name, workers.IDLE_TTL_S * 10)

        assert workers.reconcile() == [record.name]

    def test_lease_on_unknown_record_is_a_noop(self):
        assert workers.acquire_lease("no-such-worker", "run-1") is False
        workers.release_lease("no-such-worker", "run-1")  # never raises


class TestAtomicEnsureAndLease:
    def test_ensure_installs_the_lease_under_one_lock_hold(
        self, tmp_path, fake_spawn
    ):
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(
            str(repo), lease_id="run-1", lease_session="My session"
        )
        assert "run-1" in record.leases
        persisted = workers._read_record(record.name)
        assert persisted.leases["run-1"]["session"] == "My session"

    def test_bound_lease_survives_holder_death_until_remote_terminal(
        self, tmp_path, fake_spawn
    ):
        """A lease bound to a real agent/run protects the worker across a
        gateway restart (dead holder): only an OBSERVED remote terminal
        releases it — never a timer."""
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo), lease_id="run-1")
        workers.bind_lease(record.name, "run-1", "bc-agent-1", "run-abc")
        rec = workers._read_record(record.name)
        lease = dict(rec.leases["run-1"])
        lease["holder_pid"] = 999999999  # holder crashed
        lease["acquired_at"] = lease["acquired_at"] - 999999.0
        with workers._worker_lock(record.name):
            workers._update_record_locked(
                record.name, rec.generation,
                leases={"run-1": lease},
                last_active_at=rec.last_active_at - 999999.0,
            )

        assert workers.reconcile() == []          # still protected
        workers.release_lease(record.name, "run-1")
        # Release grants a fresh idle TTL; age it out again to prove the
        # protection is really gone.
        rec = workers._read_record(record.name)
        with workers._worker_lock(record.name):
            workers._update_record_locked(
                record.name, rec.generation,
                last_active_at=rec.last_active_at - 999999.0,
            )
        assert workers.reconcile() == [record.name]  # released → reapable

    def test_unbound_lease_of_live_holder_expires_eventually(
        self, tmp_path, fake_spawn
    ):
        """A dispatch lease that never bound to an agent (create failed /
        never happened) must not protect the worker forever even while
        its holder process lives on."""
        repo = tmp_path / "repo"
        repo.mkdir()
        record = workers.ensure_worker(str(repo), lease_id="run-1")
        rec = workers._read_record(record.name)
        lease = dict(rec.leases["run-1"])  # holder = this live process
        lease["acquired_at"] = (
            lease["acquired_at"] - workers.UNBOUND_LEASE_MAX_AGE_S * 10
        )
        with workers._worker_lock(record.name):
            workers._update_record_locked(
                record.name, rec.generation,
                leases={"run-1": lease},
                last_active_at=rec.last_active_at - 999999.0,
            )
        assert workers.reconcile() == [record.name]

    def test_release_leases_for_session(self, tmp_path, fake_spawn):
        repo_a, repo_b = tmp_path / "a", tmp_path / "b"
        repo_a.mkdir(), repo_b.mkdir()
        rec_a = workers.ensure_worker(
            str(repo_a), lease_id="run-a", lease_session="Session A"
        )
        rec_b = workers.ensure_worker(
            str(repo_b), lease_id="run-b", lease_session="Session B"
        )
        workers.release_leases_for_session("Session A")
        assert workers._read_record(rec_a.name).leases == {}
        assert workers._read_record(rec_b.name).leases != {}


class TestGlobalAdmission:
    def test_concurrent_distinct_worktrees_cannot_exceed_the_cap(
        self, tmp_path, monkeypatch, fake_procs
    ):
        """Two different worktrees ensured concurrently at limit-1 must
        not BOTH spawn: capacity reservation + spawn + record creation
        are one machine-global critical section."""
        import threading as _threading
        import time as _time

        monkeypatch.setattr(workers, "_max_workers", lambda: 1)
        repos = []
        for label in ("a", "b"):
            repo = tmp_path / label
            repo.mkdir()
            repos.append(repo)
        spawned = []

        def slow_spawn(name, repo_path, log_path, **kw):
            _time.sleep(0.2)  # widen the race window
            pid = 7100 + len(spawned)
            spawned.append(name)
            fake_procs.add(pid)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(log_path, "a") as fh:
                fh.write(f"{workers.READY_LINE}\n")
            return pid

        monkeypatch.setattr(workers, "_spawn_worker", slow_spawn)
        results, errors = [], []

        def ensure(repo, run_id):
            try:
                results.append(
                    workers.ensure_worker(str(repo), lease_id=run_id)
                )
            except workers.WorkerError as exc:
                errors.append(exc)

        threads = [
            _threading.Thread(target=ensure, args=(repo, f"run-{i}"))
            for i, repo in enumerate(repos)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)

        assert len(results) == 1, "exactly one spawn may win at the cap"
        assert len(errors) == 1
        assert "capacity" in str(errors[0])
        live = workers.live_workers()
        assert len(live) <= 1


class TestCapacity:
    def test_reclaims_idle_leaseless_worker_before_spawning(
        self, tmp_path, fake_spawn, monkeypatch
    ):
        monkeypatch.setattr(workers, "_max_workers", lambda: 2)
        repo_a, repo_b, repo_c = tmp_path / "a", tmp_path / "b", tmp_path / "c"
        for r in (repo_a, repo_b, repo_c):
            r.mkdir()
        rec_a = workers.ensure_worker(str(repo_a))
        rec_b = workers.ensure_worker(str(repo_b))
        workers.acquire_lease(rec_b.name, "run-b")  # b is BUSY

        rec_c = workers.ensure_worker(str(repo_c))  # at cap: a reclaimed
        assert rec_c is not None
        assert workers._read_record(rec_a.name) is None      # idle a evicted
        assert workers._read_record(rec_b.name) is not None  # leased b kept

    def test_honest_quota_exhaustion_when_all_leased(
        self, tmp_path, fake_spawn, monkeypatch
    ):
        monkeypatch.setattr(workers, "_max_workers", lambda: 2)
        repo_a, repo_b, repo_c = tmp_path / "a", tmp_path / "b", tmp_path / "c"
        for r in (repo_a, repo_b, repo_c):
            r.mkdir()
        rec_a = workers.ensure_worker(str(repo_a))
        rec_b = workers.ensure_worker(str(repo_b))
        workers.acquire_lease(rec_a.name, "run-a")
        workers.acquire_lease(rec_b.name, "run-b")

        with pytest.raises(workers.WorkerError) as err:
            workers.ensure_worker(str(repo_c))
        message = str(err.value)
        assert "capacity" in message or "quota" in message
        # Honest: names the busy workers; nothing was evicted or queued.
        assert workers._read_record(rec_a.name) is not None
        assert workers._read_record(rec_b.name) is not None


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
