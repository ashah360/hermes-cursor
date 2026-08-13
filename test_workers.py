"""Tests for workers.py — the worker controller, with a faked process
table (``_pid_alive``) and a faked spawner (``_spawn_worker``)."""

import json
import os
import subprocess

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
    """A fake process table: pids in the set are alive."""
    alive = set()
    monkeypatch.setattr(workers, "_pid_alive", lambda pid: int(pid) in alive)
    return alive


@pytest.fixture
def fake_spawn(monkeypatch, fake_procs):
    """A fake spawner that 'starts' pids 1000, 1001, ... and (by default)
    writes the ready line to the log immediately."""
    calls = []

    def spawn(name, repo_path, log_path):
        pid = 1000 + len(calls)
        calls.append({"name": name, "repo_path": repo_path, "log_path": log_path})
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

        def spawn_silent(name, repo_path, log_path):
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

        def spawn_dying(name, repo_path, log_path):
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

        def spawn_slow(name, repo_path, log_path):
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
