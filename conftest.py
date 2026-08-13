"""Test harness for the standalone ghost_cursor repo.

In the Hermes tree, an autouse conftest fixture isolates HERMES_HOME to a
temp dir so persisted state (e.g. the handle table's JSON file) never leaks
between tests or from the developer's real ~/.hermes. Standalone, we provide
the same isolation here so `pytest` is hermetic — which is also what CI needs.
"""
import os
import sys
from pathlib import Path

import pytest

# Make `import plugins.ghost_cursor...` resolve to this directory's files by
# exposing them under a `plugins.ghost_cursor` package shim if needed. In the
# Hermes tree the real package is importable and this is a no-op. Standalone,
# the plugin's intra-package imports are RELATIVE (Hermes loads user plugins
# as `hermes_plugins.<slug>`, so no absolute package name is stable), which
# means the modules need real package context — a bare sys.path insert isn't
# enough. Synthesize the package with importlib instead.
_here = Path(__file__).parent


def _ensure_plugin_package() -> None:
    try:
        import plugins.ghost_cursor  # noqa: F401  (real package in a Hermes tree)
        # A REAL package has __file__; an empty stub directory in a Hermes
        # tree (e.g. plugins/ghost_cursor/ holding only a .pytest_cache)
        # imports as a NAMESPACE package (__file__ is None) and would
        # shadow the shim — fall through and replace it in that case.
        if getattr(sys.modules["plugins.ghost_cursor"], "__file__", None):
            return
        del sys.modules["plugins.ghost_cursor"]
    except ModuleNotFoundError:
        pass
    # Only shim when this conftest actually sits beside the plugin sources
    # (standalone repo layout), never from a copied spot in a Hermes tree.
    if not (_here / "cloud_runner.py").is_file():
        raise ModuleNotFoundError("plugins.ghost_cursor")
    import importlib.util
    import types

    if "plugins" not in sys.modules:
        ns = types.ModuleType("plugins")
        ns.__path__ = []
        sys.modules["plugins"] = ns
    spec = importlib.util.spec_from_file_location(
        "plugins.ghost_cursor",
        _here / "__init__.py",
        submodule_search_locations=[str(_here)],
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["plugins.ghost_cursor"] = module
    spec.loader.exec_module(module)
    sys.modules["plugins"].ghost_cursor = module
    # pytest sees the repo root as a package (it has __init__.py) and imports
    # it as a bare `__init__` module, where relative imports can't resolve.
    # Alias the already-loaded package so pytest reuses it instead.
    sys.modules.setdefault("__init__", module)


_ensure_plugin_package()


@pytest.fixture(autouse=True)
def _isolate_hermes_home(tmp_path, monkeypatch):
    """Point HERMES_HOME at a per-test temp dir so the handle-table JSON and
    any other persisted state are fresh every test and never touch the real
    ~/.hermes. Mirrors the Hermes-tree autouse fixture the plugin tests assume.

    XDG_STATE_HOME is isolated too: the worker controller's machine-global
    control directory (records/locks/data roots) lives under it and must
    never touch the real ~/.local/state during tests.
    """
    home = tmp_path / "hermes_home"
    (home / "state").mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "xdg_state"))
    try:
        from plugins.ghost_cursor import workers as _workers

        _workers._reset_migration_for_tests()
    except Exception:
        pass
    yield home
