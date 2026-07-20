"""Unit tests for scripts/discover_suites.py."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import discover_suites  # noqa: E402


def _add_suite(package_dir: Path, suite: str) -> None:
    (package_dir / f"template_{suite}.yaml").write_text("Resources: {}\n")
    handlers_dir = package_dir / "handlers" / suite
    handlers_dir.mkdir(parents=True)
    (handlers_dir / "case.py").write_text("def handler():\n    pass\n")


def test_discovers_suites_from_templates_in_sorted_order(tmp_path: Path) -> None:
    _add_suite(tmp_path, "zeta")
    _add_suite(tmp_path, "alpha")

    assert discover_suites.discover_suites(tmp_path) == ("alpha", "zeta")


def test_ignores_non_template_yaml_files(tmp_path: Path) -> None:
    _add_suite(tmp_path, "example")
    (tmp_path / "samconfig.yaml").write_text("version: 0.1\n")

    assert discover_suites.discover_suites(tmp_path) == ("example",)


def test_fails_when_no_templates_exist(tmp_path: Path) -> None:
    with pytest.raises(SystemExit, match="No template_<suite>.yaml files found"):
        discover_suites.discover_suites(tmp_path)


def test_fails_when_handler_directory_is_missing(tmp_path: Path) -> None:
    (tmp_path / "template_example.yaml").write_text("Resources: {}\n")

    with pytest.raises(SystemExit, match="no matching handler directory"):
        discover_suites.discover_suites(tmp_path)


def test_fails_when_handler_directory_is_empty(tmp_path: Path) -> None:
    (tmp_path / "template_example.yaml").write_text("Resources: {}\n")
    (tmp_path / "handlers" / "example").mkdir(parents=True)

    with pytest.raises(SystemExit, match="No handler modules found"):
        discover_suites.discover_suites(tmp_path)
