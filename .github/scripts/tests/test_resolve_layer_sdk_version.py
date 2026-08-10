from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest


sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from resolve_layer_sdk_version import resolve_layer_sdk_version


def _write_otel_pyproject(tmp_path: Path, sdk_version: str) -> Path:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(f'[tool.lambda-layer]\nsdk-version = "{sdk_version}"\n')
    return pyproject


def test_otel_only_release_uses_pinned_published_sdk(tmp_path: Path) -> None:
    pyproject = _write_otel_pyproject(tmp_path, "1.8.0")

    assert (
        resolve_layer_sdk_version(
            event_name="release",
            release_tag="otel-v0.4.1",
            source_sdk_version="1.9.0",
            otel_pyproject=pyproject,
        )
        == "1.8.0"
    )


def test_combined_release_uses_source_sdk(tmp_path: Path) -> None:
    pyproject = _write_otel_pyproject(tmp_path, "1.9.0")

    assert (
        resolve_layer_sdk_version(
            event_name="release",
            release_tag="sdk-v1.9.0,otel-v0.5.0",
            source_sdk_version="1.9.0",
            otel_pyproject=pyproject,
        )
        == "1.9.0"
    )


def test_combined_release_requires_updated_pin(tmp_path: Path) -> None:
    pyproject = _write_otel_pyproject(tmp_path, "1.8.0")

    with pytest.raises(ValueError, match="New SDK releases must update"):
        resolve_layer_sdk_version(
            event_name="release",
            release_tag="sdk-v1.9.0,otel-v0.5.0",
            source_sdk_version="1.9.0",
            otel_pyproject=pyproject,
        )


def test_manual_run_uses_source_sdk(tmp_path: Path) -> None:
    pyproject = _write_otel_pyproject(tmp_path, "1.8.0")

    assert (
        resolve_layer_sdk_version(
            event_name="workflow_dispatch",
            release_tag="",
            source_sdk_version="1.9.0",
            otel_pyproject=pyproject,
        )
        == "1.9.0"
    )
