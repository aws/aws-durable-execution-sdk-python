from __future__ import annotations

import os
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest


sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from build_lambda_layer import BuildConfig, build_layer


def test_build_layer_installs_dependencies_and_zips_lambda_layout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    sdk_wheel = tmp_path / "aws_durable_execution_sdk_python-1.0.0-py3-none-any.whl"
    otel_wheel = (
        tmp_path / "aws_durable_execution_sdk_python_otel-1.0.0-py3-none-any.whl"
    )
    sdk_wheel.write_text("sdk")
    otel_wheel.write_text("otel")
    commands: list[list[str]] = []

    def fake_run(command: list[str], check: bool) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        target = Path(command[command.index("--target") + 1])
        (target / "aws_durable_execution_sdk_python").mkdir()
        (target / "aws_durable_execution_sdk_python" / "__init__.py").write_text("")
        (target / "aws_durable_execution_sdk_python_otel").mkdir()
        (target / "aws_durable_execution_sdk_python_otel" / "__init__.py").write_text(
            ""
        )
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    output = build_layer(
        BuildConfig(
            output=tmp_path / "layer.zip",
            sdk_distribution=sdk_wheel,
            otel_distribution=otel_wheel,
        )
    )

    assert output == tmp_path / "layer.zip"
    assert "--platform" not in commands[0]
    assert "--implementation" not in commands[0]
    assert "--python-version" not in commands[0]
    assert "--abi" not in commands[0]
    assert "--no-compile" in commands[0]
    assert str(sdk_wheel) in commands[0]
    assert str(otel_wheel) in commands[0]

    with zipfile.ZipFile(output) as archive:
        assert (
            "python/aws_durable_execution_sdk_python/__init__.py" in archive.namelist()
        )
        assert (
            "python/aws_durable_execution_sdk_python_otel/__init__.py"
            in archive.namelist()
        )


def test_build_layer_requires_built_distributions(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        build_layer(
            BuildConfig(
                output=tmp_path / "layer.zip",
                sdk_distribution=tmp_path / "missing-sdk.whl",
                otel_distribution=tmp_path / "missing-otel.whl",
            )
        )


def test_build_layer_rejects_output_inside_build_directory(tmp_path: Path) -> None:
    sdk_wheel = tmp_path / "sdk.whl"
    otel_wheel = tmp_path / "otel.whl"
    sdk_wheel.write_text("sdk")
    otel_wheel.write_text("otel")
    build_dir = tmp_path / "layer"

    with pytest.raises(ValueError, match="outside the build directory"):
        build_layer(
            BuildConfig(
                output=build_dir / "layer.zip",
                sdk_distribution=sdk_wheel,
                otel_distribution=otel_wheel,
                build_dir=build_dir,
            )
        )
