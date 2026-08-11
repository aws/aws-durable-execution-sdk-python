from __future__ import annotations

import os
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest


sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from build_lambda_layer import BuildConfig, build_layer


def _write_test_wheel(
    directory: Path,
    distribution: str,
    package_files: tuple[str, ...],
    dependencies: tuple[str, ...] = (),
) -> Path:
    normalized_distribution = distribution.replace("-", "_")
    wheel = directory / f"{normalized_distribution}-1.0.0-py3-none-any.whl"
    dist_info = f"{normalized_distribution}-1.0.0.dist-info"
    metadata = [
        "Metadata-Version: 2.1",
        f"Name: {distribution}",
        "Version: 1.0.0",
        *(f"Requires-Dist: {dependency}==1.0.0" for dependency in dependencies),
        "",
    ]

    with zipfile.ZipFile(wheel, "w") as archive:
        for package_file in package_files:
            archive.writestr(package_file, "")
        archive.writestr(f"{dist_info}/METADATA", "\n".join(metadata))
        archive.writestr(
            f"{dist_info}/WHEEL",
            "\n".join(
                (
                    "Wheel-Version: 1.0",
                    "Generator: test",
                    "Root-Is-Purelib: true",
                    "Tag: py3-none-any",
                    "",
                )
            ),
        )
        archive.writestr(f"{dist_info}/RECORD", "")

    return wheel


def test_build_layer_installs_distributions_and_zips_lambda_layout(
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
    assert "--no-compile" in commands[0]
    assert "--no-deps" in commands[0]
    assert "--platform" not in commands[0]
    assert "--python-version" not in commands[0]
    assert "--abi" not in commands[0]
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


def test_build_layer_excludes_adot_and_runtime_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    sdk_wheel = _write_test_wheel(
        tmp_path,
        "aws-durable-execution-sdk-python",
        ("aws_durable_execution_sdk_python/__init__.py",),
        ("boto3",),
    )
    otel_wheel = _write_test_wheel(
        tmp_path,
        "aws-durable-execution-sdk-python-otel",
        ("aws_durable_execution_sdk_python_otel/__init__.py",),
        ("aws-opentelemetry-distro", "opentelemetry-api"),
    )
    _write_test_wheel(tmp_path, "boto3", ("boto3/__init__.py",))
    _write_test_wheel(
        tmp_path,
        "aws-opentelemetry-distro",
        ("amazon/opentelemetry/distro/__init__.py",),
    )
    _write_test_wheel(
        tmp_path,
        "opentelemetry-api",
        ("opentelemetry/__init__.py",),
    )
    monkeypatch.setenv("PIP_FIND_LINKS", str(tmp_path))
    monkeypatch.setenv("PIP_NO_INDEX", "1")

    output = build_layer(
        BuildConfig(
            output=tmp_path / "layer.zip",
            sdk_distribution=sdk_wheel,
            otel_distribution=otel_wheel,
        )
    )

    with zipfile.ZipFile(output) as archive:
        names = archive.namelist()

    assert "python/aws_durable_execution_sdk_python/__init__.py" in names
    assert "python/aws_durable_execution_sdk_python_otel/__init__.py" in names
    assert not any(name.startswith("python/amazon/") for name in names)
    assert not any(name.startswith("python/boto3/") for name in names)
    assert not any(name.startswith("python/opentelemetry/") for name in names)


def test_build_layer_requires_built_distributions(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        build_layer(
            BuildConfig(
                output=tmp_path / "layer.zip",
                sdk_distribution=tmp_path / "missing-sdk.whl",
                otel_distribution=tmp_path / "missing-otel.whl",
            )
        )


def test_build_layer_requires_universal_wheels(tmp_path: Path) -> None:
    sdk_wheel = tmp_path / "sdk-1.0.0-cp311-cp311-manylinux2014_x86_64.whl"
    otel_wheel = tmp_path / "otel-1.0.0-py3-none-any.whl"
    sdk_wheel.write_text("sdk")
    otel_wheel.write_text("otel")

    with pytest.raises(ValueError, match="must be universal wheels"):
        build_layer(
            BuildConfig(
                output=tmp_path / "layer.zip",
                sdk_distribution=sdk_wheel,
                otel_distribution=otel_wheel,
            )
        )


def test_build_layer_rejects_output_inside_build_directory(tmp_path: Path) -> None:
    sdk_wheel = tmp_path / "sdk-1.0.0-py3-none-any.whl"
    otel_wheel = tmp_path / "otel-1.0.0-py3-none-any.whl"
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
