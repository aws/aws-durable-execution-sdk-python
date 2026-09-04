from __future__ import annotations

import os
import sys
import zipfile
from pathlib import Path

import pytest


sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from check_otel_wheel_dependencies import check_otel_wheel_dependencies


def _write_wheel(tmp_path: Path, requirements: tuple[str, ...]) -> Path:
    wheel = tmp_path / "aws_durable_execution_sdk_python_otel-1.0.0-py3-none-any.whl"
    metadata = [
        "Metadata-Version: 2.4",
        "Name: aws-durable-execution-sdk-python-otel",
        "Version: 1.0.0",
        *(f"Requires-Dist: {requirement}" for requirement in requirements),
        "",
    ]
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(
            "aws_durable_execution_sdk_python_otel-1.0.0.dist-info/METADATA",
            "\n".join(metadata),
        )
    return wheel


def test_accepts_layer_provided_dependencies_with_standalone_extra(
    tmp_path: Path,
) -> None:
    wheel = _write_wheel(
        tmp_path,
        (
            "aws-durable-execution-sdk-python>=2.0.0",
            "OpenTelemetry_API>=1.20.0; extra == 'standalone'",
            "opentelemetry.sdk>=1.20.0; extra == 'standalone'",
            "OpenTelemetry-Propagator_AWS-XRay; extra == 'standalone'",
        ),
    )

    check_otel_wheel_dependencies(wheel)


def test_rejects_default_opentelemetry_dependency(tmp_path: Path) -> None:
    wheel = _write_wheel(
        tmp_path,
        (
            "aws-durable-execution-sdk-python>=2.0.0",
            "opentelemetry-sdk>=1.20.0",
            "opentelemetry-api>=1.20.0; extra == 'standalone'",
            "opentelemetry-sdk>=1.20.0; extra == 'standalone'",
            "opentelemetry-propagator-aws-xray; extra == 'standalone'",
        ),
    )

    with pytest.raises(ValueError, match="installs OpenTelemetry dependencies"):
        check_otel_wheel_dependencies(wheel)


def test_rejects_noncanonical_default_opentelemetry_dependency(
    tmp_path: Path,
) -> None:
    wheel = _write_wheel(
        tmp_path,
        (
            "aws-durable-execution-sdk-python>=2.0.0",
            "OpenTelemetry_SDK>=1.20.0",
            "opentelemetry-api>=1.20.0; extra == 'standalone'",
            "opentelemetry-sdk>=1.20.0; extra == 'standalone'",
            "opentelemetry-propagator-aws-xray; extra == 'standalone'",
        ),
    )

    with pytest.raises(ValueError, match="installs OpenTelemetry dependencies"):
        check_otel_wheel_dependencies(wheel)


def test_rejects_incomplete_standalone_extra(tmp_path: Path) -> None:
    wheel = _write_wheel(
        tmp_path,
        (
            "aws-durable-execution-sdk-python>=2.0.0",
            "opentelemetry-api>=1.20.0; extra == 'standalone'",
            "opentelemetry-sdk>=1.20.0; extra == 'standalone'",
        ),
    )

    with pytest.raises(
        ValueError, match="standalone OpenTelemetry dependencies must be"
    ):
        check_otel_wheel_dependencies(wheel)
