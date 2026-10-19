# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Release metadata for the Durable Execution Telemetry Exporter."""

import tomllib
from importlib.metadata import version
from pathlib import Path

from packaging.version import Version

from aws_durable_execution_sdk_python_telemetry_exporter import __version__


PACKAGE_ROOT = Path(__file__).resolve().parents[1]


def test_public_version_matches_installed_stable_release() -> None:
    assert __version__ == version("aws-durable-execution-sdk-python-telemetry-exporter")
    release = Version(__version__)
    assert release.major >= 1
    assert not release.is_prerelease
    assert not release.is_devrelease


def test_package_is_marked_production_stable() -> None:
    with (PACKAGE_ROOT / "pyproject.toml").open("rb") as pyproject:
        classifiers = tomllib.load(pyproject)["project"]["classifiers"]

    assert "Development Status :: 5 - Production/Stable" in classifiers
    assert "Development Status :: 4 - Beta" not in classifiers
