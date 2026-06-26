#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2025-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path


ARCHITECTURE_PLATFORMS = {
    "x86_64": "manylinux2014_x86_64",
    "arm64": "manylinux2014_aarch64",
}


@dataclass(frozen=True)
class BuildConfig:
    output: Path
    target_python: str
    architecture: str
    sdk_distribution: Path | None = None
    otel_distribution: Path | None = None
    sdk_requirement: str = "aws-durable-execution-sdk-python"
    otel_requirement: str = "aws-durable-execution-sdk-python-otel"
    build_dir: Path | None = None


def build_layer(config: BuildConfig) -> Path:
    """Build a Lambda layer zip containing the SDK and OTel plugin."""

    _validate_config(config)

    with tempfile.TemporaryDirectory() as temp_dir:
        work_dir = config.build_dir or Path(temp_dir) / "layer"
        if work_dir.exists():
            shutil.rmtree(work_dir)
        layer_python_dir = work_dir / "python"
        layer_python_dir.mkdir(parents=True)

        _install_layer_dependencies(config, layer_python_dir)
        _write_zip(config.output, work_dir)

    return config.output


def _validate_config(config: BuildConfig) -> None:
    if config.architecture not in ARCHITECTURE_PLATFORMS:
        supported = ", ".join(sorted(ARCHITECTURE_PLATFORMS))
        raise ValueError(
            f"Unsupported architecture: {config.architecture}. "
            f"Supported architectures: {supported}"
        )

    if not config.target_python.startswith("3."):
        raise ValueError("target_python must be a Python 3 minor version, such as 3.12")

    for distribution in (config.sdk_distribution, config.otel_distribution):
        if distribution is not None and not distribution.is_file():
            raise FileNotFoundError(distribution)


def _install_layer_dependencies(config: BuildConfig, target_dir: Path) -> None:
    python_version = config.target_python
    abi = f"cp{python_version.replace('.', '')}"
    platform = ARCHITECTURE_PLATFORMS[config.architecture]
    requirements = [
        str(config.sdk_distribution or config.sdk_requirement),
        str(config.otel_distribution or config.otel_requirement),
    ]

    command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--upgrade",
        "--target",
        str(target_dir),
        "--platform",
        platform,
        "--implementation",
        "cp",
        "--python-version",
        python_version,
        "--abi",
        abi,
        "--only-binary",
        ":all:",
        "--no-compile",
        *requirements,
    ]
    subprocess.run(command, check=True)


def _write_zip(output: Path, layer_root: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        output.unlink()

    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(layer_root.rglob("*")):
            if path.is_file():
                archive.write(path, path.relative_to(layer_root))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="build_lambda_layer.py",
        description="Build the AWS Durable Execution SDK Python Lambda layer.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    build_parser = subparsers.add_parser("build", help="build a Lambda layer zip")
    build_parser.add_argument("--output", type=Path, required=True)
    build_parser.add_argument(
        "--target-python",
        required=True,
        help="Lambda Python minor version, for example 3.12",
    )
    build_parser.add_argument(
        "--architecture",
        choices=["x86_64", "arm64"],
        required=True,
        help="Lambda instruction set architecture.",
    )
    build_parser.add_argument("--sdk-distribution", type=Path)
    build_parser.add_argument("--otel-distribution", type=Path)
    build_parser.add_argument(
        "--sdk-requirement",
        default="aws-durable-execution-sdk-python",
        help="Package specifier used when --sdk-distribution is not provided.",
    )
    build_parser.add_argument(
        "--otel-requirement",
        default="aws-durable-execution-sdk-python-otel",
        help="Package specifier used when --otel-distribution is not provided.",
    )
    build_parser.add_argument(
        "--build-dir",
        type=Path,
        help="Optional scratch directory. Existing contents are replaced.",
    )

    args = parser.parse_args(argv)
    if args.command == "build":
        output = build_layer(
            BuildConfig(
                output=args.output,
                target_python=args.target_python,
                architecture=args.architecture,
                sdk_distribution=args.sdk_distribution,
                otel_distribution=args.otel_distribution,
                sdk_requirement=args.sdk_requirement,
                otel_requirement=args.otel_requirement,
                build_dir=args.build_dir,
            )
        )
        print(output)
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
