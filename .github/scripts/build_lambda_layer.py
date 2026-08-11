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


@dataclass(frozen=True)
class BuildConfig:
    output: Path
    sdk_distribution: Path
    otel_distribution: Path
    build_dir: Path | None = None


def build_layer(config: BuildConfig) -> Path:
    """Build a Lambda layer containing the SDK and OpenTelemetry plugin."""

    _validate_config(config)

    with tempfile.TemporaryDirectory() as temp_dir:
        work_dir = config.build_dir or Path(temp_dir) / "layer"
        _validate_output_location(config.output, work_dir)
        if work_dir.exists():
            shutil.rmtree(work_dir)
        layer_python_dir = work_dir / "python"
        layer_python_dir.mkdir(parents=True)

        _install_layer_dependencies(config, layer_python_dir)
        _write_zip(config.output, work_dir)

    return config.output


def _validate_config(config: BuildConfig) -> None:
    for distribution in (config.sdk_distribution, config.otel_distribution):
        if not distribution.is_file():
            raise FileNotFoundError(distribution)


def _validate_output_location(output: Path, build_dir: Path) -> None:
    if output.resolve().is_relative_to(build_dir.resolve()):
        raise ValueError("Layer output must be outside the build directory")


def _install_layer_dependencies(config: BuildConfig, target_dir: Path) -> None:
    command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--upgrade",
        "--target",
        str(target_dir),
        "--only-binary",
        ":all:",
        "--no-compile",
        str(config.sdk_distribution),
        str(config.otel_distribution),
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
        description="Build the AWS Durable Execution SDK OTel plugin Lambda layer.",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sdk-distribution", type=Path, required=True)
    parser.add_argument("--otel-distribution", type=Path, required=True)
    parser.add_argument(
        "--build-dir",
        type=Path,
        help="Optional scratch directory. Existing contents are replaced.",
    )

    args = parser.parse_args(argv)
    output = build_layer(
        BuildConfig(
            output=args.output,
            sdk_distribution=args.sdk_distribution,
            otel_distribution=args.otel_distribution,
            build_dir=args.build_dir,
        )
    )
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
