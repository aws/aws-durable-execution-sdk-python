#!/usr/bin/env python3
"""Assemble the Lambda deployment package for the conformance handlers.

Builds against the **local monorepo SDK source** (not a PyPI release) so the
handlers exercise exactly the code in this checkout. Output lands in an ignored
``lambda-build/`` directory that the SAM templates reference via
``CodeUri: lambda-build/``.

Suites are discovered from checked-in ``template_<suite>.yaml`` files. Each
template must have a matching ``handlers/<suite>/`` directory containing at
least one handler module.

Layout produced::

    lambda-build/
      aws_durable_execution_sdk_python/   # copied from the local SDK package
      <suite>/                            # copied from handlers/<suite>

``boto3`` (the core SDK's only runtime dependency) is provided by the Lambda
Python runtime, so it is intentionally not vendored here.

Usage::

    python3 scripts/build_examples.py
    python3 scripts/build_examples.py --sdk-src /path/to/aws_durable_execution_sdk_python
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

from discover_suites import discover_suites


PKG_DIR = Path(__file__).resolve().parents[1]
BUILD_DIR = PKG_DIR / "lambda-build"

# Default local SDK source: packages/aws-durable-execution-sdk-python/src/...
DEFAULT_SDK_SRC = (
    PKG_DIR.parent
    / "aws-durable-execution-sdk-python"
    / "src"
    / "aws_durable_execution_sdk_python"
)


def build(sdk_src: Path) -> None:
    if not sdk_src.is_dir():
        raise SystemExit(f"Local SDK source not found: {sdk_src}")

    suites = discover_suites(PKG_DIR)

    if BUILD_DIR.exists():
        shutil.rmtree(BUILD_DIR)
    BUILD_DIR.mkdir(parents=True)

    shutil.copytree(
        sdk_src,
        BUILD_DIR / "aws_durable_execution_sdk_python",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )

    for suite in suites:
        shutil.copytree(
            PKG_DIR / "handlers" / suite,
            BUILD_DIR / suite,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )

    print(f"Build complete: {BUILD_DIR}")
    print(f"  SDK source: {sdk_src}")
    print(f"  Suites:     {', '.join(suites)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--sdk-src",
        type=Path,
        default=DEFAULT_SDK_SRC,
        help="Path to the local aws_durable_execution_sdk_python source package.",
    )
    args = parser.parse_args()
    build(args.sdk_src)


if __name__ == "__main__":
    sys.exit(main())
