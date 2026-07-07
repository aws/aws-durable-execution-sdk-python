#!/usr/bin/env python3

import logging
import shutil
import subprocess
import sys
from pathlib import Path


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def build_examples() -> None:
    """Build examples with vendored runtime dependencies for SAM packaging."""
    examples_dir = Path(__file__).resolve().parent.parent
    build_dir = examples_dir / "build"
    src_dir = examples_dir / "src"
    packages_dir = examples_dir.parent

    runtime_packages = [
        packages_dir / "aws-durable-execution-sdk-python",
        packages_dir / "aws-durable-execution-sdk-python-otel",
        packages_dir / "aws-durable-execution-sdk-python-testing",
    ]

    if build_dir.exists():
        logger.info("Cleaning existing build directory")
        shutil.rmtree(build_dir)
    build_dir.mkdir()

    logger.info("Installing runtime dependencies into %s", build_dir)
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--upgrade",
            "--target",
            str(build_dir),
            *[str(package) for package in runtime_packages],
        ],
        check=True,
    )

    logger.info("Copying examples from %s", src_dir)
    for file_path in src_dir.rglob("*"):
        if not file_path.is_file():
            continue

        # The catalog's handler names assume flattened module paths in Lambda.
        shutil.copy2(file_path, build_dir / file_path.name)

    logger.info("Build completed successfully")


def main() -> int:
    try:
        build_examples()
    except subprocess.CalledProcessError:
        logger.exception("Failed to build examples")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
