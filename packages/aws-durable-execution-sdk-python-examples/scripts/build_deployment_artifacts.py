#!/usr/bin/env python3

import argparse
import logging
import shutil
import sys
from pathlib import Path


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _copy_package(package_dir: Path, target_dir: Path) -> None:
    """Copy the files needed for pip to install a local package."""
    if target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True)

    for file_name in ("pyproject.toml", "README.md"):
        shutil.copy2(package_dir / file_name, target_dir / file_name)

    shutil.copytree(
        package_dir / "src",
        target_dir / "src",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo", "*.egg-info"),
    )


def prepare_sam_source(source_dir: Path) -> bool:
    """Prepare a SAM Python source directory for Lambda builds."""
    repo_root = _repo_root()
    examples_dir = repo_root / "packages" / "aws-durable-execution-sdk-python-examples"
    src_dir = examples_dir / "src"

    if source_dir.exists():
        shutil.rmtree(source_dir)
    source_dir.mkdir(parents=True)

    runtime_packages = [
        repo_root / "packages" / "aws-durable-execution-sdk-python",
        repo_root / "packages" / "aws-durable-execution-sdk-python-otel",
    ]

    logger.info("Copying local runtime packages into %s", source_dir)
    for package in runtime_packages:
        _copy_package(package, source_dir / package.name)

    requirements = "\n".join(f"./{package.name}" for package in runtime_packages)
    (source_dir / "requirements.txt").write_text(f"{requirements}\n")

    logger.info("Copying examples from %s", src_dir)
    for file_path in src_dir.rglob("*"):
        if (
            file_path.is_file()
            and "__pycache__" not in file_path.parts
            and file_path.suffix != ".pyc"
        ):
            shutil.copy2(file_path, source_dir / file_path.name)

    logger.info("SAM source prepared successfully")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare SAM source artifacts")
    parser.add_argument("source_dir", type=Path)
    args = parser.parse_args()

    if not prepare_sam_source(args.source_dir):
        sys.exit(1)


if __name__ == "__main__":
    main()
