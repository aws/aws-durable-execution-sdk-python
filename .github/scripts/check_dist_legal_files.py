#!/usr/bin/env python3
"""Verify built distributions include LICENSE and NOTICE files."""

from __future__ import annotations

import sys
import tarfile
import zipfile
from pathlib import Path


REQUIRED_BASENAMES = {"LICENSE", "NOTICE"}


def list_archive_names(archive_path: Path) -> list[str]:
    if archive_path.suffix == ".whl":
        with zipfile.ZipFile(archive_path) as archive:
            return archive.namelist()

    if archive_path.suffixes[-2:] == [".tar", ".gz"]:
        with tarfile.open(archive_path) as archive:
            return archive.getnames()

    raise ValueError(f"Unsupported distribution type: {archive_path}")


def verify_package(package_dir: Path) -> list[str]:
    dist_dir = package_dir / "dist"
    errors: list[str] = []

    if not dist_dir.is_dir():
        return [f"{package_dir}: missing dist directory"]

    archives = sorted(
        path
        for path in dist_dir.iterdir()
        if path.is_file()
        and (path.suffix == ".whl" or path.suffixes[-2:] == [".tar", ".gz"])
    )
    if not archives:
        return [f"{package_dir}: no built distributions found"]

    for archive_path in archives:
        names = list_archive_names(archive_path)
        basenames = {Path(name).name for name in names}
        missing = sorted(REQUIRED_BASENAMES - basenames)
        if missing:
            errors.append(f"{archive_path}: missing {', '.join(missing)}")

    return errors


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(
            "usage: check_dist_legal_files.py <package-dir> [<package-dir> ...]",
            file=sys.stderr,
        )
        return 2

    errors: list[str] = []
    for arg in argv[1:]:
        errors.extend(verify_package(Path(arg)))

    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1

    for arg in argv[1:]:
        print(f"{arg}: LICENSE and NOTICE found in all distributions")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
