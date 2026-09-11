#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2025-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import argparse
import sys

from packaging.version import InvalidVersion, Version


def is_newest(candidate: str, existing_tags: list[str]) -> bool:
    """True if candidate is >= every v<semver> tag in existing_tags.

    Non-version tags (latest, malformed) are ignored. With no existing
    version tags the candidate is newest by default.
    """
    candidate_version = Version(candidate)
    highest = candidate_version
    for tag in existing_tags:
        if not tag.startswith("v"):
            continue
        try:
            version = Version(tag[1:])
        except InvalidVersion:
            continue
        if version > highest:
            highest = version
    return candidate_version >= highest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Decide whether a release version is the newest published."
    )
    parser.add_argument("--candidate", required=True)
    parser.add_argument(
        "tags", nargs="*", help="Existing image tags, e.g. v1.2.1 v2.0.0 latest"
    )
    args = parser.parse_args(argv)

    print("true" if is_newest(args.candidate, args.tags) else "false")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
