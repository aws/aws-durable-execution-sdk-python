#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2025-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import argparse

from packaging.version import InvalidVersion, Version


def latest_version(existing_tags: list[str]) -> str:
    """Return the highest v<semver> tag, or empty if there are none.

    Non-version tags (latest, malformed) are ignored.
    """
    highest: Version | None = None
    for tag in existing_tags:
        if not tag.startswith("v"):
            continue
        try:
            version = Version(tag[1:])
        except InvalidVersion:
            continue
        if highest is None or version > highest:
            highest = version
    return f"v{highest}" if highest is not None else ""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Print the highest published testing version tag."
    )
    parser.add_argument(
        "tags", nargs="*", help="Existing image tags, e.g. v1.2.1 v2.0.0 latest"
    )
    args = parser.parse_args(argv)

    print(latest_version(args.tags))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
