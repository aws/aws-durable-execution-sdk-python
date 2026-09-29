#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2025-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import argparse
import re

from packaging.version import InvalidVersion, Version

# Exactly vX.Y.Z with an optional PEP 440 suffix (rc1, -beta, .post1, +local),
# matching what this workflow publishes. A fourth numeric component (v1.2.3.4)
# is rejected: the suffix, if any, must not start with a dot followed by a digit.
TAG_RE = re.compile(r"^v[0-9]+\.[0-9]+\.[0-9]+(?![.0-9])")


def current_latest_version(tags_on_latest: list[str]) -> str:
    """Return the highest vX.Y.Z tag sharing the latest digest, or empty.

    Empty means latest does not resolve to a version we recognize, whether it
    carries no version tag or only off-grammar ones (a scheme change or a
    hand-pushed tag). The caller treats empty as invalid and refuses to move
    latest, since it cannot prove it is newer. If one digest carries several
    version tags the max is returned, keeping the monotonic guard conservative.
    """
    highest: Version | None = None
    highest_tag = ""
    for tag in tags_on_latest:
        if not TAG_RE.match(tag):
            continue
        try:
            version = Version(tag[1:])
        except InvalidVersion:
            continue
        if highest is None or version > highest:
            highest = version
            highest_tag = tag
    return highest_tag


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Print the version tag latest currently resolves to."
    )
    parser.add_argument(
        "tags", nargs="*", help="Tags carried by the latest digest, e.g. v1.2.1 latest"
    )
    args = parser.parse_args(argv)

    print(current_latest_version(args.tags))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
