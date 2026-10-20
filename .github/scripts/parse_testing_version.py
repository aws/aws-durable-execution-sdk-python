#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2025-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
import sys

from testing_version import parse_image_version

_PREFIX = "testing-v"


def parse_testing_version(release_tag: str) -> str:
    """Return the testing version named by the release tag, or empty if none."""
    components = [part.strip() for part in release_tag.split(",")]
    testing_components = [part for part in components if part.startswith(_PREFIX)]
    if not testing_components:
        return ""
    if len(testing_components) != 1:
        raise ValueError("Release tag names the testing package more than once")

    version = testing_components[0][len(_PREFIX) :]
    parse_image_version(version)
    return version


def main() -> int:
    release_tag = os.environ.get("RELEASE_TAG", "")
    try:
        tag_version = parse_testing_version(release_tag)
    except ValueError as error:
        print(f"Invalid testing release tag: {error}", file=sys.stderr)
        return 1

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as f:
            f.write(f"tag_version={tag_version}\n")

    print(tag_version)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
