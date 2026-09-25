#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2025-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
import re

# A release-tag component naming the testing package: testing-v<x.y.z> with an
# optional pre-release suffix. Anchored and matched against a single comma-split
# component so prefixes like "mytesting-v1.2.1" are rejected.
_COMPONENT = re.compile(r"testing-v([0-9]+\.[0-9]+\.[0-9]+[0-9A-Za-z.-]*)\Z")


def parse_testing_version(release_tag: str) -> str:
    """Return the testing version named by the release tag, or empty if none."""
    for part in release_tag.split(","):
        match = _COMPONENT.fullmatch(part.strip())
        if match:
            return match.group(1)
    return ""


def main():
    release_tag = os.environ.get("RELEASE_TAG", "")
    tag_version = parse_testing_version(release_tag)

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as f:
            f.write(f"tag_version={tag_version}\n")

    print(tag_version)


if __name__ == "__main__":
    main()
