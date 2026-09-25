#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2025-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from current_latest_version import current_latest_version

CASES = [
    # No tags, or no recognized version on the digest -> empty
    ([], ""),
    (["latest"], ""),
    (["latest", "some-branch"], ""),
    (["nightly"], ""),
    # Single version tag alongside latest
    (["v1.2.3", "latest"], "v1.2.3"),
    # Highest wins when several version tags share the digest
    (["v1.2.3", "v1.3.0", "latest"], "v1.3.0"),
    # Pre-release orders below its release
    (["v2.0.0rc1", "v2.0.0"], "v2.0.0"),
    (["v2.0.0rc1"], "v2.0.0rc1"),
    # Original tag text preserved, not normalized
    (["v2.0.0-beta"], "v2.0.0-beta"),
    # Off-grammar tags are not recognized -> empty (caller fails closed)
    (["v1"], ""),
    (["v1.2"], ""),
    (["v999"], ""),
    (["v2025.09"], ""),
    # A fourth numeric component is not our grammar -> empty
    (["v1.2.3.4"], ""),
    (["v999.0.0.1"], ""),
    (["v1.2.3-x86_64", "v1.2.3-arm64"], ""),
    # A good tag alongside a junk one still resolves cleanly
    (["v1", "v1.2.3"], "v1.2.3"),
    (["v1.2.3.4", "v1.2.3"], "v1.2.3"),
]


@pytest.mark.parametrize("tags,expected", CASES)
def test_current_latest_version(tags, expected):
    assert current_latest_version(tags) == expected


def _run_standalone() -> int:
    failures = 0
    for tags, expected in CASES:
        got = current_latest_version(tags)
        if got != expected:
            failures += 1
            print(
                f"FAIL: current_latest_version({tags!r}) = {got!r}, expected {expected!r}"
            )
    if failures:
        print(f"{failures} failing case(s)")
        return 1
    print(f"all {len(CASES)} cases passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(_run_standalone())
