#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2025-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from is_newest_testing_version import is_newest


def test_is_newest():
    test_cases = [
        # Newest so far
        ("2.0.0", ["v1.2.1", "v1.1.3", "latest"], True),
        # First ever release
        ("1.0.0", [], True),
        ("1.0.0", ["latest"], True),
        # Equal to the highest counts as newest (idempotent re-publish)
        ("2.0.0", ["v2.0.0", "latest"], True),
        # Backport below the highest is NOT newest
        ("1.1.3", ["v1.2.1", "v2.0.0", "latest"], False),
        ("1.9.9", ["v2.0.0"], False),
        # Malformed and non-version tags are ignored
        ("2.0.1", ["v2.0.0", "notaversion", "latest", "v"], True),
        # Pre-release ordering
        ("2.0.0", ["v2.0.0rc1"], True),
        ("2.0.0rc1", ["v2.0.0"], False),
    ]

    for candidate, tags, expected in test_cases:
        result = is_newest(candidate, tags)
        # Assert is expected in test functions
        assert result == expected, (  # noqa: S101
            f"Expected {expected} but got {result} for {candidate} against {tags}"
        )


if __name__ == "__main__":
    test_is_newest()
    sys.exit(0)
