#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2025-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from latest_testing_version import latest_version


def test_latest_version():
    test_cases = [
        # Highest of several
        (["v1.2.1", "v2.0.0", "v1.1.3", "latest"], "v2.0.0"),
        # Single version
        (["v1.0.0"], "v1.0.0"),
        # Order does not matter
        (["v2.0.0", "v1.0.0"], "v2.0.0"),
        # Non-version and malformed tags ignored
        (["v2.0.0", "latest", "notaversion", "v"], "v2.0.0"),
        # No version tags
        ([], ""),
        (["latest"], ""),
        # Pre-release orders below its release
        (["v2.0.0rc1", "v2.0.0"], "v2.0.0"),
        (["v2.0.0rc1"], "v2.0.0rc1"),
    ]

    for tags, expected in test_cases:
        result = latest_version(tags)
        # Assert is expected in test functions
        assert result == expected, (  # noqa: S101
            f"Expected '{expected}' but got '{result}' for {tags}"
        )


if __name__ == "__main__":
    test_latest_version()
    sys.exit(0)
