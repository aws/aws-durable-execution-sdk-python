#!/usr/bin/env python3

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from parse_testing_version import parse_testing_version


def test_parse_testing_version():
    test_cases = [
        # Testing-only tag enables the job
        ("testing-v2.0.0", "2.0.0"),
        ("testing-v1.2.1", "1.2.1"),
        # Combined comma-separated tags resolve to the testing version
        ("sdk-v2.0.0,testing-v2.0.0", "2.0.0"),
        ("testing-v2.0.0,sdk-v2.1.0", "2.0.0"),
        ("otel-v1.0.0,testing-v1.2.1,sdk-v2.0.0", "1.2.1"),
        # SDK-only or OTel-only tags do not enable the job
        ("sdk-v2.1.0", ""),
        ("otel-v1.0.0", ""),
        ("sdk-v2.0.0,otel-v1.0.0", ""),
        # Malformed or unrelated prefixes must not match
        ("not-testing-v1.2.1", ""),
        ("sdk-v2.0.0,mytesting-v1.2.1", ""),
        ("testing-v1.2", ""),
        ("testing-version-1.2.1", ""),
        # No release tag
        ("", ""),
        ("v2.0.0", ""),
        ("random-text", ""),
        # Pre-release suffix is kept
        ("testing-v2.0.0rc1", "2.0.0rc1"),
        ("testing-v2.0.0-beta,sdk-v1.0.0", "2.0.0-beta"),
    ]

    for input_text, expected in test_cases:
        result = parse_testing_version(input_text)
        # Assert is expected in test functions
        assert result == expected, (  # noqa: S101
            f"Expected '{expected}' but got '{result}' for input: {input_text}"
        )


if __name__ == "__main__":
    test_parse_testing_version()
    sys.exit(0)
