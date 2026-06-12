#!/usr/bin/env python3

import os
import re


def parse_sdk_branch(pr_body: str, default_ref: str = "main") -> str:
    """Parse PR body for TESTING_SDK_BRANCH and return the branch reference."""
    pattern = re.compile(r"(?i)TESTING_SDK_BRANCH\s*[:=]\s*(\S+)", re.MULTILINE)

    match = pattern.search(pr_body)
    if match:
        ref = match.group(1).strip()
        if ref:
            return ref

    return default_ref


def main():
    pr_body = os.environ.get("PR_BODY", "")
    ref = parse_sdk_branch(pr_body)

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as f:
            f.write(f"testing_ref={ref}\n")


if __name__ == "__main__":
    main()
