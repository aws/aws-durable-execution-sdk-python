#!/usr/bin/env python3

import argparse
import html
import os
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path


def case_status(test_case: ET.Element) -> str:
    """Return the pytest outcome represented by a JUnit testcase element."""
    if test_case.find("error") is not None:
        return "error"
    if test_case.find("failure") is not None:
        return "failed"
    if test_case.find("skipped") is not None:
        return "skipped"
    return "passed"


def case_detail(test_case: ET.Element) -> str:
    """Return the failure/error/skip detail for a JUnit testcase element."""
    for tag in ("failure", "error", "skipped"):
        element = test_case.find(tag)
        if element is not None:
            message = element.attrib.get("message") or element.text or ""
            lines = message.splitlines()
            return lines[0] if lines else ""
    return ""


def case_name(test_case: ET.Element) -> str:
    """Return a readable test case name from JUnit classname/name attributes."""
    class_name = test_case.attrib.get("classname", "")
    name = test_case.attrib.get("name", "")
    if class_name:
        return f"{class_name}.{name}"
    return name


def format_duration(seconds_text: str) -> str:
    try:
        seconds = float(seconds_text)
    except (TypeError, ValueError):
        return ""
    return f"{seconds:.2f}s"


def markdown_cell(value: str) -> str:
    return html.escape(value).replace("|", "\\|")


def testcase_rows(test_cases: list[ET.Element]) -> list[str]:
    rows = []
    for test_case in sorted(test_cases, key=case_name):
        rows.append(
            "| {status} | `{name}` | {duration} | {detail} |".format(
                status=case_status(test_case),
                name=markdown_cell(case_name(test_case)),
                duration=format_duration(test_case.attrib.get("time", "")),
                detail=markdown_cell(case_detail(test_case)),
            )
        )
    return rows


def build_summary(report_path: Path, title: str) -> str:
    if not report_path.exists():
        return "\n".join(
            [
                f"## {title}",
                "",
                f"No JUnit report was found at `{report_path}`.",
                "The test command may have failed before pytest wrote results.",
                "",
            ]
        )

    root = ET.parse(report_path).getroot()
    test_cases = root.findall(".//testcase")
    counts = Counter(case_status(test_case) for test_case in test_cases)
    total = len(test_cases)
    passed = counts["passed"]
    failed = counts["failed"]
    errors = counts["error"]
    skipped = counts["skipped"]

    lines = [
        f"## {title}",
        "",
        "| Total | Passed | Failed | Errors | Skipped |",
        "| ---: | ---: | ---: | ---: | ---: |",
        f"| {total} | {passed} | {failed} | {errors} | {skipped} |",
        "",
    ]

    unsuccessful_cases = [
        test_case for test_case in test_cases if case_status(test_case) != "passed"
    ]
    if unsuccessful_cases:
        lines.extend(
            [
                "### Failed, Error, and Skipped Test Cases",
                "",
                "| Status | Test case | Duration | Detail |",
                "| --- | --- | ---: | --- |",
                *testcase_rows(unsuccessful_cases),
                "",
            ]
        )

    lines.extend(
        [
            "<details>",
            "<summary>All test cases</summary>",
            "",
            "| Status | Test case | Duration | Detail |",
            "| --- | --- | ---: | --- |",
            *testcase_rows(test_cases),
            "",
            "</details>",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Write a GitHub Actions summary from a pytest JUnit XML report"
    )
    parser.add_argument("report", type=Path, help="Path to the JUnit XML report")
    parser.add_argument("--title", required=True, help="Summary heading")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(os.environ.get("GITHUB_STEP_SUMMARY", "-")),
        help="Path to append the Markdown summary to",
    )
    args = parser.parse_args()

    summary = build_summary(args.report, args.title)
    if str(args.output) == "-":
        print(summary)
    else:
        with args.output.open("a") as file:
            file.write(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
