"""Unit tests for run.py. They need no JS SDK, Node.js or servers.

Run from the repository root:

    scripts/js-examples/.venv/bin/python -m pytest scripts/js-examples
"""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest

import run

TEMPLATE = textwrap.dedent(
    """\
    AWSTemplateFormatVersion: "2010-09-09"
    Transform:
      - AWS::Serverless-2016-10-31
    Resources:
      DurableFunctionRole:
        Type: AWS::IAM::Role
        Properties: {}
      HelloWorld:
        Type: AWS::Serverless::Function
        Properties:
          FunctionName: HelloWorld-22x-NodeJS-Local
          Handler: hello-world.handler
          Timeout: 60
          DurableConfig:
            ExecutionTimeout: 60
            RetentionPeriodInDays: 7
          Role:
            Fn::GetAtt:
              - DurableFunctionRole
              - Arn
          Environment:
            Variables:
              DURABLE_VERBOSE_MODE: "false"
              AWS_ENDPOINT_URL_LAMBDA: http://host.docker.internal:5000
      NonDurable:
        Type: AWS::Serverless::Function
        Properties:
          FunctionName: NonDurable-22x-NodeJS-Local
          Handler: non-durable.handler
          Timeout: 5
    """
)


@pytest.fixture
def examples(tmp_path: Path) -> Path:
    """A minimal examples package: a template and some test files."""
    (tmp_path / "template.yml").write_text(TEMPLATE)
    for rel in (
        "hello-world/hello-world.test.ts",
        "non-durable/non-durable.test.ts",
        "pause-resume/pause-resume.test.ts",
        "otel/basic/otel-basic.test.ts",
    ):
        path = tmp_path / "src/examples" / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("")
    return tmp_path


def test_function_maps_from_template(examples: Path) -> None:
    maps = run.build_function_maps(examples / "template.yml")

    assert maps.name_map == {
        "hello-world": "HelloWorld-22x-NodeJS-Local",
        "non-durable": "NonDurable-22x-NodeJS-Local",
    }
    # The SAM Docker endpoint is dropped; the shim sets the runner address.
    assert maps.shim_map["HelloWorld-22x-NodeJS-Local"] == {
        "file": "hello-world",
        "export": "handler",
        "timeoutSeconds": 60,
        "environment": {"DURABLE_VERBOSE_MODE": "false"},
    }
    assert maps.shim_map["NonDurable-22x-NodeJS-Local"]["timeoutSeconds"] == 5
    assert maps.function_configs == {
        "HelloWorld-22x-NodeJS-Local": {
            "DurableConfig": {"ExecutionTimeout": 60, "RetentionPeriodInDays": 7}
        },
        "NonDurable-22x-NodeJS-Local": {},
    }


def test_template_without_functions_is_an_error(tmp_path: Path) -> None:
    (tmp_path / "template.yml").write_text("Resources: {}\n")
    with pytest.raises(run.HarnessError, match="no AWS::Serverless::Function"):
        run.build_function_maps(tmp_path / "template.yml")


def test_select_skips_undeployed_and_otel(examples: Path) -> None:
    maps = run.build_function_maps(examples / "template.yml")

    tests, not_deployed = run.select_tests(examples, [], None, maps)

    assert tests == [
        "src/examples/hello-world/hello-world.test.ts",
        "src/examples/non-durable/non-durable.test.ts",
    ]
    assert not_deployed == ["src/examples/pause-resume/pause-resume.test.ts"]


def test_select_by_pattern_and_shard(examples: Path) -> None:
    maps = run.build_function_maps(examples / "template.yml")

    assert run.select_tests(examples, ["non-"], None, maps)[0] == [
        "src/examples/non-durable/non-durable.test.ts"
    ]
    first, _ = run.select_tests(examples, [], (1, 2), maps)
    second, _ = run.select_tests(examples, [], (2, 2), maps)
    assert first == ["src/examples/hello-world/hello-world.test.ts"]
    assert second == ["src/examples/non-durable/non-durable.test.ts"]


def jest_result(examples: Path, suites: dict[str, list[str]]) -> dict[str, object]:
    """A jest --json result: test file -> the status of each test in it."""
    return {
        "testResults": [
            {
                "name": str(examples / rel),
                "status": "failed" if "failed" in statuses else "passed",
                "assertionResults": [{"status": s} for s in statuses],
            }
            for rel, statuses in suites.items()
        ]
    }


A = "src/examples/a.test.ts"
B = "src/examples/b.test.ts"
C = "src/examples/c.test.ts"


def test_outcomes_classify_and_retry_wins(examples: Path) -> None:
    first = jest_result(examples, {A: ["passed"], B: ["failed"], C: ["pending"]})
    retry = jest_result(examples, {B: ["passed"]})

    assert run.outcomes([first], examples) == {
        A: "passed",
        B: "failed",
        C: "skipped",
    }
    assert run.outcomes([first, retry], examples)[B] == "passed"


def summarize(
    examples: Path,
    tmp_path: Path,
    results: list[dict[str, object]],
    selected: list[str],
    retried: list[str] | None = None,
) -> tuple[int, dict[str, list[str]]]:
    out = tmp_path / "out"
    out.mkdir(exist_ok=True)
    status = run.summarize(results, examples, selected, out, retried or [])
    return status, json.loads((out / "report.json").read_text())


def test_summary_passes_and_reports_flaky(examples: Path, tmp_path: Path) -> None:
    results = [
        jest_result(examples, {A: ["passed"], B: ["failed"]}),
        jest_result(examples, {B: ["passed"]}),
    ]

    status, report = summarize(examples, tmp_path, results, [A, B], retried=[B])

    assert status == 0
    assert report["flaky"] == [B]


def test_summary_fails_on_unexpected_failure(examples: Path, tmp_path: Path) -> None:
    results = [jest_result(examples, {A: ["passed"], B: ["failed"]})]

    status, report = summarize(examples, tmp_path, results, [A, B])

    assert status == 1
    assert report["unexpected_failures"] == [B]


def test_summary_fails_when_a_file_did_not_run(examples: Path, tmp_path: Path) -> None:
    results = [jest_result(examples, {A: ["passed"]})]

    status, report = summarize(examples, tmp_path, results, [A, B])

    assert status == 1
    assert report["not_run"] == [B]


def test_summary_fails_when_nothing_passed(examples: Path, tmp_path: Path) -> None:
    results = [jest_result(examples, {C: ["pending"]})]

    status, _ = summarize(examples, tmp_path, results, [C])

    assert status == 1


def test_known_failures(
    examples: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    here = tmp_path / "here"
    here.mkdir()
    (here / "known-failures.txt").write_text(f"# header\n{B}  # tracked in #1\n")
    monkeypatch.setattr(run, "HERE", here)

    # A listed failure does not fail the run.
    results = [jest_result(examples, {A: ["passed"], B: ["failed"]})]
    assert summarize(examples, tmp_path, results, [A, B])[0] == 0

    # A listed file that passes fails the run, so the list stays accurate.
    results = [jest_result(examples, {A: ["passed"], B: ["passed"]})]
    status, report = summarize(examples, tmp_path, results, [A, B])
    assert status == 1
    assert report["known_failures_now_passing"] == [B]
