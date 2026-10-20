"""Boundary tests for repeated upstream lifecycle validation and its artifacts."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import check_lifecycle_log_stability as stability  # noqa: E402


@pytest.fixture
def runner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    requirement = tmp_path / "10-1.yaml"
    requirement.write_text("ExpectedLogs: []\n")
    event = {
        "eventId": "original-event-id",
        "logStreamName": "original-stream",
        "timestamp": 10,
        "ingestionTime": 20,
        "message": '{"hook": "invocation-start"}',
    }
    logs = Mock()
    logs.filter_log_events.return_value = {"events": [event], "nextToken": "next-page"}

    class Clients(dict):
        @classmethod
        def create(cls, region: str) -> Clients:
            return cls(logs=logs, **{"lambda": Mock(), "cloudformation": Mock()})

    class Invoker:
        def __init__(self, **kwargs: object) -> None:
            self.stack_name = kwargs["stack_name"]

        def invoke(self, **kwargs: object) -> SimpleNamespace:
            sample = Path(str(kwargs["event_file_path"])).parents[1].name
            return SimpleNamespace(
                output=json.dumps({"DurableExecutionArn": f"arn:execution:{sample}"})
            )

    calls: list[dict] = []
    fail_samples: set[str] = set()
    raise_samples: set[str] = set()

    def validate(**kwargs: object) -> SimpleNamespace:
        calls.append(kwargs)
        event_dir = Path(str(kwargs["tmp_dir"]))
        sample = event_dir.parent.name
        event_file = event_dir / "10-1_event.json"
        event_file.write_text('{"Input": "test"}')
        kwargs["invoker"].invoke(event_file_path=str(event_file))
        kwargs["aws_clients"]["logs"].filter_log_events(
            logGroupName="/aws/lambda/lifecycle",
            filterPattern=sample,
            startTime=1,
        )
        if sample in raise_samples:
            raise RuntimeError("CloudWatch request failed after the first page")
        history_dir = Path(str(kwargs["output_dir"]))
        history_dir.mkdir()
        (history_dir / "10-1.json").write_text('{"ExecutionStatus": "SUCCEEDED"}')
        failed = sample in fail_samples
        return SimpleNamespace(
            passed=not failed,
            errors=["log records out of order"] if failed else [],
            placeholders={"INPUT_1": "test"},
        )

    monkeypatch.setattr(
        stability, "_load_runner", lambda: (Clients, Invoker, validate, requirement)
    )
    return SimpleNamespace(
        calls=calls,
        event=event,
        fail_samples=fail_samples,
        raise_samples=raise_samples,
        requirement=requirement,
    )


def _arguments(output: Path, samples: int = 4) -> list[str]:
    return [
        "--stack-name",
        "existing-stack",
        "--region",
        "us-west-2",
        "--samples",
        str(samples),
        "--workers",
        "2",
        "--output-dir",
        str(output),
        "--log-poll-timeout-seconds",
        "60",
    ]


def test_repeats_exact_upstream_requirement_with_isolated_artifacts(
    tmp_path: Path, runner: SimpleNamespace
) -> None:
    output = tmp_path / "results"

    assert stability.main(_arguments(output)) == 0

    assert len(runner.calls) == 4
    assert len({call["tmp_dir"] for call in runner.calls}) == 4
    assert len({call["output_dir"] for call in runner.calls}) == 4
    for call in runner.calls:
        assert call["function_name"] == "PluginInvocationLifecycle"
        assert call["description_id"] == "10-1"
        assert call["test_file"] == str(runner.requirement)
        assert call["log_poll_timeout_seconds"] == 60
    summary = json.loads((output / "summary.json").read_text())
    assert (summary["samples"], summary["passed"], summary["failed"]) == (4, 4, 0)
    assert len({result["execution_arn"] for result in summary["results"]}) == 4
    assert all(
        (path / "history" / "10-1.json").exists() for path in output.glob("sample-*")
    )


def test_failure_keeps_original_cloudwatch_data_and_arn_and_returns_nonzero(
    tmp_path: Path, runner: SimpleNamespace
) -> None:
    output = tmp_path / "results"
    runner.fail_samples.add("sample-0002")

    assert stability.main(_arguments(output)) == 1

    summary = json.loads((output / "summary.json").read_text())
    assert (summary["passed"], summary["failed"]) == (3, 1)
    failed = json.loads((output / "sample-0002" / "result.json").read_text())
    assert failed["errors"] == ["log records out of order"]
    assert failed["execution_arn"] == "arn:execution:sample-0002"
    assert json.loads((output / "sample-0002" / "invocation.json").read_text()) == {
        "DurableExecutionArn": failed["execution_arn"]
    }
    page = json.loads((output / "sample-0002" / "cloudwatch-pages.jsonl").read_text())
    assert page["response"] == {"events": [runner.event], "nextToken": "next-page"}
    assert page["request"]["filterPattern"] == "sample-0002"


def test_unexpected_sample_error_preserves_pages_and_remaining_samples(
    tmp_path: Path, runner: SimpleNamespace
) -> None:
    output = tmp_path / "results"
    runner.raise_samples.add("sample-0001")

    assert stability.main(_arguments(output)) == 1

    summary = json.loads((output / "summary.json").read_text())
    assert len(runner.calls) == 4
    assert summary["failed"] == 1
    failed = summary["results"][0]
    assert failed["execution_arn"] == "arn:execution:sample-0001"
    assert failed["errors"] == [
        "RuntimeError: CloudWatch request failed after the first page"
    ]
    assert (output / "sample-0001" / "cloudwatch-pages.jsonl").is_file()


def test_records_each_page_and_failed_request_without_changing_response(
    tmp_path: Path,
) -> None:
    page = {"events": [{"timestamp": 3, "ingestionTime": 4}], "nextToken": "next"}
    client = Mock()
    client.filter_log_events.side_effect = [page, RuntimeError("unavailable")]
    output = tmp_path / "pages.jsonl"
    recorder = stability.RecordingLogsClient(client, output)

    assert recorder.filter_log_events(logGroupName="logs") is page
    with pytest.raises(RuntimeError, match="unavailable"):
        recorder.filter_log_events(logGroupName="logs", nextToken="next")

    records = [json.loads(line) for line in output.read_text().splitlines()]
    assert records[0]["response"] == page
    assert records[1] == {
        "request": {"logGroupName": "logs", "nextToken": "next"},
        "error": "RuntimeError: unavailable",
    }


@pytest.mark.parametrize(
    "extra", [["--samples", "0"], ["--workers", "0"], ["--workers", "17"]]
)
def test_rejects_invalid_sample_or_worker_counts(
    tmp_path: Path, extra: list[str]
) -> None:
    with pytest.raises(SystemExit) as caught:
        stability.main([*_arguments(tmp_path / "results"), *extra])
    assert caught.value.code == 2


def test_existing_results_are_not_overwritten(
    tmp_path: Path, runner: SimpleNamespace
) -> None:
    output = tmp_path / "results"
    output.mkdir()
    evidence = output / "summary.json"
    evidence.write_text("original evidence")

    with pytest.raises(SystemExit) as caught:
        stability.main(_arguments(output))

    assert caught.value.code == 2
    assert not runner.calls
    assert evidence.read_text() == "original evidence"
