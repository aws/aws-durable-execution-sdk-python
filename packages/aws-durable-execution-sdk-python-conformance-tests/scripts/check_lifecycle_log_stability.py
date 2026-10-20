#!/usr/bin/env python3
"""Repeat upstream requirement 10-1 against an already deployed plugin stack.

Requires the installed aws-durable-execution-conformance-tests runner. Each
sample uses its unchanged history, result, and log assertions. Raw CloudWatch
pages and invocation responses are retained so ordering failures can be traced
to their original event timestamps and ingestion times.
"""

from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any


def _load_runner() -> tuple[Any, Any, Any, Path]:
    # Keep the upstream runner optional for the SDK's ordinary unit tests.
    from aws_durable_execution_conformance_tests.clients import AwsClients
    from aws_durable_execution_conformance_tests.config import TESTS_DIR
    from aws_durable_execution_conformance_tests.sam import Invoker
    from aws_durable_execution_conformance_tests.validate import validate_description

    return AwsClients, Invoker, validate_description, TESTS_DIR / "plugin" / "10-1.yaml"


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, default=str) + "\n", encoding="utf-8")


class RecordingLogsClient:
    """Forward requests unchanged and persist every page before validation."""

    def __init__(self, client: Any, path: Path) -> None:
        self.client = client
        self.path = path

    def filter_log_events(self, **kwargs: Any) -> dict[str, Any]:
        record: dict[str, Any] = {"request": kwargs}
        try:
            response = self.client.filter_log_events(**kwargs)
            record["response"] = response
            return response
        except Exception as exc:
            record["error"] = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            with self.path.open("a", encoding="utf-8") as output:
                output.write(json.dumps(record, default=str) + "\n")


class RecordingInvoker:
    """Keep the execution ARN even when upstream returns a failed result."""

    def __init__(self, invoker: Any, path: Path) -> None:
        self.invoker = invoker
        self.stack_name = invoker.stack_name
        self.path = path
        self.execution_arn: str | None = None

    def invoke(self, **kwargs: Any) -> Any:
        result = self.invoker.invoke(**kwargs)
        self.path.write_text(result.output, encoding="utf-8")
        try:
            response = json.loads(result.output)
        except ValueError:
            pass  # Upstream reports malformed invocation responses itself.
        else:
            if isinstance(response, dict):
                self.execution_arn = response.get("DurableExecutionArn")
        return result


def _run_sample(
    number: int,
    args: argparse.Namespace,
    clients: Any,
    invoker: Any,
    runner: tuple[Any, Any, Any, Path],
) -> dict[str, Any]:
    clients_type, _, validate_description, requirement = runner
    sample_dir = args.output_dir / f"sample-{number:04d}"
    sample_dir.mkdir()
    events_dir = sample_dir / "events"
    events_dir.mkdir()
    recording_invoker = RecordingInvoker(invoker, sample_dir / "invocation.json")
    recording_clients = clients_type(
        {
            **dict(clients),
            "logs": RecordingLogsClient(
                clients["logs"], sample_dir / "cloudwatch-pages.jsonl"
            ),
        }
    )
    result: dict[str, Any] = {"sample": number, "passed": False, "errors": []}
    try:
        validated = validate_description(
            function_name="PluginInvocationLifecycle",
            description_id="10-1",
            test_file=str(requirement),
            invoker=recording_invoker,
            tmp_dir=str(events_dir),
            region=args.region,
            aws_clients=recording_clients,
            output_dir=str(sample_dir / "history"),
            log_poll_timeout_seconds=args.log_poll_timeout_seconds,
        )
        result.update(
            passed=validated.passed,
            errors=validated.errors,
            placeholders=validated.placeholders,
        )
    except Exception as exc:
        # Keep sampling after a failed request, without losing earlier pages.
        result["errors"] = [f"{type(exc).__name__}: {exc}"]
    result["execution_arn"] = recording_invoker.execution_arn
    _write_json(sample_dir / "result.json", result)
    print(f"Sample {number}: {'PASSED' if result['passed'] else 'FAILED'}", flush=True)
    return result


def _positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stack-name", required=True)
    parser.add_argument("--region", default="us-west-2")
    parser.add_argument("--samples", type=_positive_int, default=40)
    parser.add_argument("--workers", type=int, choices=range(1, 17), default=4)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--log-poll-timeout-seconds",
        type=_positive_int,
        default=120,
        help="Maximum upstream polling budget per sample; passing logs normally "
        "finish after the runner's 20-second observation window.",
    )
    args = parser.parse_args(argv)
    runner = _load_runner()
    clients_type, invoker_type, _, requirement = runner
    if not requirement.is_file():
        parser.error(f"Upstream requirement not found: {requirement}")
    if args.output_dir.exists():
        parser.error("--output-dir must be new to preserve previous diagnostic results")
    args.output_dir.mkdir(parents=True)
    clients = clients_type.create(args.region)
    invoker = invoker_type(
        stack_name=args.stack_name,
        region=args.region,
        lambda_client=clients["lambda"],
        cfn_client=clients["cloudformation"],
    )
    with ThreadPoolExecutor(max_workers=min(args.workers, args.samples)) as executor:
        results = list(
            executor.map(
                lambda number: _run_sample(number, args, clients, invoker, runner),
                range(1, args.samples + 1),
            )
        )
    passed = sum(result["passed"] for result in results)
    summary = {
        "stack_name": args.stack_name,
        "region": args.region,
        "requirement": "10-1",
        "samples": args.samples,
        "passed": passed,
        "failed": args.samples - passed,
        "results": results,
    }
    _write_json(args.output_dir / "summary.json", summary)
    print(f"Lifecycle log checks: {passed}/{args.samples} passed", flush=True)
    return 0 if passed == args.samples else 1


if __name__ == "__main__":
    raise SystemExit(main())
