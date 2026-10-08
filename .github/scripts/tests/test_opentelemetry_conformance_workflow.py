import re
from pathlib import Path

import yaml


SHARED_WORKFLOW_REF = "fb95e20acb0b9b10f5fc032ac521a4fd45efdd73"
CONFORMANCE_TEST_REF = "fb95e20acb0b9b10f5fc032ac521a4fd45efdd73"

WORKFLOW_PATH = (
    Path(__file__).parents[2] / "workflows" / "opentelemetry-conformance-tests.yml"
)
EXAMPLES_DIR = ".build/durable-sdk/packages/aws-durable-execution-sdk-python-conformance-tests-otel"


def test_opentelemetry_conformance_caller_uses_current_workflow_contract() -> None:
    workflow = WORKFLOW_PATH.read_text()

    assert "\n  schedule:" not in workflow

    orchestrator = (
        "uses: aws/aws-durable-execution-conformance-tests/.github/workflows/"
        "opentelemetry-orchestrator.yml@"
    )
    pinned_ref = re.search(re.escape(orchestrator) + r"([0-9a-f]{40})", workflow)
    assert pinned_ref is not None
    assert pinned_ref.group(1) == SHARED_WORKFLOW_REF
    assert f"default: {CONFORMANCE_TEST_REF}" in workflow
    assert "python-opentelemetry.yml@" not in workflow
    assert "\n      otlp_endpoint:" not in workflow

    for configuration in (
        "language: python",
        "runs_on: codebuild-github-actions-runner-${{ github.run_id }}-${{ github.run_attempt }}",
        "resource_prefix: p",
        "sdk_repository: aws/aws-durable-execution-sdk-python",
        "sdk_ref: ${{ github.event.pull_request.head.sha || github.sha }}",
        f"conformance_test_ref: ${{{{ inputs.conformance_test_ref || '{CONFORMANCE_TEST_REF}' }}}}",
        "checkout_sdk: true",
        f"examples_dir: {EXAMPLES_DIR}",
        "adot_release_repository: aws-observability/aws-otel-python-instrumentation",
        "collector_compatible_runtime: python3.13",
        "collector_otlp_endpoint: http://localhost:4318",
        "suite_timeout_minutes: 30",
    ):
        assert configuration in workflow

    for secret_name in (
        "DATADOG_ACCESS_TOKEN",
        "DATADOG_API_KEY",
        "DATADOG_APPLICATION_KEY",
    ):
        mapping = f"{secret_name}: ${{{{ secrets.{secret_name} }}}}"
        assert mapping in workflow

    for obsolete_secret_name in (
        "DD_API_KEY",
        "DD_APPLICATION_KEY",
        "DATADOG_OTLP_HEADERS",
    ):
        assert f"{obsolete_secret_name}:" not in workflow


def test_opentelemetry_conformance_handlers_come_from_this_repository() -> None:
    workflow = WORKFLOW_PATH.read_text()

    # The handlers live here now, so the conformance repository's bundled Python
    # example project and its contract test no longer take part in the run.
    assert "contract_test_command" not in workflow
    assert "packages/aws-durable-execution-conformance-tests-otel/" not in workflow


def test_opentelemetry_conformance_runs_when_the_handlers_change() -> None:
    workflow = WORKFLOW_PATH.read_text()

    trigger_path = (
        '      - "packages/aws-durable-execution-sdk-python-conformance-tests-otel/**"'
    )
    # Once for pull_request, once for push.
    assert workflow.count(trigger_path) == 2


def test_opentelemetry_conformance_queues_complete_runs() -> None:
    workflow = yaml.safe_load(WORKFLOW_PATH.read_text())

    assert workflow["concurrency"] == {
        "group": "otel-conformance-tests",
        "cancel-in-progress": False,
        "queue": "max",
    }


def test_cloud_tests_queue_each_shared_runtime_stack() -> None:
    workflow = yaml.safe_load(WORKFLOW_PATH.with_name("cloud-tests.yml").read_text())

    assert workflow["jobs"]["example-tests"]["concurrency"] == {
        "group": "cloud-tests-${{ matrix.python-prefix }}",
        "cancel-in-progress": False,
        "queue": "max",
    }


def test_general_conformance_keeps_all_pending_runs() -> None:
    workflow = yaml.safe_load(
        WORKFLOW_PATH.with_name("conformance-tests.yml").read_text()
    )

    assert workflow["concurrency"] == {
        "group": "conformance-tests-global",
        "cancel-in-progress": False,
        "queue": "max",
    }
