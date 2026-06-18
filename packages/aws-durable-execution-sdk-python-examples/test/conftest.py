"""Pytest configuration and fixtures for durable execution tests."""

import contextlib
import json
import logging
import os
import sys
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

import pytest
from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionCloudTestRunner,
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)

from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    OperationPayload,
)
from aws_durable_execution_sdk_python.serdes import ExtendedTypeSerDes


# Add examples/src to Python path for imports
examples_src = Path(__file__).parent.parent / "src"
if str(examples_src) not in sys.path:
    sys.path.insert(0, str(examples_src))


logger = logging.getLogger(__name__)


def deserialize_operation_payload(
    payload: OperationPayload | None, serdes: ExtendedTypeSerDes | None = None
) -> Any:
    """Deserialize an operation payload using the provided or default serializer.

    This utility function helps test code deserialize operation results that are
    returned as raw strings. It supports both the default ExtendedTypeSerDes and
    custom serializers.

    Args:
        payload: The operation payload string to deserialize, or None.
        serdes: Optional custom serializer. If None, uses ExtendedTypeSerDes.

    Returns:
        Deserialized result object, or None if payload is None.
    """
    if not payload:
        return None

    if serdes is None:
        serdes = ExtendedTypeSerDes()

    try:
        return serdes.deserialize(payload)
    except Exception:
        # Fallback to plain JSON for backwards compatibility
        return json.loads(payload)


class RunnerMode(StrEnum):
    """Runner mode for local or cloud execution."""

    LOCAL = "local"
    CLOUD = "cloud"


def pytest_addoption(parser):
    """Add custom command line options for test execution."""
    parser.addoption(
        "--runner-mode",
        action="store",
        default=RunnerMode.LOCAL,
        choices=[RunnerMode.LOCAL, RunnerMode.CLOUD],
        help="Test runner mode: local (in-memory) or cloud (deployed Lambda)",
    )


class TestRunnerAdapter:
    """Adapter that provides consistent interface for both local and cloud runners.

    This adapter encapsulates the differences between local and cloud test runners:
    - Local runner: Requires context manager for resource cleanup (scheduler thread)
    - Cloud runner: No resource cleanup needed (stateless boto3 client)

    The adapter ensures proper resource management while providing a unified interface.
    """

    def __init__(
        self,
        runner: DurableFunctionTestRunner | DurableFunctionCloudTestRunner,
        mode: str,
    ):
        """Initialize the adapter."""
        self._runner: DurableFunctionTestRunner | DurableFunctionCloudTestRunner = (
            runner
        )
        self._mode: str = mode

    def run(
        self,
        input: str | None = None,  # noqa: A002
        timeout: int = 60,
    ) -> DurableFunctionTestResult:
        """Execute the durable function and return results."""
        return self._runner.run(input=input, timeout=timeout)

    def run_async(
        self,
        input: str | None = None,  # noqa: A002
        timeout: int = 60,
    ) -> str:
        return self._runner.run_async(input=input, timeout=timeout)

    def send_callback_success(
        self, callback_id: str, result: bytes | None = None
    ) -> None:
        self._runner.send_callback_success(callback_id=callback_id, result=result)

    def send_callback_failure(
        self, callback_id: str, error: ErrorObject | None = None
    ) -> None:
        self._runner.send_callback_failure(callback_id=callback_id, error=error)

    def send_callback_heartbeat(self, callback_id: str) -> None:
        self._runner.send_callback_heartbeat(callback_id=callback_id)

    def wait_for_result(
        self, execution_arn: str, timeout: int = 60
    ) -> DurableFunctionTestResult:
        return self._runner.wait_for_result(
            execution_arn=execution_arn, timeout=timeout
        )

    def wait_for_callback(
        self, execution_arn: str, name: str | None = None, timeout: int = 60
    ) -> str:
        return self._runner.wait_for_callback(
            execution_arn=execution_arn, name=name, timeout=timeout
        )

    @property
    def mode(self) -> str:
        """Get the runner mode (local or cloud)."""
        return self._mode

    def __enter__(self):
        """Context manager entry - only calls runner's __enter__ if it's a context manager."""
        if isinstance(self._runner, contextlib.AbstractContextManager):
            self._runner.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - only calls runner's __exit__ if it's a context manager."""
        if isinstance(self._runner, contextlib.AbstractContextManager):
            return self._runner.__exit__(exc_type, exc_val, exc_tb)
        return None


@pytest.fixture
def durable_runner(request):
    """Pytest fixture that provides a test runner based on configuration.

    Configuration for cloud mode:
        Environment variables (required):
            AWS_REGION: AWS region for Lambda invocation (default: us-west-2)
            LAMBDA_ENDPOINT: Optional Lambda endpoint URL
            PYTEST_FUNCTION_NAME_MAP: JSON mapping of example names to deployed function names
        
        CLI option:
            --runner-mode=cloud (or local, default: local)
        
        Example:
            AWS_REGION=us-west-2 \
            LAMBDA_ENDPOINT=https://lambda.us-west-2.amazonaws.com \
            PYTEST_FUNCTION_NAME_MAP='{"hello world":"HelloWorld:$LATEST"}' \
            pytest --runner-mode=cloud -k test_hello_world

    Usage in tests:
        @pytest.mark.durable_execution(
            handler=hello_world.handler,
            lambda_function_name="hello world"
        )
        def test_hello_world(durable_runner):
            with durable_runner:
                result = durable_runner.run(input="test", timeout=10)
            assert result.status == InvocationStatus.SUCCEEDED
    """
    # Get marker with test configuration
    marker = request.node.get_closest_marker("durable_execution")
    if not marker:
        pytest.fail("Test must be marked with @pytest.mark.durable_execution")

    handler: Any = marker.kwargs.get("handler")
    lambda_function_name: str | None = marker.kwargs.get("lambda_function_name")

    # Get runner mode from CLI option
    runner_mode: str = request.config.getoption("--runner-mode")

    logger.info("Running test in %s mode", runner_mode.upper())

    # Create appropriate runner
    if runner_mode == RunnerMode.CLOUD:
        # Get deployed function name and AWS config from environment
        deployed_name = _get_deployed_function_name(request, lambda_function_name)
        region = os.environ.get("AWS_REGION", "us-west-2")
        lambda_endpoint = os.environ.get("LAMBDA_ENDPOINT")

        logger.info("Using AWS region: %s", region)

        # Create cloud runner (no cleanup needed)
        runner = DurableFunctionCloudTestRunner(
            function_name=deployed_name,
            region=region,
            lambda_endpoint=lambda_endpoint,
        )
    else:
        if not handler:
            pytest.fail("handler is required for local mode tests")
        # Create local runner (needs cleanup via context manager)
        runner = DurableFunctionTestRunner(handler=handler)

    # Wrap in adapter and use context manager for proper cleanup
    with TestRunnerAdapter(runner, runner_mode) as adapter:
        yield adapter


def _get_deployed_function_name(
    request: pytest.FixtureRequest,
    lambda_function_name: str | None,
) -> str:
    """Get the deployed function name from environment variables.

    Required environment variables:
    - QUALIFIED_FUNCTION_NAME: The qualified function ARN (e.g., "MyFunction:$LATEST")
    - LAMBDA_FUNCTION_TEST_NAME: The lambda function name to match against test markers

    Tests are skipped if the test's lambda_function_name doesn't match LAMBDA_FUNCTION_TEST_NAME.
    """
    if not lambda_function_name:
        pytest.fail("lambda_function_name is required for cloud mode tests")

    # Get from environment variables
    function_arn = os.environ.get("QUALIFIED_FUNCTION_NAME")
    env_function_name = os.environ.get("LAMBDA_FUNCTION_TEST_NAME")

    if not function_arn or not env_function_name:
        pytest.fail(
            "Cloud mode requires both QUALIFIED_FUNCTION_NAME and LAMBDA_FUNCTION_TEST_NAME environment variables\n"
            'Example: QUALIFIED_FUNCTION_NAME="MyFunction:$LATEST" LAMBDA_FUNCTION_TEST_NAME="hello world" pytest --runner-mode=cloud'
        )

    # Check if this test matches the function name (case-insensitive)
    if lambda_function_name.lower() == env_function_name.lower():
        logger.info(
            "Using function ARN: %s for lambda function: %s",
            function_arn,
            env_function_name,
        )
        return function_arn

    # This test doesn't match the function name, skip it
    pytest.skip(
        f"Test '{lambda_function_name}' doesn't match LAMBDA_FUNCTION_TEST_NAME '{env_function_name}'"
    )


# X-Ray ingestion is eventually consistent; give the backend time to receive and
# index spans before querying, then retry a few times.
_XRAY_QUERY_RETRIES = 3
_XRAY_RETRY_DELAY_SECONDS = 10


class XRaySpanFetcher:
    """Encapsulates all AWS X-Ray interaction for span-validation tests.

    Wraps a boto3 X-Ray client and exposes a single high-level operation that
    queries trace summaries in a time window (with retries for eventual
    consistency), batch-fetches the full traces, and locates the trace whose
    segment documents reference a marker span name.
    """

    def __init__(self, client: Any):
        """Initialize with a boto3 X-Ray client."""
        self._client = client

    def _query_trace_summaries(
        self, start_time: datetime, end_time: datetime
    ) -> list[dict]:
        """Query trace summaries in a window, retrying for consistency."""
        import time

        for attempt in range(_XRAY_QUERY_RETRIES):
            response = self._client.get_trace_summaries(
                StartTime=start_time,
                EndTime=end_time,
                TimeRangeType="Event",
                Sampling=False,
            )
            summaries = response.get("TraceSummaries", [])
            if summaries:
                return summaries

            logger.info(
                "X-Ray query returned 0 traces, retrying in %ss (attempt %d/%d)",
                _XRAY_RETRY_DELAY_SECONDS,
                attempt + 1,
                _XRAY_QUERY_RETRIES,
            )
            time.sleep(_XRAY_RETRY_DELAY_SECONDS)
        return []

    def fetch_trace_with_span(
        self,
        start_time: datetime,
        end_time: datetime,
        marker_span: str,
    ) -> tuple[str, str]:
        """Find the trace containing ``marker_span`` and return its segment text.

        Queries trace summaries in the window, then batch-fetches full traces
        (X-Ray caps BatchGetTraces at 5 trace IDs per call) and locates the
        trace whose segment documents reference the marker span name.

        Args:
            start_time: Start of the X-Ray query window.
            end_time: End of the X-Ray query window.
            marker_span: A span name expected to appear in the target trace.

        Returns:
            A tuple of (trace_id, concatenated segment-document JSON text).
        """
        summaries = self._query_trace_summaries(start_time, end_time)
        assert summaries, "Expected at least one trace in X-Ray after execution"

        trace_ids = [s["Id"] for s in summaries]

        for i in range(0, len(trace_ids), 5):
            batch = trace_ids[i : i + 5]
            result = self._client.batch_get_traces(TraceIds=batch)
            for trace in result.get("Traces", []):
                documents = [
                    seg.get("Document", "") for seg in trace.get("Segments", [])
                ]
                segment_text = "\n".join(documents)
                if marker_span in segment_text:
                    return trace["Id"], segment_text

        pytest.fail(
            f"Did not find a trace containing span '{marker_span}' in the time "
            f"window across {len(trace_ids)} trace(s)"
        )


@pytest.fixture
def xray_spans(request):
    """Provide an XRaySpanFetcher for cloud-mode span validation tests.

    The underlying boto3 X-Ray client is created in the same region as the
    cloud runner (AWS_REGION, default us-west-2). In local mode there is no
    X-Ray backend, so the fixture skips the test, mirroring the cloud-only
    gating of the durable_runner cloud path.
    """
    runner_mode: str = request.config.getoption("--runner-mode")
    if runner_mode != RunnerMode.CLOUD:
        pytest.skip("X-Ray span validation only runs in cloud mode")

    import boto3

    region = os.environ.get("AWS_REGION", "us-west-2")
    return XRaySpanFetcher(boto3.client("xray", region_name=region))
