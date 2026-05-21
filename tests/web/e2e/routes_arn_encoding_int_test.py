"""Integration test: WebServer route layer URL-decodes DurableExecutionArn.

Drives a real ``boto3`` Lambda client against a live ``WebServer`` and asserts
that ``DurableExecutionArn`` values containing characters that boto
percent-encodes in URI labels (e.g. ``/`` -> ``%2F``) round-trip correctly so
the store lookup hits.
"""

from __future__ import annotations

import threading
import time
from typing import Any

import boto3  # type: ignore
import pytest
from botocore.config import Config  # type: ignore
from botocore.exceptions import ClientError  # type: ignore

from aws_durable_execution_sdk_python_testing.checkpoint.processor import (
    CheckpointProcessor,
)
from aws_durable_execution_sdk_python_testing.execution import Execution
from aws_durable_execution_sdk_python_testing.executor import Executor
from aws_durable_execution_sdk_python_testing.model import (
    StartDurableExecutionInput,
)
from aws_durable_execution_sdk_python_testing.scheduler import Scheduler
from aws_durable_execution_sdk_python_testing.stores.memory import (
    InMemoryExecutionStore,
)
from aws_durable_execution_sdk_python_testing.web.server import (
    WebServer,
    WebServiceConfig,
)


class _NoOpInvoker:
    """Satisfies the Invoker protocol without invoking anything.

    The route-layer regression doesn't depend on actually executing the
    function; the executor just needs *some* invoker to construct it.
    """

    def create_invocation_input(self, execution: Any) -> Any:  # noqa: ARG002
        return None

    def invoke(self, *args: Any, **kwargs: Any) -> Any:  # noqa: ARG002
        return None

    def update_endpoint(self, *args: Any, **kwargs: Any) -> None:  # noqa: ARG002
        return None


def _assert_no_percent_encoding_in_error(exc: ClientError, arn: str) -> None:
    """Fail the test if a ResourceNotFoundException carries a %2F-form ARN.

    Other errors (e.g. invalid checkpoint token, wrong state) are fine; this
    test is narrowly about whether the route layer decoded the path segment.
    """
    msg = str(exc)
    assert "%2F" not in msg, (
        f"WebServer route layer did not URL-decode DurableExecutionArn. "
        f"Original ARN: {arn!r}. Error: {msg}"
    )


@pytest.fixture
def server_with_slash_arn():
    """Yield ``(boto_client, arn, executor, store)`` for a live WebServer.

    The yielded ARN contains a literal ``/`` matching the v1.2.0+ format
    produced by ``Execution.new()``. The Execution is pre-started and saved
    so read paths have something to find.
    """
    store = InMemoryExecutionStore()
    scheduler = Scheduler()
    checkpoint_processor = CheckpointProcessor(store=store, scheduler=scheduler)
    executor = Executor(
        store=store,
        scheduler=scheduler,
        invoker=_NoOpInvoker(),
        checkpoint_processor=checkpoint_processor,
    )
    checkpoint_processor.add_execution_observer(executor)
    scheduler.start()

    # Hand-build a started Execution whose ARN contains '/' so we control
    # the format under test without going through executor.start_execution
    # (which schedules a real invoke + timeout).
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-fn",
        function_qualifier="$LATEST",
        execution_name="test-exec",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="inv-12345",
        input='"hi"',
    )
    execution = Execution.new(start_input)
    execution.start()
    store.save(execution)
    arn = execution.durable_execution_arn
    assert "/" in arn, "regression precondition: ARN must contain literal '/'"

    config = WebServiceConfig(host="127.0.0.1", port=0)
    server = WebServer(config, executor)
    port = server.server_address[1]
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    # Give the listener a beat to come up before the boto client connects.
    time.sleep(0.05)

    client = boto3.client(
        "lambda",
        endpoint_url=f"http://127.0.0.1:{port}",
        region_name="us-east-1",
        aws_access_key_id="x",  # noqa: S106 - test stub
        aws_secret_access_key="y",  # noqa: S106 - test stub
        config=Config(parameter_validation=False, retries={"max_attempts": 0}),
    )

    try:
        yield client, arn, executor, store
    finally:
        server.shutdown()
        server.server_close()
        scheduler.stop()


def test_get_durable_execution_decodes_slash_in_arn(server_with_slash_arn):
    """GetDurableExecution: %2F must be decoded so the store lookup hits."""
    client, arn, _executor, _store = server_with_slash_arn

    response = client.get_durable_execution(DurableExecutionArn=arn)

    assert response["DurableExecutionArn"] == arn


def test_get_durable_execution_state_decodes_slash_in_arn(server_with_slash_arn):
    """GetDurableExecutionState: %2F must be decoded so the store lookup hits."""
    client, arn, _executor, _store = server_with_slash_arn

    response = client.get_durable_execution_state(
        DurableExecutionArn=arn,
        CheckpointToken="ignored-by-route-layer",  # noqa: S106 - test stub
    )

    # Response shape varies; the only assertion this test cares about is
    # that we got past route resolution.
    assert response is not None


def test_get_durable_execution_history_decodes_slash_in_arn(server_with_slash_arn):
    """GetDurableExecutionHistory: %2F must be decoded so the store lookup hits."""
    client, arn, _executor, _store = server_with_slash_arn

    response = client.get_durable_execution_history(DurableExecutionArn=arn)

    assert response is not None


def test_checkpoint_durable_execution_decodes_slash_in_arn(server_with_slash_arn):
    """CheckpointDurableExecution: %2F must be decoded so the store lookup hits.

    A checkpoint with no operation updates may still trip secondary
    validation; we only assert the failure (if any) is not the
    %2F-in-message 404 that indicates the route layer dropped the ball.
    """
    client, arn, _executor, store = server_with_slash_arn
    execution = store.load(arn)
    token = execution.get_new_checkpoint_token()

    try:
        client.checkpoint_durable_execution(
            DurableExecutionArn=arn,
            CheckpointToken=token,
            Updates=[],
        )
    except ClientError as exc:
        _assert_no_percent_encoding_in_error(exc, arn)


def test_stop_durable_execution_decodes_slash_in_arn(server_with_slash_arn):
    """StopDurableExecution: %2F must be decoded so the store lookup hits."""
    client, arn, _executor, _store = server_with_slash_arn

    try:
        client.stop_durable_execution(DurableExecutionArn=arn)
    except ClientError as exc:
        _assert_no_percent_encoding_in_error(exc, arn)


def test_list_durable_executions_by_function_decodes_colon_in_name(
    server_with_slash_arn,
):
    """ListDurableExecutionsByFunction: %3A/%24 in FunctionName must be decoded.

    boto percent-encodes ``:`` and ``$`` in the non-greedy ``{FunctionName}``
    URI label, so a realistic value like ``MyFunction:$LATEST`` arrives as
    ``MyFunction%3A%24LATEST``. The route layer must decode the segment so
    the store's exact-match filter on ``function_name`` returns the expected
    execution.

    Pre-fix behavior: handler filters on the encoded string, response has
    no executions. Post-fix: handler filters on the decoded string, response
    returns the seeded execution.
    """
    client, _arn, _executor, store = server_with_slash_arn

    # Seed an execution whose function_name contains characters boto encodes.
    realistic_function_name = "MyFunction:$LATEST"
    seed = StartDurableExecutionInput(
        account_id="123456789012",
        function_name=realistic_function_name,
        function_qualifier="$LATEST",
        execution_name="encoded-fn-exec",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="inv-encoded-fn",
        input='"hi"',
    )
    seeded = Execution.new(seed)
    seeded.start()
    store.save(seeded)

    response = client.list_durable_executions_by_function(
        FunctionName=realistic_function_name,
    )

    arns = [e["DurableExecutionArn"] for e in response.get("DurableExecutions", [])]
    assert seeded.durable_execution_arn in arns, (
        f"WebServer route layer did not URL-decode FunctionName. "
        f"Seeded function_name {realistic_function_name!r} produced arn "
        f"{seeded.durable_execution_arn!r}, but list response contained "
        f"{arns!r}."
    )
