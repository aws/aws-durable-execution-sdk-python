"""End-to-end handling of invalid invocation outputs through the test runner."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import ErrorObject

from aws_durable_execution_sdk_python_testing.execution import ExecutionStatus
from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)
from aws_durable_execution_sdk_python_testing.stores.base import ExecutionStore
from aws_durable_execution_sdk_python_testing.stores.filesystem import (
    FileSystemExecutionStore,
)
from aws_durable_execution_sdk_python_testing.stores.memory import (
    InMemoryExecutionStore,
)
from aws_durable_execution_sdk_python_testing.stores.sqlite import (
    SQLiteExecutionStore,
)


@pytest.mark.parametrize(
    ("output", "message"),
    [
        pytest.param(
            {"Status": "FAILED", "Result": "result"},
            "Cannot provide a Result for FAILED status.",
            id="failed-with-result",
        ),
        pytest.param(
            {"Status": "SUCCEEDED", "Error": {"ErrorMessage": "boom"}},
            "Cannot provide an Error for SUCCEEDED status.",
            id="succeeded-with-error",
        ),
        pytest.param(
            {"Result": "result"},
            "Invalid invocation output: None is not a valid InvocationStatus",
            id="missing-status",
        ),
        pytest.param(
            {"Status": "DONE"},
            "Invalid invocation output: 'DONE' is not a valid InvocationStatus",
            id="unknown-status",
        ),
        pytest.param(
            {"Status": "RETRY"},
            "Unexpected invocation status: InvocationStatus.RETRY",
            id="internal-status",
        ),
        pytest.param(
            ["SUCCEEDED"],
            "Invalid invocation output: 'list' object has no attribute 'get'",
            id="not-an-object",
        ),
    ],
)
def test_invalid_output_fails_the_execution_after_one_invocation(
    output: Any, message: str
) -> None:
    """The service fails the execution on the first invalid output, with an
    InvalidParameterValueException error, and does not invoke again."""
    invocations: list[Any] = []

    def handler(event: Any, context: Any) -> Any:  # noqa: ARG001
        invocations.append(event)
        return output

    with DurableFunctionTestRunner(handler=handler, execution_timeout=10) as runner:
        execution_arn = runner.run_async(input="input str")
        result: DurableFunctionTestResult = runner.wait_for_result(
            execution_arn, timeout=10
        )

    assert len(invocations) == 1
    assert result.status is InvocationStatus.FAILED
    assert result.execution_status is ExecutionStatus.FAILED
    assert result.error == ErrorObject(
        message=message,
        type="InvalidParameterValueException",
        data=None,
        stack_trace=None,
    )


@pytest.mark.parametrize(
    "store_factory",
    [
        pytest.param(lambda _tmp: InMemoryExecutionStore(), id="memory"),
        pytest.param(
            lambda tmp: FileSystemExecutionStore.create(tmp / "fs-store"),
            id="filesystem",
        ),
        pytest.param(
            lambda tmp: SQLiteExecutionStore.create_and_initialize(
                str(tmp / "exec.db")
            ),
            id="sqlite",
        ),
    ],
)
@pytest.mark.parametrize(
    ("error", "message"),
    [
        pytest.param(
            {},
            "Cannot provide an Error for SUCCEEDED status.",
            id="empty-object",
        ),
        pytest.param(
            [],
            "Invalid invocation output: 'list' object has no attribute 'get'",
            id="empty-list",
        ),
        pytest.param(
            0,
            "Invalid invocation output: 'int' object has no attribute 'get'",
            id="zero",
        ),
        pytest.param(
            False,
            "Invalid invocation output: 'bool' object has no attribute 'get'",
            id="false",
        ),
    ],
)
def test_falsey_error_fails_a_succeeded_output_on_each_store(
    store_factory: Callable[[Path], ExecutionStore],
    error: Any,
    message: str,
    tmp_path: Path,
) -> None:
    """The SDK reads any falsey Error as no Error. An empty Error object is
    still an Error, and the other falsey values are not objects, so each
    fails the execution after one invocation, on every store."""
    invocations: list[Any] = []

    def handler(event: Any, context: Any) -> Any:  # noqa: ARG001
        invocations.append(event)
        return {"Status": "SUCCEEDED", "Error": error}

    with DurableFunctionTestRunner(
        handler=handler, store=store_factory(tmp_path), execution_timeout=10
    ) as runner:
        execution_arn = runner.run_async(input="input str")
        result: DurableFunctionTestResult = runner.wait_for_result(
            execution_arn, timeout=10
        )

    assert len(invocations) == 1
    assert result.status is InvocationStatus.FAILED
    assert result.execution_status is ExecutionStatus.FAILED
    assert result.error == ErrorObject(
        message=message,
        type="InvalidParameterValueException",
        data=None,
        stack_trace=None,
    )
