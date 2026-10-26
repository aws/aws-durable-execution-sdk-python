"""Checks for the cloud tests of the MicroVM examples."""

import json
import logging
import os
import time
from collections.abc import Callable
from typing import Any

from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)

ENDED_STATES = frozenset({"TERMINATING", "TERMINATED"})
POLL_INTERVAL_SECONDS = 2.0
TIMEOUT_SECONDS = 30.0
HISTORY_TIMEOUT_SECONDS = 120.0
STATE_TIMEOUT_SECONDS = 150.0


def _region() -> str:
    return os.environ.get("AWS_REGION", "us-west-2")


def _default_microvms_client() -> Any:
    # The client is created on first use, not at import. So the test modules
    # import without AWS credentials, and the local run can skip them.
    import boto3

    return boto3.client("lambda-microvms", region_name=_region())


def _default_lambda_client() -> Any:
    # The cloud runner calls the Lambda endpoint in LAMBDA_ENDPOINT. The
    # execution history is on the same endpoint. So this client uses it too.
    import boto3

    return boto3.client(
        "lambda",
        region_name=_region(),
        endpoint_url=os.environ.get("LAMBDA_ENDPOINT") or None,
    )


def _is_not_found(error: ClientError) -> bool:
    return error.response.get("Error", {}).get("Code") == "ResourceNotFoundException"


def expect_microvm_terminated(
    microvm_id: str,
    *,
    client: Any = None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """Assert that the operation terminated the MicroVM.

    1. A TerminateMicrovm call that fails does not fail the operation. The
       operation logs the error and returns the job's result.
    2. So the operation tree shows a successful terminate step either way.
    3. So this check reads the MicroVM state with GetMicrovm instead.

    TerminateMicrovm returns before the state changes. The API does not
    document how soon GetMicrovm reports the change. So the check reads the
    state every 2 seconds, until it is TERMINATING or TERMINATED, for up to
    30 seconds. A MicroVM that GetMicrovm does not find is not running either.
    A MicroVM in another state after 30 seconds fails the test.

    The default client is created here, not at import. So the test modules
    import without AWS credentials, and the local run can skip them.
    """
    if client is None:
        client = _default_microvms_client()

    deadline = clock() + TIMEOUT_SECONDS
    while True:
        try:
            state = client.get_microvm(microvmIdentifier=microvm_id).get("state")
        except ClientError as error:
            if _is_not_found(error):
                return
            raise
        if state in ENDED_STATES:
            return
        if clock() >= deadline:
            msg = (
                f"MicroVM {microvm_id} is {state} {TIMEOUT_SECONDS:.0f} seconds "
                "after the execution ended. The operation's TerminateMicrovm "
                "call failed or did not take effect."
            )
            raise AssertionError(msg)
        sleep(POLL_INTERVAL_SECONDS)


def _find_event(
    history_pages: list[dict[str, Any]], event_type: str, name: str
) -> dict[str, Any] | None:
    for page in history_pages:
        for event in page.get("Events", []):
            if event.get("EventType") == event_type and event.get("Name") == name:
                return event
    return None


def wait_for_history_event(
    execution_arn: str,
    event_type: str,
    name: str,
    *,
    client: Any = None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    """Return the first history event with ``event_type`` and ``name``.

    1. GetDurableExecutionHistory returns the events while the execution
       still runs.
    2. So the check reads the history every 2 seconds, until it contains the
       event, for up to 120 seconds.

    GetDurableExecutionHistory answers ResourceNotFoundException for a short
    time after an asynchronous Invoke. The check reads again after that
    answer. The history is paginated. The check reads every page.
    """
    if client is None:
        client = _default_lambda_client()

    deadline = clock() + HISTORY_TIMEOUT_SECONDS
    while True:
        pages: list[dict[str, Any]] = []
        request: dict[str, Any] = {
            "DurableExecutionArn": execution_arn,
            "IncludeExecutionData": True,
        }
        try:
            while True:
                page = client.get_durable_execution_history(**request)
                pages.append(page)
                if not page.get("NextMarker"):
                    break
                request["Marker"] = page["NextMarker"]
        except ClientError as error:
            if not _is_not_found(error):
                raise
        event = _find_event(pages, event_type, name)
        if event is not None:
            return event
        if clock() >= deadline:
            msg = (
                f"The execution history had no {event_type} event named "
                f"{name} within {HISTORY_TIMEOUT_SECONDS:.0f} seconds."
            )
            raise AssertionError(msg)
        sleep(POLL_INTERVAL_SECONDS)


def find_launched_microvm_id(
    execution_arn: str,
    launch_step_name: str,
    *,
    client: Any = None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> str:
    """Return the MicroVM ID from the launch step of a running execution.

    1. The launch step checkpoints the RunMicrovm result, with the MicroVM ID.
    2. The history records the checkpoint as a StepSucceeded event with the
       step's name.
    3. So the check waits for that event with wait_for_history_event.

    Other tests launch MicroVMs from the same image at the same time. So a
    list of the image's MicroVMs could name another test's MicroVM. The
    history names only this execution's MicroVM.

    The result payload is execution data. So wait_for_history_event requests
    the history with IncludeExecutionData.
    """
    event = wait_for_history_event(
        execution_arn,
        "StepSucceeded",
        launch_step_name,
        client=client,
        sleep=sleep,
        clock=clock,
    )
    payload = event.get("StepSucceededDetails", {}).get("Result", {}).get("Payload")
    # The launch step checkpoints its result with JsonSerDes.
    return json.loads(payload)["microvmId"]


def wait_for_microvm_state(
    microvm_id: str,
    expected_state: str,
    *,
    client: Any = None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> list[str]:
    """Wait until GetMicrovm reports ``expected_state``.

    Returns the states that GetMicrovm reported, in order, with each run of
    one state reported once.

    The check reads the state every 2 seconds, for up to 150 seconds. A
    MicroVM that ends, or that GetMicrovm does not find, cannot reach the
    state any more. So the check fails at once in that case. A MicroVM in
    another state after 150 seconds fails the check too.
    """
    if client is None:
        client = _default_microvms_client()

    observed: list[str] = []
    deadline = clock() + STATE_TIMEOUT_SECONDS
    while True:
        try:
            state = client.get_microvm(microvmIdentifier=microvm_id).get("state")
        except ClientError as error:
            if _is_not_found(error):
                msg = (
                    f"GetMicrovm did not find MicroVM {microvm_id} before it "
                    f"was {expected_state}. States seen: {observed}"
                )
                raise AssertionError(msg) from error
            raise
        if not observed or observed[-1] != state:
            observed.append(state)
        if state == expected_state:
            return observed
        if state in ENDED_STATES:
            msg = (
                f"MicroVM {microvm_id} is {state} before it was "
                f"{expected_state}. States seen: {observed}"
            )
            raise AssertionError(msg)
        if clock() >= deadline:
            msg = (
                f"MicroVM {microvm_id} was not {expected_state} within "
                f"{STATE_TIMEOUT_SECONDS:.0f} seconds. States seen: {observed}"
            )
            raise AssertionError(msg)
        sleep(POLL_INTERVAL_SECONDS)


TEST_FAILED_ERROR = ErrorObject(
    message="The test failed before it completed the callback.",
    type="TestFailed",
    data=None,
    stack_trace=None,
)


def fail_callback_after_test_failure(runner: Any, callback_id: str) -> None:
    """Fail a callback that a failed test left pending.

    1. A pending callback holds the execution until the callback times out.
    2. A callback failure ends the wait at once. The handler then raises
       CallbackError, and the session terminates its MicroVM.
    3. So the test calls this function when it fails before it completes the
       callback.

    The call runs while the test's own error propagates. An error from the
    call would replace that error. So this function logs the error and
    returns.
    """
    try:
        runner.send_callback_failure(callback_id=callback_id, error=TEST_FAILED_ERROR)
    except Exception:
        logger.warning(
            "Could not fail callback %s after the test failed.",
            callback_id,
            exc_info=True,
        )
