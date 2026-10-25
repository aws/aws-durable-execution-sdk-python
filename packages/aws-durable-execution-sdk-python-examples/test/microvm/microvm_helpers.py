"""Checks for the cloud tests of the MicroVM examples."""

import json
import os
import time
from collections.abc import Callable
from typing import Any

from botocore.exceptions import ClientError


ENDED_STATES = frozenset({"TERMINATING", "TERMINATED"})
POLL_INTERVAL_SECONDS = 2.0
TIMEOUT_SECONDS = 30.0
LAUNCH_TIMEOUT_SECONDS = 120.0
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


def _launch_result(history_pages: list[dict[str, Any]], step_name: str) -> Any:
    for page in history_pages:
        for event in page.get("Events", []):
            if event.get("EventType") == "StepSucceeded" and (
                event.get("Name") == step_name
            ):
                payload = (
                    event.get("StepSucceededDetails", {})
                    .get("Result", {})
                    .get("Payload")
                )
                # The launch step checkpoints its result with JsonSerDes.
                return json.loads(payload)
    return None


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
    2. GetDurableExecutionHistory returns the checkpoint while the execution
       still runs.
    3. So the check reads the history every 2 seconds, until the launch step
       has succeeded, for up to 120 seconds.

    Other tests launch MicroVMs from the same image at the same time. So a
    list of the image's MicroVMs could name another test's MicroVM. The
    history names only this execution's MicroVM.

    GetDurableExecutionHistory answers ResourceNotFoundException for a short
    time after an asynchronous Invoke. The check reads again after that
    answer. The history is paginated. The check reads every page.
    """
    if client is None:
        client = _default_lambda_client()

    deadline = clock() + LAUNCH_TIMEOUT_SECONDS
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
        launch = _launch_result(pages, launch_step_name)
        if launch is not None:
            return launch["microvmId"]
        if clock() >= deadline:
            msg = (
                f"The step {launch_step_name} did not succeed within "
                f"{LAUNCH_TIMEOUT_SECONDS:.0f} seconds of the execution start."
            )
            raise AssertionError(msg)
        sleep(POLL_INTERVAL_SECONDS)


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
