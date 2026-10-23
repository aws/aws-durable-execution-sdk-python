"""Checks for the cloud tests of the MicroVM examples."""

import os
import time
from collections.abc import Callable
from typing import Any

from botocore.exceptions import ClientError


ENDED_STATES = frozenset({"TERMINATING", "TERMINATED"})
POLL_INTERVAL_SECONDS = 2.0
TIMEOUT_SECONDS = 30.0


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
        import boto3

        client = boto3.client(
            "lambda-microvms", region_name=os.environ.get("AWS_REGION", "us-west-2")
        )

    deadline = clock() + TIMEOUT_SECONDS
    while True:
        try:
            state = client.get_microvm(microvmIdentifier=microvm_id).get("state")
        except ClientError as error:
            if (
                error.response.get("Error", {}).get("Code")
                == "ResourceNotFoundException"
            ):
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
