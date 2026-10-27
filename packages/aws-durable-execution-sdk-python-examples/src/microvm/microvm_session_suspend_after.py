"""Run two jobs in one Lambda MicroVM that the worker suspends after the first.

The MicroVM runs the image that scripts/ensure_microvm_image.py builds from
microvm-image/. The image's routes /write and /read write and read a file in
the MicroVM. The image enables the resume hook. So the worker accepts the
next job as soon as the MicroVM resumes.

The session does not suspend when idle. The first job sets suspend_after, so
the worker suspends the MicroVM after that job. A callback named gate holds
the second job. The caller completes the callback with
SendDurableExecutionCallbackSuccess to start the second job.
"""

import os
from typing import Any

from aws_durable_execution_sdk_python.config import CallbackConfig, Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python_extras.microvm import (
    MicrovmInvokeOptions,
    MicrovmSession,
    MicrovmSessionConfig,
    microvm_session,
)


# The callback named gate holds the second job until the caller completes it.
# If the caller never completes it, the callback times out after 5 minutes.
# The handler then raises an error, and the session terminates the MicroVM.
# So a failed caller leaves the MicroVM suspended for 5 minutes, not for the
# whole execution timeout.
GATE = CallbackConfig(timeout=Duration.from_minutes(5))

# The second job waits for the resume. The worker can refuse it with HTTP 503
# for up to 30 seconds before that, and the request step retries for up to
# 60 seconds. So its heartbeat timeout is longer than both together.
RESUMED_JOB_HEARTBEAT_TIMEOUT = Duration.from_seconds(120)


def _require_microvm_env() -> tuple[str, str]:
    """Return the image ARN and the MicroVM execution role ARN.

    The SAM template sets both variables on every example with
    ``"usesMicrovm": true``. scripts/build_examples.py copies every file under
    src/ into one directory. So a shared helper module would need a different
    import in Lambda than in the local tests. So each MicroVM example defines
    this function itself.
    """
    image_arn = os.environ.get("MICROVM_IMAGE_ARN")
    execution_role_arn = os.environ.get("MICROVM_EXECUTION_ROLE_ARN")
    if not image_arn or not execution_role_arn:
        msg = (
            "MICROVM_IMAGE_ARN and MICROVM_EXECUTION_ROLE_ARN must be set. "
            "Build the image with scripts/ensure_microvm_image.py."
        )
        raise RuntimeError(msg)
    return image_arn, execution_role_arn


@durable_execution
def handler(event: dict[str, Any], context: DurableContext) -> Any:
    image_arn, execution_role_arn = _require_microvm_env()
    key: str = event["key"]

    def write_gate_read(vm: MicrovmSession, session_context: DurableContext) -> Any:
        # The worker suspends the MicroVM when this job has ended and its
        # result is reported.
        written = vm.invoke(
            session_context,
            "write",
            job_input={"key": key, "value": f"written-by-{key}"},
            options=MicrovmInvokeOptions(
                path="/write",
                timeout=Duration.from_minutes(5),
                # A job that the MicroVM stops answering fails after this
                # time, instead of after the timeout.
                heartbeat_timeout=Duration.from_seconds(30),
                suspend_after=True,
            ),
        )

        # 1. The first job has ended before the gate starts.
        # 2. The second job starts only after the caller completes the gate.
        # 3. So no job runs while the gate is pending.
        # 4. So the worker suspends the MicroVM after the first job.
        # 5. So a caller that sees the MicroVM SUSPENDED while the gate is
        #    pending sees a suspend between the two jobs.
        # The callback result carries no data. So the handler ignores it.
        session_context.create_callback(name="gate", config=GATE).result()

        # The first job set suspend_after. So the request step calls
        # GetMicrovm, finds the MicroVM SUSPENDED, and calls ResumeMicrovm
        # before it sends the job. A suspended MicroVM keeps its files. So
        # this job reads the file that the first job wrote.
        read = vm.invoke(
            session_context,
            "read",
            job_input={"key": key},
            options=MicrovmInvokeOptions(
                path="/read",
                timeout=Duration.from_minutes(5),
                heartbeat_timeout=RESUMED_JOB_HEARTBEAT_TIMEOUT,
            ),
        )

        return {
            "sessionMicrovmId": vm.microvm_id,
            "writtenBy": written["microvmId"],
            "readBy": read["microvmId"],
            "value": read["value"],
        }

    # The session launches one MicroVM, and terminates it when the handler
    # returns. It does not suspend when idle.
    return microvm_session(
        context,
        "suspend-after-session",
        config=MicrovmSessionConfig(
            image_identifier=image_arn,
            execution_role_arn=execution_role_arn,
            timeout=Duration.from_minutes(10),
        ),
        handler=write_gate_read,
    )
