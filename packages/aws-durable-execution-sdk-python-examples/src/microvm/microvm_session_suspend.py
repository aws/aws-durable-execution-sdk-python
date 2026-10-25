"""Run two jobs in one Lambda MicroVM that suspends itself between them.

The MicroVM runs the image that scripts/ensure_microvm_image.py builds from
microvm-image/. The image's routes /write and /read write and read a file in
the MicroVM. The image enables the resume hook. So the worker accepts the
next job as soon as the MicroVM resumes.
"""

import os
from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python_extras.microvm import (
    MicrovmInvokeOptions,
    MicrovmSession,
    MicrovmSessionConfig,
    microvm_session,
)


# 1. The worker starts its idle time of 10 seconds at the run hook, and again
#    after each job. So the idle time starts again when the first job ends.
# 2. When the idle time ends, the worker calls SuspendMicrovm. The call
#    returns before the MicroVM freezes. The image gives the suspend hook up
#    to 10 seconds.
# 3. The service does not document how long the freeze takes. So this
#    example assumes that the MicroVM is SUSPENDED within about 25 seconds
#    of the end of the first job.
# 4. The wait starts after the first job ends. So the wait ends more than 60
#    seconds after the first job ends.
# 5. So the MicroVM stays SUSPENDED for about 35 seconds or more before the
#    second job resumes it.
# 6. The cloud test reads the MicroVM state every 2 seconds. So it observes
#    the SUSPENDED state.
# A longer wait would only make the test slower.
PAUSE = Duration.from_seconds(60)
IDLE_TIME = Duration.from_seconds(10)


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


def _job_options(path: str) -> MicrovmInvokeOptions:
    return MicrovmInvokeOptions(
        path=path,
        timeout=Duration.from_minutes(5),
        # A job that the MicroVM stops answering fails after this time,
        # instead of after the timeout.
        heartbeat_timeout=Duration.from_seconds(30),
    )


@durable_execution
def handler(event: dict[str, Any], context: DurableContext) -> Any:
    image_arn, execution_role_arn = _require_microvm_env()
    key: str = event["key"]

    def write_pause_read(vm: MicrovmSession, session_context: DurableContext) -> Any:
        written = vm.invoke(
            session_context,
            "write",
            job_input={"key": key, "value": f"written-by-{key}"},
            options=_job_options("/write"),
        )

        # No job runs during the wait. So the worker suspends the MicroVM.
        session_context.wait(PAUSE, name="pause")

        # The request step calls GetMicrovm, finds the MicroVM SUSPENDED, and
        # calls ResumeMicrovm before it sends the job. A suspended MicroVM
        # keeps its files. So this job reads the file that the first job
        # wrote.
        read = vm.invoke(
            session_context,
            "read",
            job_input={"key": key},
            options=_job_options("/read"),
        )

        return {
            "sessionMicrovmId": vm.microvm_id,
            "writtenBy": written["microvmId"],
            "readBy": read["microvmId"],
            "value": read["value"],
        }

    # The session launches one MicroVM, and terminates it when the handler
    # returns.
    return microvm_session(
        context,
        "suspend-session",
        config=MicrovmSessionConfig(
            image_identifier=image_arn,
            execution_role_arn=execution_role_arn,
            timeout=Duration.from_minutes(10),
            auto_suspend_on_idle=True,
            auto_suspend_idle_time=IDLE_TIME,
        ),
        handler=write_pause_read,
    )
