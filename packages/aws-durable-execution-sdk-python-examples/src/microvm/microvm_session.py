"""Run two jobs in one Lambda MicroVM, with a durable wait between them.

The MicroVM runs the image that scripts/ensure_microvm_image.py builds from
microvm-image/. The image's routes /write and /read write and read a file in
the MicroVM.
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

    def write_then_read(vm: MicrovmSession, session_context: DurableContext) -> Any:
        # Each vm.invoke sends one job over HTTP to a route of the app in the
        # MicroVM.
        written = vm.invoke(
            session_context,
            "write",
            job_input={"key": key, "value": f"written-by-{key}"},
            options=_job_options("/write"),
        )

        # The wait ends this invocation. The MicroVM keeps running. So the
        # next job finds the file that the first job wrote.
        session_context.wait(Duration.from_seconds(5), name="pause")

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
        "file-session",
        config=MicrovmSessionConfig(
            image_identifier=image_arn,
            execution_role_arn=execution_role_arn,
            timeout=Duration.from_minutes(10),
        ),
        handler=write_then_read,
    )
