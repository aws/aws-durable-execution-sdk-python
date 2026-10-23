"""Run one job in a Lambda MicroVM and return the result that it reports.

The MicroVM runs the image that scripts/ensure_microvm_image.py builds from
microvm-image/. The image's default handler counts the words of the text and
hashes it.
"""

import os
from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python_extras.microvm import MicrovmConfig, microvm


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

    # The operation launches a MicroVM from the image and waits on a durable
    # callback. The input is small, so it goes in the MicroVM's run hook. The
    # worker in the MicroVM runs the job and completes the callback. The
    # operation then terminates the MicroVM.
    return microvm(
        context,
        "count-words",
        job_input={"text": event["text"]},
        config=MicrovmConfig(
            image_identifier=image_arn,
            execution_role_arn=execution_role_arn,
            timeout=Duration.from_minutes(5),
            # A MicroVM that fails to boot fails the operation after this
            # time, instead of after the timeout.
            heartbeat_timeout=Duration.from_seconds(60),
        ),
    )
