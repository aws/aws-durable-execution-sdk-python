# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The request IDs of the AWS calls that the MicroVM operations make.

The core SDK checkpoints a step failure as its type and its message,
``str(error)``. It does not checkpoint ``__cause__``, or the ``response`` of a
botocore ``ClientError``. The handler then receives a ``StepError`` rebuilt
from that record, on the first run and on replay. The request ID of a failed
call is in ``error.response["ResponseMetadata"]["RequestId"]``. So only the
message can carry the request ID to the handler and to the execution history.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Literal

from aws_durable_execution_sdk_python.retries import RetryDecision
from botocore.exceptions import ClientError


MicrovmApiOperation = Literal[
    "RunMicrovm",
    "GetMicrovm",
    "ResumeMicrovm",
    "CreateMicrovmAuthToken",
    "TerminateMicrovm",
]
"""The Lambda MicroVMs API operations that the MicroVM operations call."""


def error_code(error: ClientError) -> str | None:
    """Return the error code of a botocore ``ClientError``."""
    return error.response.get("Error", {}).get("Code")


def is_microvms_service_error(operation: MicrovmApiOperation, error: object) -> bool:
    """Return whether ``error`` is the answer of the Lambda MicroVMs call
    ``operation``.

    1. boto3 resolves the credentials inside each call.
    2. A credential provider can call another service there, such as STS
       ``AssumeRole`` for an assumed role.
    3. When that call fails, the MicroVMs call raises the ``ClientError`` of
       the other service. That error carries the request ID of the other
       service.
    4. botocore sets ``operation_name`` to the name of the API operation that
       answered. An STS error names ``AssumeRole``, not ``RunMicrovm``.
    5. So only a ``ClientError`` whose ``operation_name`` is ``operation``
       gets the name of a Lambda MicroVMs operation next to its request ID.
    """
    return isinstance(error, ClientError) and error.operation_name == operation


def request_id_sentence(operation: MicrovmApiOperation, response: object) -> str:
    """Return ``<Operation> request ID: <id>.`` for an AWS call.

    ``response`` is the output of a boto3 call, or the ``response`` of a
    ``ClientError``. Both carry the request ID in
    ``ResponseMetadata.RequestId``. A custom client can return an output
    without it. So the function returns ``""`` when ``response`` has no
    request ID that is a non-empty string.
    """
    metadata = (
        response.get("ResponseMetadata") if isinstance(response, Mapping) else None
    )
    request_id = metadata.get("RequestId") if isinstance(metadata, Mapping) else None
    if isinstance(request_id, str) and request_id:
        return f"{operation} request ID: {request_id}."
    return ""


def with_request_id(
    message: str, operation: MicrovmApiOperation, response: object
) -> str:
    """Return ``message`` followed by the sentence of
    :func:`request_id_sentence`.

    The function returns ``message`` unchanged when ``response`` has no
    request ID. A service message often ends without a period, as in
    ``MicroVM not found: mvm-1``. So a message that does not end a sentence
    gets a period before the request ID sentence.
    """
    sentence = request_id_sentence(operation, response)
    if not sentence:
        return message
    if not message:
        return sentence
    end = "" if message.endswith((".", "!", "?")) else "."
    return f"{message}{end} {sentence}"


def add_request_id(operation: MicrovmApiOperation, error: ClientError) -> ClientError:
    """Return a copy of ``error`` whose message ends with the request ID of
    the failed call, or ``error`` itself.

    The step raises the result. The caller raises it without ``from``: the
    copy already has ``error`` as its ``__cause__``, and ``error`` must not
    become its own cause.

    The function makes a copy, and it does not change ``error``. The
    reasons, in order:

    1. The core SDK records ``str()`` of the raised error as the message,
       and the fully-qualified name of its class as the error type.
    2. So the raised error must have the request ID in ``str()``, and the
       class of ``error``. A wrapper of another class would change the
       recorded error type.
    3. The copy has the class of ``error``, the same ``response``, and the
       same ``operation_name``. Only ``args``, ``__cause__``, and
       ``_original_error`` differ. ``args`` gives ``str()``. So
       :func:`default_microvm_retry_strategy`, which reads the class, the
       error code, and the HTTP status, decides the same way.
    4. A fake or a custom client can raise one error object for several
       calls. ``error`` keeps its message, and each call builds its copy from
       that message. So the message names only the call that raised it.
    5. A custom strategy can match the message, for example with
       ``create_retry_strategy(RetryStrategyConfig(retryable_errors=[...]))``.
       The longer message could change its decision. So the copy holds
       ``error`` in its ``_original_error`` attribute, and
       :func:`decide_on_original_error` gives the strategy ``error`` instead
       of the copy. The core SDK builds the step record before it calls the
       strategy. So the record keeps the message with the request ID.

    The copy is made without ``__init__``. A subclass of ``ClientError`` can
    take other arguments, so ``__init__`` could fail. The copy takes
    ``__dict__`` of ``error``, which holds ``response``, ``operation_name``,
    and any notes. ``_original_error`` is set in the same ``__dict__``. When
    ``error`` is itself a copy, its ``_original_error`` is replaced with
    ``error``. So the attribute always names the error that this call
    received.

    ``raise ... from ...`` sets ``__cause__``, and code can set
    ``__cause__`` too. Neither changes ``_original_error``. So the strategy
    receives ``error`` even after ``__cause__`` changes. No module state
    holds an error. The copy holds ``error``. So ``error`` lives at least as
    long as the copy, and the garbage collector frees both when nothing
    else refers to them.

    The function returns ``error`` itself in these cases:

    - ``error`` is not the answer of ``operation``. See
      :func:`is_microvms_service_error`.
    - ``error`` has no request ID.
    - The class of ``error`` builds ``str()`` from something other than
      ``args``. The copy would not show the request ID.
    """
    if not is_microvms_service_error(operation, error):
        return error
    message = with_request_id(str(error), operation, error.response)
    if message == str(error):
        return error
    labelled = type(error).__new__(type(error))
    labelled.__dict__.update(vars(error))
    labelled.args = (message,)
    if str(labelled) != message:
        return error
    labelled.__cause__ = error
    labelled.__dict__["_original_error"] = error
    return labelled


RetryStrategy = Callable[[Exception, int], RetryDecision]


def decide_on_original_error(strategy: RetryStrategy) -> RetryStrategy:
    """Return a retry strategy that calls ``strategy`` with the error that
    botocore raised.

    The step raises the copy that :func:`add_request_id` made. The core SDK
    calls the step retry strategy with that copy. The copy holds the error
    that botocore raised in its ``_original_error`` attribute. The function
    passes on that error instead: the same object as before the request ID
    existed, with its message. So every strategy decides as it did before.

    Any other error passes unchanged. This includes a ``ClientError`` without
    ``_original_error``, and the errors that the MicroVM operation creates
    itself, such as ``MicrovmStateError``. Their messages have the request
    ID.

    The core SDK records the step failure before it calls the strategy. So
    the record, and the handler, keep the message with the request ID.

    Neither error changes its message at any time. So a traceback that code
    formats during the strategy call shows the same message as at any other
    time.
    """

    def decide(error: Exception, attempt: int) -> RetryDecision:
        original = (
            getattr(error, "_original_error", None)
            if isinstance(error, ClientError)
            else None
        )
        return strategy(
            original if isinstance(original, ClientError) else error, attempt
        )

    return decide
