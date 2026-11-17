"""Unit tests for the StepConfig.pause_every option."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from aws_durable_execution_sdk_python.config import (
    Duration,
    StepConfig,
    StepSemantics,
)
from aws_durable_execution_sdk_python.exceptions import (
    StepError,
    SuspendExecution,
    TimedSuspendExecution,
)
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import (
    LambdaClient,
    Operation,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
    PausableDurableServiceClient,
    PauseExecutionRequest,
    StepDetails,
)
from aws_durable_execution_sdk_python.logger import Logger
from aws_durable_execution_sdk_python.operation.step import StepOperationExecutor
from aws_durable_execution_sdk_python.plugin import PluginExecutor
from aws_durable_execution_sdk_python.retries import RetryDecision
from aws_durable_execution_sdk_python.state import CheckpointedResult, ExecutionState


_OPERATION_ID = "a" * 64
_IDENTIFIER = OperationIdentifier(
    _OPERATION_ID,
    OperationSubType.STEP,
    None,
    "charge",
    operation_type=OperationType.STEP,
)


def _checkpoint(status: OperationStatus, previous_attempts: int) -> CheckpointedResult:
    """A checkpoint for a step that has already failed previous_attempts times."""
    return CheckpointedResult.create_from_operation(
        Operation(
            operation_id=_OPERATION_ID,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name="charge",
            status=status,
            step_details=StepDetails(attempt=previous_attempts),
        )
    )


def _state(checkpoint: CheckpointedResult) -> Mock:
    state = Mock(spec=ExecutionState)
    state.durable_execution_arn = "test_arn"
    state.get_checkpoint_result.return_value = checkpoint
    state.wrap_user_function.side_effect = lambda func, *_args: func
    state.pause_execution.return_value = True
    return state


def _logger() -> Mock:
    logger = Mock(spec=Logger)
    logger.with_log_info.return_value = logger
    return logger


def _run(
    state: Mock,
    config: StepConfig,
    logger: Mock | None = None,
) -> None:
    def failing_step(_ctx: object) -> str:
        msg = "boom"
        raise RuntimeError(msg)

    StepOperationExecutor(
        func=failing_step,
        config=config,
        state=state,
        operation_identifier=_IDENTIFIER,
        context_logger=logger or _logger(),
    ).process()


def _retry(_error: Exception, _attempt: int) -> RetryDecision:
    return RetryDecision.retry(Duration.from_seconds(5))


def _stop(_error: Exception, _attempt: int) -> RetryDecision:
    return RetryDecision.no_retry()


def _actions(state: Mock) -> list[OperationAction]:
    return [
        c.kwargs["operation_update"].action
        for c in state.create_checkpoint.call_args_list
    ]


def test_pauses_on_attempt_multiple_then_checkpoints_retry_and_suspends():
    """Attempt 2 with pause_every=2 pauses, sends RETRY, and suspends untimed."""
    state = _state(_checkpoint(OperationStatus.READY, previous_attempts=1))
    order = Mock()
    order.attach_mock(state.pause_execution, "pause")
    order.attach_mock(state.create_checkpoint, "checkpoint")

    with pytest.raises(SuspendExecution) as exc_info:
        _run(state, StepConfig(retry_strategy=_retry, pause_every=2))

    assert not isinstance(exc_info.value, TimedSuspendExecution)
    state.pause_execution.assert_called_once_with(
        PauseExecutionRequest.create_for_step_attempt(_IDENTIFIER, 2)
    )
    assert _actions(state) == [OperationAction.START, OperationAction.RETRY]
    # The pause request precedes the RETRY checkpoint.
    names = [c[0] for c in order.mock_calls]
    assert names == ["checkpoint", "pause", "checkpoint"]
    retry_update = state.create_checkpoint.call_args_list[1].kwargs["operation_update"]
    assert retry_update.error.type == "RuntimeError"
    assert retry_update.step_options.next_attempt_delay_seconds == 5


@pytest.mark.parametrize("previous_attempts", [3, 5])
def test_pauses_on_later_multiples(previous_attempts: int):
    """Attempts 4 and 6 are multiples of 2, so they pause too."""
    state = _state(_checkpoint(OperationStatus.READY, previous_attempts))

    with pytest.raises(SuspendExecution) as exc_info:
        _run(state, StepConfig(retry_strategy=_retry, pause_every=2))

    assert not isinstance(exc_info.value, TimedSuspendExecution)
    state.pause_execution.assert_called_once_with(
        PauseExecutionRequest.create_for_step_attempt(
            _IDENTIFIER, previous_attempts + 1
        )
    )


@pytest.mark.parametrize("previous_attempts", [0, 2, 4])
def test_no_pause_on_other_attempts(previous_attempts: int):
    """Attempts 1, 3 and 5 are not multiples of 2, so they retry as usual."""
    state = _state(_checkpoint(OperationStatus.READY, previous_attempts))

    with pytest.raises(TimedSuspendExecution):
        _run(state, StepConfig(retry_strategy=_retry, pause_every=2))

    state.pause_execution.assert_not_called()
    assert _actions(state)[-1] is OperationAction.RETRY


def test_no_pause_by_default():
    """pause_every defaults to None and never pauses."""
    state = _state(_checkpoint(OperationStatus.READY, previous_attempts=0))

    with pytest.raises(TimedSuspendExecution):
        _run(state, StepConfig(retry_strategy=_retry))

    state.pause_execution.assert_not_called()


def test_no_pause_when_retry_strategy_stops():
    """The step fails as usual when the retry strategy stops."""
    state = _state(_checkpoint(OperationStatus.READY, previous_attempts=0))

    with pytest.raises(StepError):
        _run(state, StepConfig(retry_strategy=_stop, pause_every=1))

    state.pause_execution.assert_not_called()
    assert _actions(state) == [OperationAction.START, OperationAction.FAIL]


def test_pause_failure_logs_warning_and_retries_as_usual():
    """A failed pause call logs a warning and falls back to a timed retry."""
    state = _state(_checkpoint(OperationStatus.READY, previous_attempts=0))
    state.pause_execution.side_effect = RuntimeError("AccessDeniedException")
    logger = _logger()

    with pytest.raises(TimedSuspendExecution):
        _run(state, StepConfig(retry_strategy=_retry, pause_every=1), logger)

    logger.warning.assert_called_once()
    warning_args = logger.warning.call_args.args
    assert "lambda:PauseDurableExecution" in warning_args[0]
    assert warning_args[1:3] == (1, "charge")
    assert _actions(state) == [OperationAction.START, OperationAction.RETRY]


def test_client_without_pause_support_retries_as_usual():
    """When the client cannot pause, the step retries as usual."""
    state = _state(_checkpoint(OperationStatus.READY, previous_attempts=0))
    state.pause_execution.return_value = False

    with pytest.raises(TimedSuspendExecution):
        _run(state, StepConfig(retry_strategy=_retry, pause_every=1))

    state.pause_execution.assert_called_once()
    assert _actions(state) == [OperationAction.START, OperationAction.RETRY]


def test_no_pause_on_interrupted_at_most_once_replay():
    """An interrupted AT_MOST_ONCE attempt goes to retry without pausing."""
    state = _state(_checkpoint(OperationStatus.STARTED, previous_attempts=0))

    with pytest.raises(TimedSuspendExecution):
        _run(
            state,
            StepConfig(
                retry_strategy=_retry,
                step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
                pause_every=1,
            ),
        )

    state.pause_execution.assert_not_called()
    assert _actions(state) == [OperationAction.RETRY]


def test_revoked_token_on_retry_checkpoint_suspends():
    """A tokenless answer to the RETRY checkpoint suspends the invocation."""
    state = _state(_checkpoint(OperationStatus.READY, previous_attempts=0))
    revoked = SuspendExecution("Checkpoint token revoked")
    state.create_checkpoint.side_effect = [None, revoked]

    with pytest.raises(SuspendExecution) as exc_info:
        _run(state, StepConfig(retry_strategy=_retry, pause_every=1))

    assert exc_info.value is revoked
    state.pause_execution.assert_called_once()


def test_pause_request_client_token_is_stable_per_attempt():
    """The client token depends only on the operation id and the attempt."""
    first = PauseExecutionRequest.create_for_step_attempt(_IDENTIFIER, 5)
    again = PauseExecutionRequest.create_for_step_attempt(_IDENTIFIER, 5)
    renamed = PauseExecutionRequest.create_for_step_attempt(
        OperationIdentifier(
            _OPERATION_ID,
            OperationSubType.STEP,
            None,
            "other",
            operation_type=OperationType.STEP,
        ),
        5,
    )
    next_attempt = PauseExecutionRequest.create_for_step_attempt(_IDENTIFIER, 10)

    assert first == again
    assert first.client_token == renamed.client_token
    assert first.client_token != next_attempt.client_token
    assert len(first.client_token) == 64
    assert first.client_token.isascii()
    assert first.client_token.isalnum()
    assert first.reason == 'Step "charge" failed attempt 5'


def test_pause_request_reason_uses_operation_id_without_name():
    identifier = OperationIdentifier(
        _OPERATION_ID,
        OperationSubType.STEP,
        operation_type=OperationType.STEP,
    )

    request = PauseExecutionRequest.create_for_step_attempt(identifier, 3)

    assert request.reason == f'Step "{_OPERATION_ID}" failed attempt 3'


def test_pause_request_reason_fits_api_limit():
    identifier = OperationIdentifier(
        _OPERATION_ID,
        OperationSubType.STEP,
        None,
        "n" * 2000,
        operation_type=OperationType.STEP,
    )

    request = PauseExecutionRequest.create_for_step_attempt(identifier, 1)

    assert len(request.reason) == 1024


def _execution_state(client: object) -> ExecutionState:
    return ExecutionState(
        durable_execution_arn="test_arn",
        initial_checkpoint_token="token",  # noqa: S106
        operations={},
        service_client=client,  # type: ignore[arg-type]
        plugin_executor=PluginExecutor(plugins=None),
    )


def test_execution_state_pause_calls_client():
    client = Mock(spec=LambdaClient)
    request = PauseExecutionRequest(client_token="t", reason="r")  # noqa: S106

    assert _execution_state(client).pause_execution(request) is True

    client.pause_durable_execution.assert_called_once_with("test_arn", request)


def test_execution_state_pause_without_client_support():
    class CheckpointOnlyClient:
        def checkpoint(self, *args: object, **kwargs: object) -> None: ...

        def get_execution_state(self, *args: object, **kwargs: object) -> None: ...

    client = CheckpointOnlyClient()
    request = PauseExecutionRequest(client_token="t", reason="r")  # noqa: S106

    assert not isinstance(client, PausableDurableServiceClient)
    assert _execution_state(client).pause_execution(request) is False


def test_execution_state_pause_propagates_client_error():
    client = Mock(spec=LambdaClient)
    client.pause_durable_execution.side_effect = RuntimeError("denied")
    request = PauseExecutionRequest(client_token="t", reason="r")  # noqa: S106

    with pytest.raises(RuntimeError, match="denied"):
        _execution_state(client).pause_execution(request)


def test_lambda_client_pause_durable_execution():
    boto_client = Mock()
    request = PauseExecutionRequest(client_token="token", reason="why")  # noqa: S106

    LambdaClient(boto_client).pause_durable_execution("arn", request)

    boto_client.pause_durable_execution.assert_called_once_with(
        DurableExecutionArn="arn", ClientToken="token", Reason="why"
    )


def test_step_config_pause_every_is_keyword_only_and_defaults_to_none():
    assert StepConfig().pause_every is None
    assert StepConfig(pause_every=3).pause_every == 3
