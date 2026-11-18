"""Unit tests for pausing a step from the retry decision."""

from __future__ import annotations

import threading
from collections.abc import Callable
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

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
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    LambdaClient,
    Operation,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
    PausableDurableServiceClient,
    PauseExecutionRequest,
    StateOutput,
    StepDetails,
)
from aws_durable_execution_sdk_python.logger import Logger
from aws_durable_execution_sdk_python.operation.step import StepOperationExecutor
from aws_durable_execution_sdk_python.plugin import PluginExecutor
from aws_durable_execution_sdk_python.retries import (
    JitterStrategy,
    RetryDecision,
    RetryStrategyConfig,
    create_retry_strategy,
)
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


def _pause(_error: Exception, _attempt: int) -> RetryDecision:
    return RetryDecision.retry(Duration.from_seconds(5), pause=True)


def _pause_on(*attempts: int) -> Callable[[Exception, int], RetryDecision]:
    """A custom strategy that pauses only on the given attempts."""

    def strategy(_error: Exception, attempt: int) -> RetryDecision:
        return RetryDecision.retry(Duration.from_seconds(5), pause=attempt in attempts)

    return strategy


def _pause_every(n: int) -> Callable[[Exception, int], RetryDecision]:
    """The built-in strategy with pause_every and no jitter."""
    return create_retry_strategy(
        RetryStrategyConfig(
            max_attempts=10,
            initial_delay=Duration.from_seconds(5),
            backoff_rate=1,
            jitter_strategy=JitterStrategy.NONE,
            pause_every=n,
        )
    )


def _request(
    identifier: OperationIdentifier, attempt: int, arn: str = "test_arn"
) -> PauseExecutionRequest:
    return PauseExecutionRequest.create_for_step_attempt(
        identifier, attempt, durable_execution_arn=arn
    )


def _stop(_error: Exception, _attempt: int) -> RetryDecision:
    return RetryDecision.no_retry()


def _actions(state: Mock) -> list[OperationAction]:
    return [
        c.kwargs["operation_update"].action
        for c in state.create_checkpoint.call_args_list
    ]


def test_checkpoints_retry_then_pauses_when_decision_pauses():
    """A strategy that pauses on attempt 2 sends RETRY, pauses, and suspends untimed."""
    state = _state(_checkpoint(OperationStatus.READY, previous_attempts=1))
    order = Mock()
    order.attach_mock(state.pause_execution, "pause")
    order.attach_mock(state.create_checkpoint, "checkpoint")

    with pytest.raises(SuspendExecution) as exc_info:
        _run(state, StepConfig(retry_strategy=_pause_on(2)))

    assert not isinstance(exc_info.value, TimedSuspendExecution)
    state.pause_execution.assert_called_once_with(_request(_IDENTIFIER, 2))
    assert _actions(state) == [OperationAction.START, OperationAction.RETRY]
    # The pause request follows the RETRY checkpoint.
    names = [c[0] for c in order.mock_calls]
    assert names == ["checkpoint", "checkpoint", "pause"]
    retry_update = state.create_checkpoint.call_args_list[1].kwargs["operation_update"]
    assert retry_update.error.type == "RuntimeError"
    assert retry_update.step_options.next_attempt_delay_seconds == 5


@pytest.mark.parametrize("previous_attempts", [3, 5])
def test_helper_pause_every_pauses_on_later_multiples(previous_attempts: int):
    """With the helper's pause_every=2, attempts 4 and 6 pause too."""
    state = _state(_checkpoint(OperationStatus.READY, previous_attempts))

    with pytest.raises(SuspendExecution) as exc_info:
        _run(state, StepConfig(retry_strategy=_pause_every(2)))

    assert not isinstance(exc_info.value, TimedSuspendExecution)
    state.pause_execution.assert_called_once_with(
        _request(_IDENTIFIER, previous_attempts + 1)
    )


@pytest.mark.parametrize("previous_attempts", [0, 2, 4])
def test_helper_pause_every_does_not_pause_other_attempts(previous_attempts: int):
    """With the helper's pause_every=2, attempts 1, 3 and 5 retry as usual."""
    state = _state(_checkpoint(OperationStatus.READY, previous_attempts))

    with pytest.raises(TimedSuspendExecution):
        _run(state, StepConfig(retry_strategy=_pause_every(2)))

    state.pause_execution.assert_not_called()
    assert _actions(state)[-1] is OperationAction.RETRY


def test_no_pause_when_decision_has_no_pause():
    """A decision without pause (the default) retries as usual."""
    state = _state(_checkpoint(OperationStatus.READY, previous_attempts=0))

    with pytest.raises(TimedSuspendExecution):
        _run(state, StepConfig(retry_strategy=_retry))

    state.pause_execution.assert_not_called()
    assert _actions(state) == [OperationAction.START, OperationAction.RETRY]


def test_no_pause_when_decision_pause_is_false():
    """A strategy that returns pause=False on this attempt retries as usual."""
    state = _state(_checkpoint(OperationStatus.READY, previous_attempts=0))

    with pytest.raises(TimedSuspendExecution):
        _run(state, StepConfig(retry_strategy=_pause_on(2)))

    state.pause_execution.assert_not_called()
    assert _actions(state) == [OperationAction.START, OperationAction.RETRY]


def test_strategy_without_pause_still_retries():
    """An existing strategy that builds RetryDecision(should_retry, delay) works unchanged."""

    def existing(_error: Exception, _attempt: int) -> RetryDecision:
        return RetryDecision(True, Duration.from_seconds(7))  # noqa: FBT003

    state = _state(_checkpoint(OperationStatus.READY, previous_attempts=0))

    with pytest.raises(TimedSuspendExecution):
        _run(state, StepConfig(retry_strategy=existing))

    state.pause_execution.assert_not_called()
    retry_update = state.create_checkpoint.call_args_list[1].kwargs["operation_update"]
    assert retry_update.step_options.next_attempt_delay_seconds == 7


def test_no_pause_when_retry_strategy_stops():
    """The step fails as usual when the retry strategy stops."""
    state = _state(_checkpoint(OperationStatus.READY, previous_attempts=0))

    with pytest.raises(StepError):
        _run(state, StepConfig(retry_strategy=_stop))

    state.pause_execution.assert_not_called()
    assert _actions(state) == [OperationAction.START, OperationAction.FAIL]


def test_pause_is_ignored_when_should_retry_is_false():
    """pause=True with should_retry=False fails the step and does not pause."""

    def stop_and_pause(_error: Exception, _attempt: int) -> RetryDecision:
        return RetryDecision(should_retry=False, delay=Duration(), pause=True)

    state = _state(_checkpoint(OperationStatus.READY, previous_attempts=0))

    with pytest.raises(StepError):
        _run(state, StepConfig(retry_strategy=stop_and_pause))

    state.pause_execution.assert_not_called()
    assert _actions(state) == [OperationAction.START, OperationAction.FAIL]


def test_pause_failure_logs_warning_and_retries_as_usual():
    """A failed pause call logs a warning and falls back to a timed retry."""
    state = _state(_checkpoint(OperationStatus.READY, previous_attempts=0))
    state.pause_execution.side_effect = RuntimeError("AccessDeniedException")
    logger = _logger()

    with pytest.raises(TimedSuspendExecution):
        _run(state, StepConfig(retry_strategy=_pause), logger)

    logger.warning.assert_called_once()
    warning_args = logger.warning.call_args.args
    message = warning_args[0] % warning_args[1:]
    assert warning_args[1:3] == (1, "charge")
    assert "Error: RuntimeError: AccessDeniedException." in message
    assert "missing lambda:PauseDurableExecution permission" in message
    assert "one possible cause" in message
    assert _actions(state) == [OperationAction.START, OperationAction.RETRY]


def test_pause_client_error_warning_names_the_service_error_code():
    """A botocore ClientError is named by its service error code."""
    state = _state(_checkpoint(OperationStatus.READY, previous_attempts=0))
    state.pause_execution.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "not allowed"}},
        "PauseDurableExecution",
    )
    logger = _logger()

    with pytest.raises(TimedSuspendExecution):
        _run(state, StepConfig(retry_strategy=_pause), logger)

    warning_args = logger.warning.call_args.args
    message = warning_args[0] % warning_args[1:]
    assert "Error: AccessDeniedException: " in message
    assert "not allowed" in message
    assert "one possible cause" in message


def test_pause_conflict_warning_does_not_blame_permissions():
    """A ResourceConflictException says the execution is closed or paused."""
    state = _state(_checkpoint(OperationStatus.READY, previous_attempts=0))
    state.pause_execution.side_effect = ClientError(
        {"Error": {"Code": "ResourceConflictException", "Message": "paused"}},
        "PauseDurableExecution",
    )
    logger = _logger()

    with pytest.raises(TimedSuspendExecution):
        _run(state, StepConfig(retry_strategy=_pause), logger)

    warning_args = logger.warning.call_args.args
    message = warning_args[0] % warning_args[1:]
    assert "Error: ResourceConflictException: " in message
    assert "has closed (for example, stopped) or is already paused" in message
    assert "permission" not in message


def test_pause_client_error_without_code_uses_type_name():
    """A ClientError without an error code falls back to the type name."""
    state = _state(_checkpoint(OperationStatus.READY, previous_attempts=0))
    state.pause_execution.side_effect = ClientError({}, "PauseDurableExecution")
    logger = _logger()

    with pytest.raises(TimedSuspendExecution):
        _run(state, StepConfig(retry_strategy=_pause), logger)

    warning_args = logger.warning.call_args.args
    assert "Error: ClientError: " in warning_args[0] % warning_args[1:]
    assert _actions(state) == [OperationAction.START, OperationAction.RETRY]


def test_client_without_pause_support_retries_as_usual():
    """When the client cannot pause, the step retries as usual."""
    state = _state(_checkpoint(OperationStatus.READY, previous_attempts=0))
    state.pause_execution.return_value = False

    with pytest.raises(TimedSuspendExecution):
        _run(state, StepConfig(retry_strategy=_pause))

    state.pause_execution.assert_called_once()
    assert _actions(state) == [OperationAction.START, OperationAction.RETRY]


def test_no_pause_on_interrupted_at_most_once_replay():
    """An interrupted AT_MOST_ONCE attempt ignores pause=True and retries."""
    state = _state(_checkpoint(OperationStatus.STARTED, previous_attempts=0))
    strategy = Mock(side_effect=_pause)

    with pytest.raises(TimedSuspendExecution):
        _run(
            state,
            StepConfig(
                retry_strategy=strategy,
                step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
            ),
        )

    # The replay path asks the strategy, and the strategy returns pause=True.
    strategy.assert_called_once()
    state.pause_execution.assert_not_called()
    assert _actions(state) == [OperationAction.RETRY]


def test_revoked_token_on_retry_checkpoint_suspends():
    """A tokenless answer to the RETRY checkpoint suspends the invocation."""
    state = _state(_checkpoint(OperationStatus.READY, previous_attempts=0))
    revoked = SuspendExecution("Checkpoint token revoked")
    state.create_checkpoint.side_effect = [None, revoked]

    with pytest.raises(SuspendExecution) as exc_info:
        _run(state, StepConfig(retry_strategy=_pause))

    assert exc_info.value is revoked
    # The execution is already pausing, so the step does not pause it again.
    state.pause_execution.assert_not_called()


def test_pause_waits_until_retry_checkpoint_is_accepted():
    """The pause request starts only after the blocking RETRY checkpoint returns."""
    state = _state(_checkpoint(OperationStatus.READY, previous_attempts=0))
    accepted: list[OperationAction] = []

    def checkpoint(operation_update: object, **_kwargs: object) -> None:
        accepted.append(operation_update.action)  # type: ignore[attr-defined]

    def pause(_request: PauseExecutionRequest) -> bool:
        assert accepted == [OperationAction.START, OperationAction.RETRY]
        return True

    state.create_checkpoint.side_effect = checkpoint
    state.pause_execution.side_effect = pause

    with pytest.raises(SuspendExecution) as exc_info:
        _run(state, StepConfig(retry_strategy=_pause))

    assert not isinstance(exc_info.value, TimedSuspendExecution)
    state.pause_execution.assert_called_once()


class _RecordingClient:
    """Checkpoint client that records each request and when pause is called."""

    def __init__(self) -> None:
        self.events: list[str] = []

    def checkpoint(
        self,
        durable_execution_arn: str,  # noqa: ARG002
        checkpoint_token: str,  # noqa: ARG002
        updates: list[object],
        client_token: str | None,  # noqa: ARG002
    ) -> CheckpointOutput:
        actions = ",".join(u.action.value for u in updates)  # type: ignore[attr-defined]
        self.events.append(f"checkpoint[{actions}]")
        return CheckpointOutput(
            checkpoint_token="next",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(operations=[]),
        )

    def get_execution_state(self, *args: object, **kwargs: object) -> StateOutput:
        return StateOutput(operations=[], next_marker=None)

    def pause_durable_execution(
        self,
        durable_execution_arn: str,  # noqa: ARG002
        request: PauseExecutionRequest,  # noqa: ARG002
    ) -> None:
        self.events.append("pause")


def test_real_state_sends_start_and_retry_before_pause():
    """With the real batcher, the async START and the RETRY are sent before the pause."""
    client = _RecordingClient()
    state = _execution_state(client)
    thread = threading.Thread(target=state.checkpoint_batches_forever, daemon=True)
    thread.start()

    def failing_step(_ctx: object) -> str:
        msg = "boom"
        raise RuntimeError(msg)

    try:
        with pytest.raises(SuspendExecution) as exc_info:
            StepOperationExecutor(
                func=failing_step,
                config=StepConfig(retry_strategy=_pause),
                state=state,
                operation_identifier=_IDENTIFIER,
                context_logger=_logger(),
            ).process()
    finally:
        state.stop_checkpointing()
        thread.join(2)

    assert not isinstance(exc_info.value, TimedSuspendExecution)
    assert client.events[-1] == "pause"
    sent = "".join(client.events[:-1])
    assert sent.index("START") < sent.index("RETRY")


def test_pause_request_client_token_is_stable_per_attempt():
    """The client token depends on the execution ARN, operation id and attempt."""
    first = _request(_IDENTIFIER, 5)
    again = _request(_IDENTIFIER, 5)
    renamed = _request(
        OperationIdentifier(
            _OPERATION_ID,
            OperationSubType.STEP,
            None,
            "other",
            operation_type=OperationType.STEP,
        ),
        5,
    )
    next_attempt = _request(_IDENTIFIER, 10)

    assert first == again
    assert first.client_token == renamed.client_token
    assert first.client_token != next_attempt.client_token
    assert len(first.client_token) == 64
    assert first.client_token.isascii()
    assert first.client_token.isalnum()
    assert first.reason == 'Step "charge" failed attempt 5'


def test_pause_request_client_token_differs_per_execution():
    """The same step attempt in two executions gets two different tokens."""
    first = _request(_IDENTIFIER, 5, arn="arn:aws:lambda:us-west-2:1:function:f/a")
    second = _request(_IDENTIFIER, 5, arn="arn:aws:lambda:us-west-2:1:function:f/b")

    assert first.client_token != second.client_token
    assert len(second.client_token) == 64
    assert first == _request(
        _IDENTIFIER, 5, arn="arn:aws:lambda:us-west-2:1:function:f/a"
    )


def test_step_pause_request_uses_the_execution_arn():
    """The step builds the pause request from the state's execution ARN."""
    state = _state(_checkpoint(OperationStatus.READY, previous_attempts=0))
    state.durable_execution_arn = "other_arn"

    with pytest.raises(SuspendExecution):
        _run(state, StepConfig(retry_strategy=_pause))

    state.pause_execution.assert_called_once_with(
        _request(_IDENTIFIER, 1, arn="other_arn")
    )
    assert state.pause_execution.call_args.args[0] != _request(_IDENTIFIER, 1)


def test_pause_request_reason_uses_operation_id_without_name():
    identifier = OperationIdentifier(
        _OPERATION_ID,
        OperationSubType.STEP,
        operation_type=OperationType.STEP,
    )

    request = _request(identifier, 3)

    assert request.reason == f'Step "{_OPERATION_ID}" failed attempt 3'


def test_pause_request_reason_fits_api_limit():
    identifier = OperationIdentifier(
        _OPERATION_ID,
        OperationSubType.STEP,
        None,
        "n" * 2000,
        operation_type=OperationType.STEP,
    )

    request = _request(identifier, 1)

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
