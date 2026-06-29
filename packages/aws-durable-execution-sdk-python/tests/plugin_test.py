import datetime
import logging
import unittest
from unittest.mock import MagicMock

from aws_durable_execution_sdk_python.lambda_service import (
    DurableExecutionInvocationOutput,
    ErrorObject,
    InvocationStatus,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
)
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
    OperationEndInfo,
    OperationStartInfo,
    PluginExecutor,
    UserFunctionEndInfo,
    UserFunctionOutcome,
    UserFunctionStartInfo,
)


# region Dataclass Tests

ERROR = ErrorObject(message="boom", type="Error", data=None, stack_trace=None)
START_TS = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
END_TS = datetime.datetime(2025, 1, 2, tzinfo=datetime.UTC)
LAMBDA_CTX = MagicMock()
LAMBDA_CTX.aws_request_id = "req-1"

OPERATION_START_INFO = OperationStartInfo(
    operation_id="op-2",
    operation_type=OperationType.CALLBACK,
    sub_type=OperationSubType.CALLBACK,
    name="my-op",
    parent_id="parent-1",
    start_time=START_TS,
)
OPERATION_END_INFO = OperationEndInfo(
    operation_id="op-1",
    operation_type=OperationType.STEP,
    sub_type=OperationSubType.STEP,
    name="my-op",
    parent_id="parent-1",
    start_time=START_TS,
    status=OperationStatus.FAILED,
    end_time=END_TS,
    error=ERROR,
)

INVOCATION_START_INFO = InvocationStartInfo(
    request_id="req-1",
    execution_arn="arn:aws:lambda:us-east-1:123:durable:abc",
    start_time=START_TS,
    is_first_invocation=True,
)
INVOCATION_END_INFO = InvocationEndInfo(
    request_id="req-1",
    execution_arn="arn:test",
    start_time=START_TS,
    status=InvocationStatus.FAILED,
    error=ERROR,
    is_first_invocation=False,
    end_time=END_TS,
)

USER_FUNCTION_START_INFO = UserFunctionStartInfo(
    operation_id="op-1",
    operation_type=OperationType.STEP,
    sub_type=OperationSubType.STEP,
    name="func",
    parent_id="parent-1",
    start_time=START_TS,
)

USER_FUNCTION_END_INFO = UserFunctionEndInfo(
    operation_id="op-1",
    operation_type=OperationType.STEP,
    sub_type=OperationSubType.STEP,
    name="func",
    parent_id="parent-1",
    start_time=START_TS,
    is_replay_children=False,
    attempt=1,
    outcome=UserFunctionOutcome.FAILED,
    end_time=END_TS,
    error=ERROR,
)


class TestDataClasses(unittest.TestCase):
    def test_operation_start_info(self):
        self.assertEqual(OPERATION_START_INFO.sub_type, OperationSubType.CALLBACK)
        self.assertEqual(OPERATION_START_INFO.name, "my-op")
        self.assertEqual(OPERATION_START_INFO.parent_id, "parent-1")
        self.assertEqual(OPERATION_START_INFO.start_time, START_TS)

    def test_operation_end_info(self):
        self.assertEqual(OPERATION_END_INFO.status, OperationStatus.FAILED)
        self.assertEqual(OPERATION_END_INFO.end_time, END_TS)
        self.assertEqual(OPERATION_END_INFO.error, ERROR)
        self.assertEqual(OPERATION_END_INFO.operation_type, OperationType.STEP)
        self.assertEqual(OPERATION_END_INFO.sub_type, OperationSubType.STEP)
        self.assertEqual(OPERATION_END_INFO.name, "my-op")
        self.assertEqual(OPERATION_END_INFO.parent_id, "parent-1")
        self.assertEqual(OPERATION_END_INFO.operation_id, "op-1")
        self.assertEqual(OPERATION_END_INFO.status, OperationStatus.FAILED)
        self.assertEqual(OPERATION_END_INFO.operation_id, "op-1")

    def test_invocation_start_info(self):
        self.assertEqual(INVOCATION_START_INFO.request_id, "req-1")
        self.assertEqual(
            INVOCATION_START_INFO.execution_arn,
            "arn:aws:lambda:us-east-1:123:durable:abc",
        )
        self.assertEqual(INVOCATION_START_INFO.start_time, START_TS)
        self.assertTrue(INVOCATION_START_INFO.is_first_invocation)

    def test_invocation_end_info(self):
        self.assertEqual(INVOCATION_END_INFO.request_id, "req-1")
        self.assertEqual(INVOCATION_END_INFO.execution_arn, "arn:test")
        self.assertEqual(INVOCATION_END_INFO.start_time, START_TS)
        self.assertFalse(INVOCATION_END_INFO.is_first_invocation)
        self.assertEqual(INVOCATION_END_INFO.status, InvocationStatus.FAILED)
        self.assertEqual(INVOCATION_END_INFO.error.message, "boom")
        self.assertEqual(INVOCATION_END_INFO.end_time, END_TS)

    def test_user_function_start_info(self):
        self.assertEqual(USER_FUNCTION_START_INFO.operation_id, "op-1")
        self.assertEqual(USER_FUNCTION_START_INFO.operation_type, OperationType.STEP)
        self.assertEqual(USER_FUNCTION_START_INFO.sub_type, OperationSubType.STEP)
        self.assertEqual(USER_FUNCTION_START_INFO.name, "func")
        self.assertEqual(USER_FUNCTION_START_INFO.parent_id, "parent-1")
        self.assertEqual(USER_FUNCTION_START_INFO.start_time, START_TS)

    def test_user_function_end_info(self):
        self.assertEqual(USER_FUNCTION_END_INFO.operation_id, "op-1")
        self.assertEqual(USER_FUNCTION_END_INFO.operation_type, OperationType.STEP)
        self.assertEqual(USER_FUNCTION_END_INFO.sub_type, OperationSubType.STEP)
        self.assertEqual(USER_FUNCTION_END_INFO.name, "func")
        self.assertEqual(USER_FUNCTION_END_INFO.parent_id, "parent-1")
        self.assertEqual(USER_FUNCTION_END_INFO.start_time, START_TS)
        self.assertFalse(USER_FUNCTION_END_INFO.is_replay_children)
        self.assertEqual(USER_FUNCTION_END_INFO.attempt, 1)
        self.assertEqual(USER_FUNCTION_END_INFO.outcome, UserFunctionOutcome.FAILED)
        self.assertEqual(USER_FUNCTION_END_INFO.end_time, END_TS)
        self.assertEqual(USER_FUNCTION_END_INFO.error.message, "boom")


# endregion Dataclass Tests


# region DurableInstrumentationPlugin Tests
class TestDurableInstrumentationPlugin(unittest.TestCase):
    def test_default_methods_are_noop(self):
        """All default hook methods should be callable and return None."""
        plugin = _NoOpPlugin()
        self.assertIsNone(plugin.on_invocation_start(INVOCATION_START_INFO))
        self.assertIsNone(plugin.on_invocation_end(INVOCATION_END_INFO))
        self.assertIsNone(plugin.on_operation_start(OPERATION_START_INFO))
        self.assertIsNone(plugin.on_operation_end(OPERATION_END_INFO))
        self.assertIsNone(plugin.on_user_function_start(USER_FUNCTION_START_INFO))
        self.assertIsNone(plugin.on_user_function_end(USER_FUNCTION_END_INFO))

    def test_subclass_override(self):
        """A subclass can override specific hooks."""
        plugin = _TrackingPlugin()

        plugin.on_invocation_start(INVOCATION_START_INFO)
        plugin.on_operation_start(OPERATION_START_INFO)

        self.assertEqual(
            ["invocation_start:req-1", "operation_start:op-2"], plugin.calls
        )


# endregion DurableInstrumentationPlugin Tests


# region PluginExecutor Tests


class TestPluginExecutorInit(unittest.TestCase):
    def test_init_with_none(self):
        executor = PluginExecutor(plugins=None)
        self.assertEqual(executor._plugins, [])

    def test_init_with_empty_list(self):
        executor = PluginExecutor(plugins=[])
        self.assertEqual(executor._plugins, [])

    def test_init_with_plugins(self):
        p1 = _NoOpPlugin()
        p2 = _TrackingPlugin()
        executor = PluginExecutor(plugins=[p1, p2])
        self.assertEqual(len(executor._plugins), 2)


class TestPluginExecutor(unittest.TestCase):
    def test_no_thread_pool_when_plugins_is_none(self):
        """Tests that PluginExecutor does not create a thread pool when plugins is empty."""
        executor = PluginExecutor(plugins=None)
        self.assertIsNone(executor._executor)

    def test_no_thread_pool_when_plugins_is_empty_list(self):
        executor = PluginExecutor(plugins=[])
        self.assertIsNone(executor._executor)

    def test_thread_pool_created_when_plugins_provided(self):
        executor = PluginExecutor(plugins=[_NoOpPlugin()])
        with executor.run():
            self.assertIsNotNone(executor._executor)

    def test_start_is_noop_when_empty(self):
        executor = PluginExecutor(plugins=[])
        # Should not raise
        with executor.run():
            pass

    def test_on_invocation_start_is_safe_when_empty(self):
        executor = PluginExecutor(plugins=[])
        # Should not raise
        executor.on_invocation_start(
            execution_arn="arn:exec",
            lambda_context=LAMBDA_CTX,
            execution_start_time=START_TS,
            is_first_invocation=False,
        )

    def test_on_invocation_end_is_safe_when_empty(self):
        executor = PluginExecutor(plugins=[])
        executor.on_invocation_start(
            execution_arn="arn:exec",
            lambda_context=LAMBDA_CTX,
            execution_start_time=START_TS,
            is_first_invocation=False,
        )
        output = DurableExecutionInvocationOutput(
            status=InvocationStatus.SUCCEEDED, result=None, error=None
        )

        # Should not raise
        executor.on_invocation_end(
            output=output,
        )

    def test_on_operation_action_is_safe_when_empty(self):
        executor = PluginExecutor(plugins=[])
        update = MagicMock()
        update.action = OperationAction.START
        update.operation_id = "op-1"
        update.operation_type = OperationType.STEP
        update.sub_type = OperationSubType.STEP
        update.name = "my-step"
        update.parent_id = None

        # Should not raise
        executor.on_operation_action(update)

    def test_on_operation_update_is_safe_when_empty(self):
        executor = PluginExecutor(plugins=[])
        op = MagicMock()
        op.operation_id = "op-1"
        op.operation_type = OperationType.STEP
        op.sub_type = OperationSubType.STEP
        op.name = "my-step"
        op.parent_id = None
        op.start_time = START_TS
        op.end_time = END_TS
        op.status = OperationStatus.SUCCEEDED
        op.step_details = MagicMock()
        op.step_details.attempt = 1
        op.step_details.error = None
        op.callback_details = None
        op.chained_invoke_details = None
        op.context_details = None

        # Should not raise
        executor.on_operation_update(op)


class TestPluginExecutorExecutePlugins(unittest.TestCase):
    """Tests for the execute_plugins dispatch method."""

    def setUp(self):
        self.plugin = _TrackingPlugin()
        self.executor = PluginExecutor(plugins=[self.plugin])

    def test_dispatch_invocation_start_info(self):
        with self.executor.run():
            self.executor.execute_plugins(INVOCATION_START_INFO, sync=True)
        self.assertIn("invocation_start:req-1", self.plugin.calls)

    def test_dispatch_invocation_end_info(self):
        with self.executor.run():
            self.executor.execute_plugins(INVOCATION_END_INFO, sync=True)
        self.assertIn("invocation_end:req-1", self.plugin.calls)

    def test_dispatch_operation_end_info(self):
        with self.executor.run():
            self.executor.execute_plugins(OPERATION_END_INFO, sync=False)
        self.assertIn("operation_end:op-1", self.plugin.calls)

    def test_dispatch_operation_start_info(self):
        with self.executor.run():
            self.executor.execute_plugins(OPERATION_START_INFO, sync=False)
        self.assertIn("operation_start:op-2", self.plugin.calls)

    def test_dispatch_user_function_start_info(self):
        with self.executor.run():
            self.executor.execute_plugins(USER_FUNCTION_START_INFO, sync=True)
        self.assertIn("user_function_start:op-1", self.plugin.calls)

    def test_dispatch_user_function_end_info(self):
        with self.executor.run():
            self.executor.execute_plugins(USER_FUNCTION_END_INFO, sync=True)
        self.assertIn("user_function_end:op-1", self.plugin.calls)

    def test_dispatch_unknown_type_logs_exception(self):
        """Unknown info types should be caught and logged."""
        with self.assertLogs(
            "aws_durable_execution_sdk_python.plugin", level=logging.ERROR
        ):
            with self.executor.run():
                self.executor.execute_plugins("not a valid info type", sync=True)

    def test_plugin_exception_is_swallowed(self):
        """If a plugin raises, the exception is logged and execution continues."""
        failing_plugin = _FailingPlugin()
        tracking_plugin = _TrackingPlugin()
        executor = PluginExecutor(plugins=[failing_plugin, tracking_plugin])

        with self.assertLogs(
            "aws_durable_execution_sdk_python.plugin", level=logging.ERROR
        ):
            with executor.run():
                executor.execute_plugins(OPERATION_START_INFO, sync=True)

        # The second plugin should still have been called
        self.assertIn("operation_start:op-2", tracking_plugin.calls)

    def test_multiple_plugins_all_called(self):
        p1 = _TrackingPlugin()
        p2 = _TrackingPlugin()
        executor = PluginExecutor(plugins=[p1, p2])

        with executor.run():
            executor.execute_plugins(OPERATION_START_INFO, sync=True)

        self.assertIn("operation_start:op-2", p1.calls)
        self.assertIn("operation_start:op-2", p2.calls)


class TestPluginExecutorOnInvocationStart(unittest.TestCase):
    """Tests for PluginExecutor.on_invocation_start."""

    def setUp(self):
        self.plugin = _TrackingPlugin()
        self.executor = PluginExecutor(plugins=[self.plugin])
        self.ts = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)

    def _make_operation(self, start_time=None):
        op = MagicMock()
        op.start_time = start_time or self.ts
        return op

    def test_first_invocation_fires_invocation_start(self):
        with self.executor.run():
            self.executor.on_invocation_start(
                execution_arn="arn:exec",
                lambda_context=LAMBDA_CTX,
                execution_start_time=START_TS,
                is_first_invocation=False,
            )

            self.assertEqual("arn:exec", self.executor._invocation_status.execution_arn)
            self.assertEqual(
                LAMBDA_CTX.aws_request_id, self.executor._invocation_status.request_id
            )
            self.assertEqual(START_TS, self.executor._invocation_status.start_time)
            self.assertFalse(self.executor._invocation_status.is_first_invocation)

        self.assertIsNone(self.executor._invocation_status)

        # ExecutionStartInfo dispatches to on_invocation_start in match
        # InvocationStartInfo dispatches to on_invocation_start in match
        # So we expect two invocation_start calls
        invocation_calls = [
            c for c in self.plugin.calls if c.startswith("invocation_start")
        ]
        self.assertEqual(1, len(invocation_calls))

    def test_replay_invocation_fires_invocation_start(self):
        with self.executor.run():
            self.executor.on_invocation_start(
                execution_arn="arn:exec",
                lambda_context=LAMBDA_CTX,
                execution_start_time=START_TS,
                is_first_invocation=True,
            )

        # Only InvocationStartInfo should be dispatched (not ExecutionStartInfo)
        invocation_calls = [
            c for c in self.plugin.calls if c.startswith("invocation_start")
        ]
        self.assertEqual(1, len(invocation_calls))

    def test_none_context_uses_none_request_id(self):
        with self.executor.run():
            self.executor.on_invocation_start(
                execution_arn="arn:exec",
                lambda_context=None,
                execution_start_time=START_TS,
                is_first_invocation=False,
            )

        invocation_calls = [
            c for c in self.plugin.calls if c.startswith("invocation_start")
        ]
        # Both ExecutionStartInfo and InvocationStartInfo dispatched
        self.assertEqual(len(invocation_calls), 1)
        # request_id should be None
        self.assertIn("invocation_start:None", self.plugin.calls)


class TestPluginExecutorOnInvocationEnd(unittest.TestCase):
    """Tests for PluginExecutor.on_invocation_end."""

    def setUp(self):
        self.plugin = _TrackingPlugin()
        self.executor = PluginExecutor(plugins=[self.plugin])
        self.ts = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)

    def _make_operation(self, start_ts=None, end_ts=None):
        op = MagicMock()
        op.start_time = start_ts or self.ts
        op.end_time = end_ts
        return op

    def test_succeeded_fires_invocation_end(self):
        output = DurableExecutionInvocationOutput(
            status=InvocationStatus.SUCCEEDED, result=None, error=None
        )

        with self.executor.run():
            self.executor.on_invocation_start(
                execution_arn="arn:exec",
                lambda_context=LAMBDA_CTX,
                execution_start_time=START_TS,
                is_first_invocation=False,
            )
            self.executor.on_invocation_end(
                output=output,
            )

        self.assertIn("invocation_end:req-1", self.plugin.calls)

    def test_failed_fires_invocation_end(self):
        output = DurableExecutionInvocationOutput(
            status=InvocationStatus.FAILED, result=None, error=ERROR
        )

        with self.executor.run():
            self.executor.on_invocation_start(
                execution_arn="arn:exec",
                lambda_context=LAMBDA_CTX,
                execution_start_time=START_TS,
                is_first_invocation=False,
            )
            self.executor.on_invocation_end(
                output=output,
            )

        self.assertIn("invocation_end:req-1", self.plugin.calls)

    def test_pending_fires_invocation_end(self):
        output = DurableExecutionInvocationOutput(
            status=InvocationStatus.PENDING, result=None, error=None
        )

        with self.executor.run():
            self.executor.on_invocation_start(
                execution_arn="arn:exec",
                lambda_context=LAMBDA_CTX,
                execution_start_time=START_TS,
                is_first_invocation=False,
            )
            self.executor.on_invocation_end(
                output=output,
            )

        self.assertIn("invocation_end:req-1", self.plugin.calls)


class TestPluginExecutorOnOperationAction(unittest.TestCase):
    """Tests for PluginExecutor.on_operation_action."""

    def setUp(self):
        self.plugin = _TrackingPlugin()
        self.executor = PluginExecutor(plugins=[self.plugin])

    def test_start_action_fires_operation_start(self):
        update = MagicMock()
        update.action = OperationAction.START
        update.operation_id = "op-1"
        update.operation_type = OperationType.STEP
        update.sub_type = OperationSubType.STEP
        update.name = "my-step"
        update.parent_id = "parent-1"

        with self.executor.run():
            self.executor.on_operation_action(update)

        self.assertIn("operation_start:op-1", self.plugin.calls)

    def test_non_start_action_does_not_fire(self):
        update = MagicMock()
        update.action = OperationAction.SUCCEED
        update.operation_id = "op-1"

        self.executor.on_operation_action(update)

        self.assertEqual(self.plugin.calls, [])

    def test_fail_action_does_not_fire(self):
        update = MagicMock()
        update.action = OperationAction.FAIL
        update.operation_id = "op-1"

        self.executor.on_operation_action(update)

        self.assertEqual(self.plugin.calls, [])


class TestPluginExecutorOnOperationUpdate(unittest.TestCase):
    """Tests for PluginExecutor.on_operation_update."""

    def setUp(self):
        self.plugin = _TrackingPlugin()
        self.executor = PluginExecutor(plugins=[self.plugin])

    def _make_operation(
        self,
        status=OperationStatus.SUCCEEDED,
        step_details=None,
        callback_details=None,
        chained_invoke_details=None,
        context_details=None,
    ):
        op = MagicMock()
        op.operation_id = "op-1"
        op.operation_type = OperationType.STEP
        op.sub_type = OperationSubType.STEP
        op.name = "my-step"
        op.parent_id = "parent-1"
        op.start_time = START_TS
        op.end_time = END_TS
        op.status = status
        op.step_details = step_details
        op.callback_details = callback_details
        op.chained_invoke_details = chained_invoke_details
        op.context_details = context_details
        return op

    def test_terminal_status_without_step_details_fires_operation_only(self):
        op = self._make_operation(status=OperationStatus.FAILED, step_details=None)

        with self.executor.run():
            self.executor.on_operation_update(op)

        self.assertIn("operation_end:op-1", self.plugin.calls)

    def test_non_terminal_status_without_step_details_fires_nothing(self):
        op = self._make_operation(status=OperationStatus.STARTED, step_details=None)

        with self.executor.run():
            self.executor.on_operation_update(op)

        self.assertEqual(self.plugin.calls, [])

    def test_ready_status_fires_nothing(self):
        op = self._make_operation(status=OperationStatus.READY, step_details=None)

        with self.executor.run():
            self.executor.on_operation_update(op)

        self.assertEqual(self.plugin.calls, [])

    def test_timed_out_is_terminal(self):
        op = self._make_operation(status=OperationStatus.TIMED_OUT, step_details=None)

        with self.executor.run():
            self.executor.on_operation_update(op)

        self.assertIn("operation_end:op-1", self.plugin.calls)

    def test_cancelled_is_terminal(self):
        op = self._make_operation(status=OperationStatus.CANCELLED, step_details=None)

        with self.executor.run():
            self.executor.on_operation_update(op)

        self.assertIn("operation_end:op-1", self.plugin.calls)

    def test_stopped_is_terminal(self):
        op = self._make_operation(status=OperationStatus.STOPPED, step_details=None)

        with self.executor.run():
            self.executor.on_operation_update(op)

        self.assertIn("operation_end:op-1", self.plugin.calls)


class TestPluginExecutorExtractError(unittest.TestCase):
    """Tests for PluginExecutor._extract_error static method."""

    def test_extract_error_from_step_details(self):
        op = MagicMock()
        op.step_details = MagicMock()
        op.step_details.error = ERROR
        op.callback_details = None
        op.chained_invoke_details = None
        op.context_details = None

        result = PluginExecutor._extract_error(op)
        self.assertEqual(result.message, "boom")

    def test_extract_error_from_callback_details(self):
        op = MagicMock()
        op.step_details = None
        op.callback_details = MagicMock()
        op.callback_details.error = ERROR
        op.chained_invoke_details = None
        op.context_details = None

        result = PluginExecutor._extract_error(op)
        self.assertEqual(result.message, "boom")

    def test_extract_error_from_chained_invoke_details(self):
        op = MagicMock()
        op.step_details = None
        op.callback_details = None
        op.chained_invoke_details = MagicMock()
        op.chained_invoke_details.error = ERROR
        op.context_details = None

        result = PluginExecutor._extract_error(op)
        self.assertEqual(result.message, "boom")

    def test_extract_error_from_context_details(self):
        op = MagicMock()
        op.step_details = None
        op.callback_details = None
        op.chained_invoke_details = None
        op.context_details = MagicMock()
        op.context_details.error = ERROR

        result = PluginExecutor._extract_error(op)
        self.assertEqual(result.message, "boom")

    def test_extract_error_returns_none_when_no_error(self):
        op = MagicMock()
        op.step_details = None
        op.callback_details = None
        op.chained_invoke_details = None
        op.context_details = None

        result = PluginExecutor._extract_error(op)
        self.assertIsNone(result)

    def test_extract_error_step_details_no_error(self):
        """step_details exists but has no error - falls through to callback."""
        op = MagicMock()
        op.step_details = MagicMock()
        op.step_details.error = None
        op.callback_details = MagicMock()
        op.callback_details.error = ERROR
        op.chained_invoke_details = None
        op.context_details = None

        result = PluginExecutor._extract_error(op)
        self.assertEqual(result.message, "boom")


class TestPluginExecutorIsTerminalStatus(unittest.TestCase):
    """Tests for PluginExecutor._is_terminal_status static method."""

    def test_succeeded_is_terminal(self):
        self.assertTrue(PluginExecutor._is_terminal_status(OperationStatus.SUCCEEDED))

    def test_failed_is_terminal(self):
        self.assertTrue(PluginExecutor._is_terminal_status(OperationStatus.FAILED))

    def test_timed_out_is_terminal(self):
        self.assertTrue(PluginExecutor._is_terminal_status(OperationStatus.TIMED_OUT))

    def test_cancelled_is_terminal(self):
        self.assertTrue(PluginExecutor._is_terminal_status(OperationStatus.CANCELLED))

    def test_stopped_is_terminal(self):
        self.assertTrue(PluginExecutor._is_terminal_status(OperationStatus.STOPPED))

    def test_started_is_not_terminal(self):
        self.assertFalse(PluginExecutor._is_terminal_status(OperationStatus.STARTED))

    def test_pending_is_not_terminal(self):
        self.assertFalse(PluginExecutor._is_terminal_status(OperationStatus.PENDING))

    def test_ready_is_not_terminal(self):
        self.assertFalse(PluginExecutor._is_terminal_status(OperationStatus.READY))


# endregion PluginExecutor Tests


# region Helper Classes


class _NoOpPlugin(DurableInstrumentationPlugin):
    """Concrete subclass that inherits all default no-op methods."""

    pass


class _TrackingPlugin(DurableInstrumentationPlugin):
    """Concrete subclass that tracks calls to all hooks."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self.calls.append(f"invocation_start:{info.request_id}")

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        self.calls.append(f"invocation_end:{info.request_id}")

    def on_operation_start(self, info: OperationStartInfo) -> None:
        self.calls.append(f"operation_start:{info.operation_id}")

    def on_operation_end(self, info: OperationEndInfo) -> None:
        self.calls.append(f"operation_end:{info.operation_id}")

    def on_user_function_start(self, info: UserFunctionStartInfo) -> None:
        self.calls.append(f"user_function_start:{info.operation_id}")

    def on_user_function_end(self, info: UserFunctionEndInfo) -> None:
        self.calls.append(f"user_function_end:{info.operation_id}")


class _FailingPlugin(DurableInstrumentationPlugin):
    """Plugin that raises on every hook call."""

    def on_execution_start(self, info):
        raise RuntimeError("boom")

    def on_execution_end(self, info):
        raise RuntimeError("boom")

    def on_invocation_start(self, info):
        raise RuntimeError("boom")

    def on_invocation_end(self, info):
        raise RuntimeError("boom")

    def on_operation_start(self, info):
        raise RuntimeError("boom")

    def on_operation_end(self, info):
        raise RuntimeError("boom")

    def on_operation_attempt_start(self, info):
        raise RuntimeError("boom")

    def on_operation_attempt_end(self, info):
        raise RuntimeError("boom")


# endregion Helper Classes


# region Suspend Outcome Tests
class TestUserFunctionOutcomeValues(unittest.TestCase):
    def test_outcome_values(self):
        self.assertEqual(
            {o.value for o in UserFunctionOutcome},
            {"SUCCEEDED", "FAILED"},
        )


class TestUserFunctionOutcomeFromError(unittest.TestCase):
    def test_none_error_is_succeeded(self):
        self.assertEqual(
            UserFunctionOutcome.from_error(None), UserFunctionOutcome.SUCCEEDED
        )

    def test_error_is_failed(self):
        self.assertEqual(
            UserFunctionOutcome.from_error(ERROR), UserFunctionOutcome.FAILED
        )


# endregion Suspend Outcome Tests


if __name__ == "__main__":
    unittest.main()
