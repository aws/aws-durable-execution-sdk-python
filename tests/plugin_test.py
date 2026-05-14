import datetime
import logging
import unittest
from unittest.mock import MagicMock, patch

from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    InvocationStatus,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
    DurableExecutionInvocationOutput,
)
from aws_durable_execution_sdk_python.plugin import (
    AttemptEndInfo,
    AttemptStartInfo,
    DurableExecutionPlugin,
    ExecutionEndInfo,
    ExecutionStartInfo,
    InvocationEndInfo,
    InvocationStartInfo,
    OperationEndInfo,
    OperationStartInfo,
    PluginExecutor,
)


# region Dataclass Tests


class TestOperationStartInfo(unittest.TestCase):
    def test_required_fields(self):
        info = OperationStartInfo(
            operation_id="op-1", operation_type=OperationType.STEP
        )
        self.assertEqual(info.operation_id, "op-1")
        self.assertEqual(info.operation_type, OperationType.STEP)
        self.assertIsNone(info.sub_type)
        self.assertIsNone(info.name)
        self.assertIsNone(info.parent_id)
        self.assertIsNone(info.start_timestamp)

    def test_all_fields(self):
        ts = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
        info = OperationStartInfo(
            operation_id="op-2",
            operation_type=OperationType.CALLBACK,
            sub_type=OperationSubType.CALLBACK,
            name="my-op",
            parent_id="parent-1",
            start_timestamp=ts,
        )
        self.assertEqual(info.sub_type, OperationSubType.CALLBACK)
        self.assertEqual(info.name, "my-op")
        self.assertEqual(info.parent_id, "parent-1")
        self.assertEqual(info.start_timestamp, ts)


class TestOperationEndInfo(unittest.TestCase):
    def test_inherits_operation_start_info(self):
        self.assertTrue(issubclass(OperationEndInfo, OperationStartInfo))

    def test_defaults(self):
        info = OperationEndInfo(operation_id="op-1", operation_type=OperationType.STEP)
        self.assertEqual(info.status, OperationStatus.SUCCEEDED)
        self.assertIsNone(info.end_timestamp)
        self.assertIsNone(info.attempt)
        self.assertIsNone(info.error)

    def test_with_error(self):
        err = ErrorObject(
            message="fail", type="RuntimeError", data=None, stack_trace=None
        )
        info = OperationEndInfo(
            operation_id="op-1",
            operation_type=OperationType.STEP,
            status=OperationStatus.FAILED,
            error=err,
            attempt=3,
        )
        self.assertEqual(info.status, OperationStatus.FAILED)
        self.assertEqual(info.attempt, 3)
        self.assertEqual(info.error.message, "fail")


class TestAttemptStartInfo(unittest.TestCase):
    def test_inherits_operation_start_info(self):
        self.assertTrue(issubclass(AttemptStartInfo, OperationStartInfo))

    def test_default_attempt(self):
        info = AttemptStartInfo(operation_id="op-1", operation_type=OperationType.STEP)
        self.assertEqual(info.attempt, 1)

    def test_custom_attempt(self):
        info = AttemptStartInfo(
            operation_id="op-1", operation_type=OperationType.STEP, attempt=5
        )
        self.assertEqual(info.attempt, 5)


class TestAttemptEndInfo(unittest.TestCase):
    def test_inherits_attempt_start_info(self):
        self.assertTrue(issubclass(AttemptEndInfo, AttemptStartInfo))

    def test_defaults(self):
        info = AttemptEndInfo(operation_id="op-1", operation_type=OperationType.STEP)
        self.assertIsNone(info.succeeded)
        self.assertIsNone(info.error)
        self.assertIsNone(info.next_attempt_delay_seconds)

    def test_retry_with_delay(self):
        err = ErrorObject(
            message="timeout", type="TimeoutError", data=None, stack_trace=None
        )
        info = AttemptEndInfo(
            operation_id="op-1",
            operation_type=OperationType.STEP,
            succeeded=False,
            error=err,
            next_attempt_delay_seconds=30,
        )
        self.assertFalse(info.succeeded)
        self.assertEqual(info.next_attempt_delay_seconds, 30)
        self.assertEqual(info.error.type, "TimeoutError")


class TestInvocationStartInfo(unittest.TestCase):
    def test_fields(self):
        ts = datetime.datetime(2025, 6, 1, tzinfo=datetime.UTC)
        info = InvocationStartInfo(
            request_id="req-1",
            execution_arn="arn:aws:lambda:us-east-1:123:durable:abc",
            start_timestamp=ts,
        )
        self.assertEqual(info.request_id, "req-1")
        self.assertEqual(info.execution_arn, "arn:aws:lambda:us-east-1:123:durable:abc")
        self.assertEqual(info.start_timestamp, ts)


class TestInvocationEndInfo(unittest.TestCase):
    def test_inherits_invocation_start_info(self):
        self.assertTrue(issubclass(InvocationEndInfo, InvocationStartInfo))

    def test_defaults(self):
        ts = datetime.datetime(2025, 6, 1, tzinfo=datetime.UTC)
        info = InvocationEndInfo(
            request_id="req-1", execution_arn="arn:test", start_timestamp=ts
        )
        self.assertEqual(info.status, InvocationStatus.SUCCEEDED)
        self.assertIsNone(info.error)

    def test_failed(self):
        ts = datetime.datetime(2025, 6, 1, tzinfo=datetime.UTC)
        err = ErrorObject(message="boom", type="Error", data=None, stack_trace=None)
        info = InvocationEndInfo(
            request_id="req-1",
            execution_arn="arn:test",
            start_timestamp=ts,
            status=InvocationStatus.FAILED,
            error=err,
        )
        self.assertEqual(info.status, InvocationStatus.FAILED)
        self.assertEqual(info.error.message, "boom")


class TestExecutionStartInfo(unittest.TestCase):
    def test_inherits_invocation_start_info(self):
        self.assertTrue(issubclass(ExecutionStartInfo, InvocationStartInfo))

    def test_construction(self):
        ts = datetime.datetime(2025, 6, 1, tzinfo=datetime.UTC)
        info = ExecutionStartInfo(
            request_id="req-1", execution_arn="arn:test", start_timestamp=ts
        )
        self.assertEqual(info.request_id, "req-1")


class TestExecutionEndInfo(unittest.TestCase):
    def test_inherits_execution_start_info(self):
        self.assertTrue(issubclass(ExecutionEndInfo, ExecutionStartInfo))

    def test_defaults(self):
        ts = datetime.datetime(2025, 6, 1, tzinfo=datetime.UTC)
        info = ExecutionEndInfo(
            request_id="req-1", execution_arn="arn:test", start_timestamp=ts
        )
        self.assertEqual(info.status, InvocationStatus.SUCCEEDED)
        self.assertIsNone(info.error)

    def test_with_error(self):
        ts = datetime.datetime(2025, 6, 1, tzinfo=datetime.UTC)
        err = ErrorObject(message="crash", type="Error", data=None, stack_trace=None)
        info = ExecutionEndInfo(
            request_id="req-1",
            execution_arn="arn:test",
            start_timestamp=ts,
            status=InvocationStatus.FAILED,
            end_timestamp=ts,
            error=err,
        )
        self.assertEqual(info.status, InvocationStatus.FAILED)
        self.assertEqual(info.end_timestamp, ts)
        self.assertEqual(info.error.message, "crash")


# endregion Dataclass Tests


# region DurableExecutionPlugin Tests


class TestDurableExecutionPlugin(unittest.TestCase):
    def test_default_methods_are_noop(self):
        """All default hook methods should be callable and return None."""
        plugin = _NoOpPlugin()
        ts = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)

        exec_start = ExecutionStartInfo(
            request_id="r", execution_arn="a", start_timestamp=ts
        )
        exec_end = ExecutionEndInfo(
            request_id="r", execution_arn="a", start_timestamp=ts
        )
        inv_start = InvocationStartInfo(
            request_id="r", execution_arn="a", start_timestamp=ts
        )
        inv_end = InvocationEndInfo(
            request_id="r", execution_arn="a", start_timestamp=ts
        )
        op_start = OperationStartInfo(
            operation_id="o", operation_type=OperationType.STEP
        )
        op_end = OperationEndInfo(operation_id="o", operation_type=OperationType.STEP)
        att_start = AttemptStartInfo(
            operation_id="o", operation_type=OperationType.STEP
        )
        att_end = AttemptEndInfo(operation_id="o", operation_type=OperationType.STEP)

        self.assertIsNone(plugin.on_execution_start(exec_start))
        self.assertIsNone(plugin.on_execution_end(exec_end))
        self.assertIsNone(plugin.on_invocation_start(inv_start))
        self.assertIsNone(plugin.on_invocation_end(inv_end))
        self.assertIsNone(plugin.on_operation_start(op_start))
        self.assertIsNone(plugin.on_operation_end(op_end))
        self.assertIsNone(plugin.on_operation_attempt_start(att_start))
        self.assertIsNone(plugin.on_operation_attempt_end(att_end))

    def test_subclass_override(self):
        """A subclass can override specific hooks."""
        plugin = _TrackingPlugin()
        ts = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)

        plugin.on_execution_start(
            ExecutionStartInfo(request_id="r", execution_arn="a", start_timestamp=ts)
        )
        plugin.on_operation_start(
            OperationStartInfo(operation_id="o", operation_type=OperationType.WAIT)
        )

        self.assertEqual(plugin.calls, ["execution_start:r", "operation_start:o"])


# endregion DurableExecutionPlugin Tests


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
        ctx = MagicMock()
        ctx.aws_request_id = "req-1"
        op = MagicMock()
        op.start_timestamp = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)

        # Should not raise
        executor.on_invocation_start(
            durable_execution_arn="arn:exec",
            context=ctx,
            execution_operation=op,
            is_replaying=False,
        )

    def test_on_invocation_end_is_safe_when_empty(self):
        executor = PluginExecutor(plugins=[])
        ctx = MagicMock()
        ctx.aws_request_id = "req-1"
        op = MagicMock()
        op.start_timestamp = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
        op.end_timestamp = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
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
        executor.on_operation_action(None, update)

    def test_on_operation_update_is_safe_when_empty(self):
        executor = PluginExecutor(plugins=[])
        op = MagicMock()
        op.operation_id = "op-1"
        op.operation_type = OperationType.STEP
        op.sub_type = OperationSubType.STEP
        op.name = "my-step"
        op.parent_id = None
        op.start_timestamp = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
        op.end_timestamp = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
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

    def test_dispatch_execution_start_info(self):
        ts = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
        info = ExecutionStartInfo(
            request_id="req-1", execution_arn="arn:test", start_timestamp=ts
        )
        with self.executor.run():
            self.executor.execute_plugins(info)
        self.assertIn("execution_start:req-1", self.plugin.calls)

    def test_dispatch_execution_end_info(self):
        ts = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
        info = ExecutionEndInfo(
            request_id="req-1", execution_arn="arn:test", start_timestamp=ts
        )
        with self.executor.run():
            self.executor.execute_plugins(info)
        self.assertIn("execution_end:req-1", self.plugin.calls)

    def test_dispatch_invocation_start_info(self):
        ts = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
        info = InvocationStartInfo(
            request_id="req-1", execution_arn="arn:test", start_timestamp=ts
        )
        with self.executor.run():
            self.executor.execute_plugins(info)
        self.assertIn("invocation_start:req-1", self.plugin.calls)

    def test_dispatch_invocation_end_info(self):
        ts = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
        info = InvocationEndInfo(
            request_id="req-1", execution_arn="arn:test", start_timestamp=ts
        )
        with self.executor.run():
            self.executor.execute_plugins(info)
        self.assertIn("invocation_end:req-1", self.plugin.calls)

    def test_dispatch_operation_end_info(self):
        info = OperationEndInfo(operation_id="op-1", operation_type=OperationType.STEP)
        with self.executor.run():
            self.executor.execute_plugins(info)
        self.assertIn("operation_end:op-1", self.plugin.calls)

    def test_dispatch_operation_start_info(self):
        info = OperationStartInfo(
            operation_id="op-1", operation_type=OperationType.STEP
        )
        with self.executor.run():
            self.executor.execute_plugins(info)
        self.assertIn("operation_start:op-1", self.plugin.calls)

    def test_dispatch_attempt_start_info(self):
        info = AttemptStartInfo(operation_id="op-1", operation_type=OperationType.STEP)
        with self.executor.run():
            self.executor.execute_plugins(info)
        self.assertIn("attempt_start:op-1", self.plugin.calls)

    def test_dispatch_attempt_end_info(self):
        info = AttemptEndInfo(operation_id="op-1", operation_type=OperationType.STEP)
        with self.executor.run():
            self.executor.execute_plugins(info)
        self.assertIn("attempt_end:op-1", self.plugin.calls)

    def test_dispatch_unknown_type_logs_exception(self):
        """Unknown info types should be caught and logged."""
        with self.assertLogs(
            "aws_durable_execution_sdk_python.plugin", level=logging.ERROR
        ):
            with self.executor.run():
                self.executor.execute_plugins("not a valid info type")

    def test_plugin_exception_is_swallowed(self):
        """If a plugin raises, the exception is logged and execution continues."""
        failing_plugin = _FailingPlugin()
        tracking_plugin = _TrackingPlugin()
        executor = PluginExecutor(plugins=[failing_plugin, tracking_plugin])

        info = OperationStartInfo(
            operation_id="op-1", operation_type=OperationType.STEP
        )
        with self.assertLogs(
            "aws_durable_execution_sdk_python.plugin", level=logging.ERROR
        ):
            with executor.run():
                executor.execute_plugins(info)

        # The second plugin should still have been called
        self.assertIn("operation_start:op-1", tracking_plugin.calls)

    def test_multiple_plugins_all_called(self):
        p1 = _TrackingPlugin()
        p2 = _TrackingPlugin()
        executor = PluginExecutor(plugins=[p1, p2])

        info = OperationStartInfo(
            operation_id="op-1", operation_type=OperationType.STEP
        )
        with executor.run():
            executor.execute_plugins(info)

        self.assertIn("operation_start:op-1", p1.calls)
        self.assertIn("operation_start:op-1", p2.calls)


class TestPluginExecutorOnInvocationStart(unittest.TestCase):
    """Tests for PluginExecutor.on_invocation_start."""

    def setUp(self):
        self.plugin = _TrackingPlugin()
        self.executor = PluginExecutor(plugins=[self.plugin])
        self.ts = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)

    def _make_context(self, request_id="req-123"):
        ctx = MagicMock()
        ctx.aws_request_id = request_id
        return ctx

    def _make_operation(self, start_timestamp=None):
        op = MagicMock()
        op.start_timestamp = start_timestamp or self.ts
        return op

    def test_first_invocation_fires_execution_start_and_invocation_start(self):
        ctx = self._make_context()
        op = self._make_operation()

        with self.executor.run():
            self.executor.on_invocation_start(
                durable_execution_arn="arn:exec",
                context=ctx,
                execution_operation=op,
                is_replaying=False,
            )

        self.assertEqual("arn:exec", self.executor._durable_execution_arn)
        self.assertEqual(ctx.aws_request_id, self.executor._aws_request_id)
        self.assertEqual(op, self.executor._execution_operation)

        # ExecutionStartInfo dispatches to on_invocation_start in match
        # InvocationStartInfo dispatches to on_invocation_start in match
        # So we expect two invocation_start calls
        invocation_calls = [
            c
            for c in self.plugin.calls
            if c.startswith("invocation_start") or c.startswith("execution_start")
        ]
        self.assertEqual(len(invocation_calls), 2)

    def test_replay_invocation_skips_execution_start(self):
        ctx = self._make_context()
        op = self._make_operation()

        with self.executor.run():
            self.executor.on_invocation_start(
                durable_execution_arn="arn:exec",
                context=ctx,
                execution_operation=op,
                is_replaying=True,
            )

        # Only InvocationStartInfo should be dispatched (not ExecutionStartInfo)
        invocation_calls = [
            c
            for c in self.plugin.calls
            if c.startswith("invocation_start") or c.startswith("execution_start")
        ]
        self.assertEqual(len(invocation_calls), 1)

    def test_none_context_uses_none_request_id(self):
        op = self._make_operation()

        with self.executor.run():
            self.executor.on_invocation_start(
                durable_execution_arn="arn:exec",
                context=None,
                execution_operation=op,
                is_replaying=False,
            )

        invocation_calls = [
            c
            for c in self.plugin.calls
            if c.startswith("invocation_start") or c.startswith("execution_start")
        ]
        # Both ExecutionStartInfo and InvocationStartInfo dispatched
        self.assertEqual(len(invocation_calls), 2)
        # request_id should be None
        self.assertIn("invocation_start:None", self.plugin.calls)


class TestPluginExecutorOnInvocationEnd(unittest.TestCase):
    """Tests for PluginExecutor.on_invocation_end."""

    def setUp(self):
        self.plugin = _TrackingPlugin()
        self.executor = PluginExecutor(plugins=[self.plugin])
        self.ts = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)

    def _make_context(self, request_id="req-123"):
        ctx = MagicMock()
        ctx.aws_request_id = request_id
        return ctx

    def _make_operation(self, start_ts=None, end_ts=None):
        op = MagicMock()
        op.start_timestamp = start_ts or self.ts
        op.end_timestamp = end_ts
        return op

    def test_succeeded_fires_invocation_end_and_execution_end(self):
        ctx = self._make_context()
        op = self._make_operation(end_ts=self.ts)
        output = DurableExecutionInvocationOutput(
            status=InvocationStatus.SUCCEEDED, result=None, error=None
        )

        with self.executor.run():
            self.executor.on_invocation_start(
                durable_execution_arn="arn:exec",
                context=ctx,
                execution_operation=op,
                is_replaying=False,
            )
            self.executor.on_invocation_end(
                output=output,
            )

        self.assertIn("invocation_end:req-123", self.plugin.calls)
        self.assertIn("execution_end:req-123", self.plugin.calls)

    def test_failed_fires_invocation_end_and_execution_end(self):
        ctx = self._make_context()
        op = self._make_operation(end_ts=self.ts)
        err = ErrorObject(message="oops", type="Error", data=None, stack_trace=None)
        output = DurableExecutionInvocationOutput(
            status=InvocationStatus.FAILED, result=None, error=err
        )

        with self.executor.run():
            self.executor.on_invocation_start(
                durable_execution_arn="arn:exec",
                context=ctx,
                execution_operation=op,
                is_replaying=False,
            )
            self.executor.on_invocation_end(
                output=output,
            )

        self.assertIn("invocation_end:req-123", self.plugin.calls)
        self.assertIn("execution_end:req-123", self.plugin.calls)

    def test_pending_fires_only_invocation_end(self):
        ctx = self._make_context()
        op = self._make_operation(end_ts=self.ts)
        output = DurableExecutionInvocationOutput(
            status=InvocationStatus.PENDING, result=None, error=None
        )

        with self.executor.run():
            self.executor.on_invocation_start(
                durable_execution_arn="arn:exec",
                context=ctx,
                execution_operation=op,
                is_replaying=False,
            )
            self.executor.on_invocation_end(
                output=output,
            )

        self.assertIn("invocation_end:req-123", self.plugin.calls)
        execution_end_calls = [
            c for c in self.plugin.calls if c.startswith("execution_end")
        ]
        self.assertEqual(len(execution_end_calls), 0)

    def test_none_execution_operation_uses_now_for_end_timestamp(self):
        ctx = self._make_context()
        output = DurableExecutionInvocationOutput(
            status=InvocationStatus.SUCCEEDED, result=None, error=None
        )

        with patch("aws_durable_execution_sdk_python.plugin.datetime") as mock_dt:
            mock_dt.datetime.now.return_value = self.ts
            with self.executor.run():
                self.executor.on_invocation_start(
                    durable_execution_arn="arn:exec",
                    context=ctx,
                    execution_operation=None,
                    is_replaying=False,
                )
                self.executor.on_invocation_end(
                    output=output,
                )

        self.assertIn("invocation_end:req-123", self.plugin.calls)

    def test_none_end_timestamp_on_operation_uses_now(self):
        ctx = self._make_context()
        op = self._make_operation(end_ts=None)
        output = DurableExecutionInvocationOutput(
            status=InvocationStatus.SUCCEEDED, result=None, error=None
        )

        with self.executor.run():
            self.executor.on_invocation_start(
                durable_execution_arn="arn:exec",
                context=ctx,
                execution_operation=op,
                is_replaying=False,
            )
            self.executor.on_invocation_end(
                output=output,
            )

        self.assertIn("invocation_end:req-123", self.plugin.calls)


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
            self.executor.on_operation_action(None, update)

        self.assertIn("operation_start:op-1", self.plugin.calls)

    def test_start_action_for_step_fires_attempt_start(self):
        update = MagicMock()
        update.action = OperationAction.START
        update.operation_id = "op-1"
        update.operation_type = OperationType.STEP
        update.sub_type = OperationSubType.STEP
        update.name = "my-step"
        update.parent_id = "parent-1"

        with self.executor.run():
            self.executor.on_operation_action(None, update)

        self.assertIn("attempt_start:op-1", self.plugin.calls)

    def test_start_action_for_step_with_existing_operation_uses_attempt(self):
        update = MagicMock()
        update.action = OperationAction.START
        update.operation_id = "op-1"
        update.operation_type = OperationType.STEP
        update.sub_type = OperationSubType.STEP
        update.name = "my-step"
        update.parent_id = "parent-1"

        operation = MagicMock()
        operation.step_details = MagicMock()
        operation.step_details.attempt = 3

        with self.executor.run():
            self.executor.on_operation_action(operation, update)

        self.assertIn("attempt_start:op-1", self.plugin.calls)

    def test_start_action_for_non_step_does_not_fire_attempt_start(self):
        update = MagicMock()
        update.action = OperationAction.START
        update.operation_id = "op-1"
        update.operation_type = OperationType.WAIT
        update.sub_type = OperationSubType.WAIT
        update.name = "my-wait"
        update.parent_id = "parent-1"

        with self.executor.run():
            self.executor.on_operation_action(None, update)

        self.assertIn("operation_start:op-1", self.plugin.calls)
        attempt_calls = [c for c in self.plugin.calls if c.startswith("attempt")]
        self.assertEqual(len(attempt_calls), 0)

    def test_non_start_action_does_not_fire(self):
        update = MagicMock()
        update.action = OperationAction.SUCCEED
        update.operation_id = "op-1"

        self.executor.on_operation_action(None, update)

        self.assertEqual(self.plugin.calls, [])

    def test_fail_action_does_not_fire(self):
        update = MagicMock()
        update.action = OperationAction.FAIL
        update.operation_id = "op-1"

        self.executor.on_operation_action(None, update)

        self.assertEqual(self.plugin.calls, [])


class TestPluginExecutorOnOperationUpdate(unittest.TestCase):
    """Tests for PluginExecutor.on_operation_update."""

    def setUp(self):
        self.plugin = _TrackingPlugin()
        self.executor = PluginExecutor(plugins=[self.plugin])
        self.ts = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)

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
        op.start_timestamp = self.ts
        op.end_timestamp = self.ts
        op.status = status
        op.step_details = step_details
        op.callback_details = callback_details
        op.chained_invoke_details = chained_invoke_details
        op.context_details = context_details
        return op

    def test_terminal_status_with_step_details_fires_attempt_and_operation(self):
        step_details = MagicMock()
        step_details.attempt = 2
        step_details.error = None
        op = self._make_operation(
            status=OperationStatus.SUCCEEDED, step_details=step_details
        )

        with self.executor.run():
            self.executor.on_operation_update(op)

        self.assertIn("attempt_end:op-1", self.plugin.calls)
        self.assertIn("operation_end:op-1", self.plugin.calls)

    def test_pending_status_with_step_details_fires_attempt_only(self):
        step_details = MagicMock()
        step_details.attempt = 1
        step_details.error = ErrorObject(
            message="retry", type="Error", data=None, stack_trace=None
        )
        op = self._make_operation(
            status=OperationStatus.PENDING, step_details=step_details
        )

        with self.executor.run():
            self.executor.on_operation_update(op)

        self.assertIn("attempt_end:op-1", self.plugin.calls)
        # Should NOT fire operation_end for PENDING
        operation_end_calls = [
            c for c in self.plugin.calls if c.startswith("operation_end")
        ]
        self.assertEqual(len(operation_end_calls), 0)

    def test_terminal_status_without_step_details_fires_operation_only(self):
        op = self._make_operation(status=OperationStatus.FAILED, step_details=None)

        with self.executor.run():
            self.executor.on_operation_update(op)

        self.assertIn("operation_end:op-1", self.plugin.calls)
        attempt_calls = [c for c in self.plugin.calls if c.startswith("attempt")]
        self.assertEqual(len(attempt_calls), 0)

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

    def _make_error(self, msg="error"):
        return ErrorObject(message=msg, type="Error", data=None, stack_trace=None)

    def test_extract_error_from_step_details(self):
        err = self._make_error("step error")
        op = MagicMock()
        op.step_details = MagicMock()
        op.step_details.error = err
        op.callback_details = None
        op.chained_invoke_details = None
        op.context_details = None

        result = PluginExecutor._extract_error(op)
        self.assertEqual(result.message, "step error")

    def test_extract_error_from_callback_details(self):
        err = self._make_error("callback error")
        op = MagicMock()
        op.step_details = None
        op.callback_details = MagicMock()
        op.callback_details.error = err
        op.chained_invoke_details = None
        op.context_details = None

        result = PluginExecutor._extract_error(op)
        self.assertEqual(result.message, "callback error")

    def test_extract_error_from_chained_invoke_details(self):
        err = self._make_error("invoke error")
        op = MagicMock()
        op.step_details = None
        op.callback_details = None
        op.chained_invoke_details = MagicMock()
        op.chained_invoke_details.error = err
        op.context_details = None

        result = PluginExecutor._extract_error(op)
        self.assertEqual(result.message, "invoke error")

    def test_extract_error_from_context_details(self):
        err = self._make_error("context error")
        op = MagicMock()
        op.step_details = None
        op.callback_details = None
        op.chained_invoke_details = None
        op.context_details = MagicMock()
        op.context_details.error = err

        result = PluginExecutor._extract_error(op)
        self.assertEqual(result.message, "context error")

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
        err = self._make_error("callback error")
        op = MagicMock()
        op.step_details = MagicMock()
        op.step_details.error = None
        op.callback_details = MagicMock()
        op.callback_details.error = err
        op.chained_invoke_details = None
        op.context_details = None

        result = PluginExecutor._extract_error(op)
        self.assertEqual(result.message, "callback error")

    def test_extract_error_priority_step_over_callback(self):
        """step_details error takes priority over callback error."""
        step_err = self._make_error("step error")
        cb_err = self._make_error("callback error")
        op = MagicMock()
        op.step_details = MagicMock()
        op.step_details.error = step_err
        op.callback_details = MagicMock()
        op.callback_details.error = cb_err
        op.chained_invoke_details = None
        op.context_details = None

        result = PluginExecutor._extract_error(op)
        self.assertEqual(result.message, "step error")


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


class _NoOpPlugin(DurableExecutionPlugin):
    """Concrete subclass that inherits all default no-op methods."""

    pass


class _TrackingPlugin(DurableExecutionPlugin):
    """Concrete subclass that tracks calls to all hooks."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def on_execution_start(self, info: ExecutionStartInfo) -> None:
        self.calls.append(f"execution_start:{info.request_id}")

    def on_execution_end(self, info: ExecutionEndInfo) -> None:
        self.calls.append(f"execution_end:{info.request_id}")

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self.calls.append(f"invocation_start:{info.request_id}")

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        self.calls.append(f"invocation_end:{info.request_id}")

    def on_operation_start(self, info: OperationStartInfo) -> None:
        self.calls.append(f"operation_start:{info.operation_id}")

    def on_operation_end(self, info: OperationEndInfo) -> None:
        self.calls.append(f"operation_end:{info.operation_id}")

    def on_operation_attempt_start(self, info: AttemptStartInfo) -> None:
        self.calls.append(f"attempt_start:{info.operation_id}")

    def on_operation_attempt_end(self, info: AttemptEndInfo) -> None:
        self.calls.append(f"attempt_end:{info.operation_id}")


class _FailingPlugin(DurableExecutionPlugin):
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


if __name__ == "__main__":
    unittest.main()
