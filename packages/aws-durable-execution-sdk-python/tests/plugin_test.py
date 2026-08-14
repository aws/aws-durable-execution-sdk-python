import datetime
import logging
import pickle
import unittest
from copy import deepcopy
from dataclasses import asdict, fields
from unittest.mock import MagicMock, patch

from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import (
    DurableExecutionInvocationOutput,
    ErrorObject,
    InvocationStatus,
    Operation,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType,
    StepDetails,
)
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationInfo,
    InvocationStartInfo,
    OperationChangeInfo,
    OperationEndInfo,
    OperationInfo,
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
    is_replayed=False,
    status=OperationStatus.STARTED,
)
OPERATION_END_INFO = OperationEndInfo(
    operation_id="op-1",
    operation_type=OperationType.STEP,
    sub_type=OperationSubType.STEP,
    name="my-op",
    parent_id="parent-1",
    start_time=START_TS,
    is_replayed=False,
    status=OperationStatus.FAILED,
    end_time=END_TS,
    error=ERROR,
)
OPERATION_CHANGE_INFO = OperationChangeInfo(
    execution_arn="arn:test",
    updated_operations={"op-1": OPERATION_END_INFO},
    operations={"op-1": OPERATION_END_INFO},
)

INVOCATION_START_INFO = InvocationStartInfo(
    request_id="req-1",
    execution_arn="arn:aws:lambda:us-east-1:123:durable:abc",
    execution_start_time=START_TS,
    is_first_invocation=True,
    execution_input={"name": "World"},
)
INVOCATION_END_INFO = InvocationEndInfo(
    request_id="req-1",
    execution_arn="arn:test",
    execution_start_time=START_TS,
    status=InvocationStatus.FAILED,
    error=ERROR,
    is_first_invocation=False,
    execution_input={"name": "World"},
    execution_result='"Hello, World!"',
)

USER_FUNCTION_START_INFO = UserFunctionStartInfo(
    operation_id="op-1",
    operation_type=OperationType.STEP,
    sub_type=OperationSubType.STEP,
    name="func",
    parent_id="parent-1",
    start_time=START_TS,
    is_replayed=False,
    status=OperationStatus.STARTED,
)

USER_FUNCTION_END_INFO = UserFunctionEndInfo(
    operation_id="op-1",
    operation_type=OperationType.STEP,
    sub_type=OperationSubType.STEP,
    name="func",
    parent_id="parent-1",
    start_time=START_TS,
    is_replayed=False,
    status=OperationStatus.STARTED,
    is_replay_children=False,
    attempt=1,
    outcome=UserFunctionOutcome.FAILED,
    end_time=END_TS,
    error=ERROR,
)


class TestDataClasses(unittest.TestCase):
    def test_payload_fields_are_excluded_from_repr(self):
        """Payload values must not leak into repr.

        Instrumentation logs hook infos wholesale -- the bundled OTel plugins at
        debug level, the plugin example at info -- so a payload in repr would
        implicitly write customer input and results into logs.
        """
        secret_input = {"password": "hunter2", "ssn": "123-45-6789"}
        secret_result = '{"token": "sk-live-do-not-log"}'

        start_info = InvocationStartInfo(
            request_id="req-1",
            execution_arn="arn:test",
            is_first_invocation=True,
            execution_input=secret_input,
        )
        end_info = InvocationEndInfo(
            request_id="req-1",
            execution_arn="arn:test",
            is_first_invocation=False,
            status=InvocationStatus.SUCCEEDED,
            execution_input=secret_input,
            execution_result=secret_result,
        )

        for info in (start_info, end_info):
            rendered = repr(info)
            for leaked in ("hunter2", "123-45-6789", "sk-live-do-not-log"):
                self.assertNotIn(leaked, rendered, f"{type(info).__name__}: {leaked}")
            # Identity fields are still rendered, so the repr stays useful.
            self.assertIn("req-1", rendered)
            self.assertIn("arn:test", rendered)

        # The values remain readable via the attributes themselves.
        self.assertEqual(secret_input, start_info.execution_input)
        self.assertEqual(secret_result, end_info.execution_result)

    def test_payload_fields_are_declared_non_repr(self):
        """Pin the field declaration, not just the rendered string."""
        start_fields = {f.name: f for f in fields(InvocationStartInfo)}
        end_fields = {f.name: f for f in fields(InvocationEndInfo)}

        self.assertFalse(start_fields["execution_input"].repr)
        self.assertFalse(end_fields["execution_input"].repr)
        self.assertFalse(end_fields["execution_result"].repr)

    def test_payload_fields_do_not_break_hashability(self):
        """A payload must not make a previously hashable info unhashable.

        ``execution_input`` holds arbitrary deserialized JSON, so a dict or list
        value would otherwise propagate into the generated ``__hash__`` and
        raise ``TypeError``.
        """
        base = {
            "request_id": "req-1",
            "execution_arn": "arn:test",
            "is_first_invocation": True,
        }

        for payload in ({"k": "v"}, ["a", "b"], {"nested": {"deep": [1, 2]}}, "plain"):
            info = InvocationStartInfo(**base, execution_input=payload)
            # Must not raise, and must match the payload-free hash.
            self.assertEqual(hash(InvocationStartInfo(**base)), hash(info))

        end_info = InvocationEndInfo(
            **base,
            status=InvocationStatus.SUCCEEDED,
            execution_input={"k": "v"},
            execution_result='{"big": "payload"}',
        )
        self.assertEqual(
            hash(InvocationEndInfo(**base, status=InvocationStatus.SUCCEEDED)),
            hash(end_info),
        )

    def test_payload_fields_are_excluded_from_equality(self):
        """Adding the payload fields must not change how infos compare.

        Infos built from the earlier field set still compare equal to infos
        carrying a payload, so this widening stays additive for callers.
        """
        base = {
            "request_id": "req-1",
            "execution_arn": "arn:test",
            "is_first_invocation": True,
        }

        self.assertEqual(
            InvocationStartInfo(**base),
            InvocationStartInfo(**base, execution_input={"k": "v"}),
        )
        # Two different payloads also compare equal -- payloads are incidental
        # data, not part of the info's identity.
        self.assertEqual(
            InvocationStartInfo(**base, execution_input={"a": 1}),
            InvocationStartInfo(**base, execution_input={"b": 2}),
        )
        self.assertEqual(
            InvocationEndInfo(**base, status=InvocationStatus.SUCCEEDED),
            InvocationEndInfo(
                **base,
                status=InvocationStatus.SUCCEEDED,
                execution_input={"k": "v"},
                execution_result='"result"',
            ),
        )
        # Identity fields still drive inequality.
        self.assertNotEqual(
            InvocationStartInfo(**base, execution_input={"k": "v"}),
            InvocationStartInfo(
                **{**base, "request_id": "req-2"}, execution_input={"k": "v"}
            ),
        )

    def test_operation_map_fields_are_additive(self):
        """The operation maps must be as additive as the payload fields.

        They are mapping-valued, so including them in the generated methods
        would make a previously hashable info unhashable, and their entries
        carry operation results and errors that instrumentation logs wholesale.
        """
        start_fields = {f.name: f for f in fields(InvocationStartInfo)}
        end_fields = {f.name: f for f in fields(InvocationEndInfo)}

        for holder, name in (
            (start_fields, "operations"),
            (start_fields, "updated_operations"),
            (end_fields, "operations"),
        ):
            self.assertFalse(holder[name].repr, name)
            self.assertFalse(holder[name].compare, name)
            self.assertIs(holder[name].hash, False, name)

        base = {
            "request_id": "req-1",
            "execution_arn": "arn:test",
            "is_first_invocation": True,
        }
        with_maps = InvocationStartInfo(
            **base,
            operations={"op-1": OPERATION_END_INFO},
            updated_operations={"op-1": OPERATION_END_INFO},
        )

        # Hashable despite the mapping fields, and equal to the map-free info.
        self.assertEqual(hash(InvocationStartInfo(**base)), hash(with_maps))
        self.assertEqual(InvocationStartInfo(**base), with_maps)
        # Operation results do not leak through the info's repr.
        self.assertNotIn("op-1", repr(with_maps))

    def test_payload_fields_are_declared_non_compare(self):
        """Pin the declarations, not just the observed behaviour."""
        start_fields = {f.name: f for f in fields(InvocationStartInfo)}
        end_fields = {f.name: f for f in fields(InvocationEndInfo)}

        for holder, name in (
            (start_fields, "execution_input"),
            (end_fields, "execution_input"),
            (end_fields, "execution_result"),
        ):
            self.assertFalse(holder[name].compare, name)
            self.assertIs(holder[name].hash, False, name)

    def test_payload_fields_are_marked_experimental(self):
        plugin_info_types = (
            OperationInfo,
            OperationStartInfo,
            OperationEndInfo,
            OperationChangeInfo,
            UserFunctionStartInfo,
            UserFunctionEndInfo,
            InvocationInfo,
            InvocationStartInfo,
            InvocationEndInfo,
        )
        payload_field_terms = ("input", "output", "result", "error")

        for info_type in plugin_info_types:
            for info_field in fields(info_type):
                if any(term in info_field.name for term in payload_field_terms):
                    self.assertIs(
                        info_field.metadata.get("experimental"),
                        True,
                        f"{info_type.__name__}.{info_field.name}",
                    )

    def test_operation_start_info(self):
        self.assertEqual(OPERATION_START_INFO.sub_type, OperationSubType.CALLBACK)
        self.assertEqual(OPERATION_START_INFO.name, "my-op")
        self.assertEqual(OPERATION_START_INFO.parent_id, "parent-1")
        self.assertEqual(OPERATION_START_INFO.start_time, START_TS)
        self.assertFalse(OPERATION_START_INFO.is_replayed)
        self.assertEqual(OPERATION_START_INFO.status, OperationStatus.STARTED)

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
        self.assertFalse(OPERATION_END_INFO.is_replayed)

    def test_invocation_start_info(self):
        self.assertEqual(INVOCATION_START_INFO.request_id, "req-1")
        self.assertEqual(
            INVOCATION_START_INFO.execution_arn,
            "arn:aws:lambda:us-east-1:123:durable:abc",
        )
        self.assertEqual(INVOCATION_START_INFO.execution_start_time, START_TS)
        self.assertTrue(INVOCATION_START_INFO.is_first_invocation)
        self.assertEqual(INVOCATION_START_INFO.execution_input, {"name": "World"})

    def test_invocation_info_execution_input_defaults_to_none(self):
        info = InvocationStartInfo(
            request_id="req-1",
            execution_arn="arn:test",
            execution_start_time=START_TS,
            is_first_invocation=True,
        )
        self.assertIsNone(info.execution_input)

    def test_invocation_end_info(self):
        self.assertEqual(INVOCATION_END_INFO.request_id, "req-1")
        self.assertEqual(INVOCATION_END_INFO.execution_arn, "arn:test")
        self.assertEqual(INVOCATION_END_INFO.execution_start_time, START_TS)
        self.assertFalse(INVOCATION_END_INFO.is_first_invocation)
        self.assertEqual(INVOCATION_END_INFO.status, InvocationStatus.FAILED)
        self.assertEqual(INVOCATION_END_INFO.error.message, "boom")
        self.assertEqual(INVOCATION_END_INFO.execution_input, {"name": "World"})
        self.assertEqual(INVOCATION_END_INFO.execution_result, '"Hello, World!"')

    def test_invocation_end_info_from_invocation_output_carries_input_and_result(self):
        output = DurableExecutionInvocationOutput(
            status=InvocationStatus.SUCCEEDED,
            result='"Hello, World!"',
        )
        end_info = InvocationEndInfo.from_durable_execution_invocation_output(
            INVOCATION_START_INFO, output
        )
        self.assertEqual(end_info.request_id, INVOCATION_START_INFO.request_id)
        self.assertEqual(end_info.execution_arn, INVOCATION_START_INFO.execution_arn)
        self.assertEqual(end_info.execution_input, {"name": "World"})
        self.assertEqual(end_info.execution_result, '"Hello, World!"')
        self.assertEqual(end_info.status, InvocationStatus.SUCCEEDED)
        self.assertIsNone(end_info.error)

    def test_invocation_info_operation_maps_default_to_empty(self):
        """Both maps default to empty so existing constructor calls keep working."""
        self.assertEqual({}, INVOCATION_START_INFO.operations)
        self.assertEqual({}, INVOCATION_START_INFO.updated_operations)
        self.assertEqual({}, INVOCATION_END_INFO.operations)

    def test_invocation_start_info_carries_operation_maps(self):
        info = InvocationStartInfo(
            request_id="req-1",
            execution_arn="arn:test",
            is_first_invocation=False,
            operations={"op-1": OPERATION_END_INFO, "op-2": OPERATION_START_INFO},
            updated_operations={"op-1": OPERATION_END_INFO},
        )

        self.assertEqual(
            {"op-1": OPERATION_END_INFO, "op-2": OPERATION_START_INFO},
            info.operations,
        )
        self.assertEqual({"op-1": OPERATION_END_INFO}, info.updated_operations)

    def test_invocation_end_info_inherits_start_operations(self):
        """The end factory reuses the start snapshot when given no override."""
        start = InvocationStartInfo(
            request_id="req-1",
            execution_arn="arn:test",
            is_first_invocation=True,
            operations={"op-1": OPERATION_END_INFO},
            updated_operations={"op-1": OPERATION_END_INFO},
        )
        output = DurableExecutionInvocationOutput(
            status=InvocationStatus.SUCCEEDED, result=None, error=None
        )

        end = InvocationEndInfo.from_durable_execution_invocation_output(start, output)

        self.assertEqual({"op-1": OPERATION_END_INFO}, end.operations)
        # updated_operations is a start-hook-only surface.
        self.assertFalse(hasattr(end, "updated_operations"))

    def test_invocation_end_info_accepts_fresher_operations(self):
        """An explicit map wins, letting the end hook see end-of-invocation state."""
        start = InvocationStartInfo(
            request_id="req-1",
            execution_arn="arn:test",
            is_first_invocation=True,
            operations={"op-1": OPERATION_START_INFO},
        )
        output = DurableExecutionInvocationOutput(
            status=InvocationStatus.SUCCEEDED, result=None, error=None
        )

        end = InvocationEndInfo.from_durable_execution_invocation_output(
            start,
            output,
            operations={"op-1": OPERATION_END_INFO, "op-2": OPERATION_START_INFO},
        )

        self.assertEqual(
            {"op-1": OPERATION_END_INFO, "op-2": OPERATION_START_INFO},
            end.operations,
        )

    def test_user_function_start_info(self):
        self.assertEqual(USER_FUNCTION_START_INFO.operation_id, "op-1")
        self.assertEqual(USER_FUNCTION_START_INFO.operation_type, OperationType.STEP)
        self.assertEqual(USER_FUNCTION_START_INFO.sub_type, OperationSubType.STEP)
        self.assertEqual(USER_FUNCTION_START_INFO.name, "func")
        self.assertEqual(USER_FUNCTION_START_INFO.parent_id, "parent-1")
        self.assertEqual(USER_FUNCTION_START_INFO.start_time, START_TS)
        self.assertEqual(USER_FUNCTION_START_INFO.status, OperationStatus.STARTED)

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
        self.assertIsNone(plugin.on_operation_change(OPERATION_CHANGE_INFO))
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

    def test_dispatch_operation_change_info(self):
        with self.executor.run():
            self.executor.execute_plugins(OPERATION_CHANGE_INFO, sync=False)
        self.assertIn("operation_change:op-1", self.plugin.calls)

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
            self.assertEqual(
                START_TS, self.executor._invocation_status.execution_start_time
            )
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


class TestInvocationHookOperationMaps(unittest.TestCase):
    """Tests for the operations / updated_operations maps on invocation hooks."""

    def setUp(self):
        self.captured: list[object] = []

        class _CapturingPlugin(DurableInstrumentationPlugin):
            def on_invocation_start(_self, info):  # noqa: N805
                self.captured.append(info)

            def on_invocation_end(_self, info):  # noqa: N805
                self.captured.append(info)

        self.executor = PluginExecutor(plugins=[_CapturingPlugin()])

    @staticmethod
    def _operation(operation_id, status=OperationStatus.SUCCEEDED):
        return Operation(
            operation_id=operation_id,
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name=f"name-{operation_id}",
            parent_id="root",
            status=status,
            start_timestamp=START_TS,
            end_timestamp=END_TS,
            step_details=StepDetails(attempt=1),
        )

    def _start(self, *, operations=None, updated_operation_ids=None):
        self.executor.on_invocation_start(
            execution_arn="arn:exec",
            lambda_context=LAMBDA_CTX,
            execution_start_time=START_TS,
            is_first_invocation=False,
            operations_provider=(lambda: operations)
            if operations is not None
            else None,
            updated_operation_ids=updated_operation_ids,
        )

    def test_start_info_converts_operations_to_operation_info(self):
        operations = {"op-1": self._operation("op-1")}

        with self.executor.run():
            self._start(operations=operations)

        (start_info,) = self.captured
        self.assertEqual(["op-1"], list(start_info.operations))
        converted = start_info.operations["op-1"]
        self.assertIsInstance(converted, OperationInfo)
        self.assertEqual("op-1", converted.operation_id)
        self.assertEqual(OperationStatus.SUCCEEDED, converted.status)
        self.assertEqual(START_TS, converted.start_time)
        self.assertEqual(END_TS, converted.end_time)
        self.assertEqual(1, converted.attempt)
        # Stored state, not a replay event for the operation.
        self.assertFalse(converted.is_replayed)

    def test_updated_operations_is_the_subset_named_by_updated_ids(self):
        operations = {
            "op-1": self._operation("op-1"),
            "op-2": self._operation("op-2"),
        }

        with self.executor.run():
            self._start(operations=operations, updated_operation_ids=["op-2"])

        (start_info,) = self.captured
        self.assertEqual(2, len(start_info.operations))
        self.assertEqual(["op-2"], list(start_info.updated_operations))
        # Same converted instance as in the full map, not a second conversion.
        self.assertIs(
            start_info.operations["op-2"], start_info.updated_operations["op-2"]
        )

    def test_updated_ids_absent_from_the_map_are_skipped(self):
        operations = {"op-1": self._operation("op-1")}

        with self.executor.run():
            self._start(
                operations=operations, updated_operation_ids=["op-1", "op-missing"]
            )

        (start_info,) = self.captured
        self.assertEqual(["op-1"], list(start_info.updated_operations))

    def test_first_invocation_has_empty_maps(self):
        with self.executor.run():
            self._start(operations={}, updated_operation_ids=[])

        (start_info,) = self.captured
        self.assertEqual({}, start_info.operations)
        self.assertEqual({}, start_info.updated_operations)

    def test_missing_provider_yields_empty_maps(self):
        """Callers that supply no provider still get a usable, empty map."""
        with self.executor.run():
            self._start()
            self.executor.on_invocation_end(
                output=DurableExecutionInvocationOutput(
                    status=InvocationStatus.SUCCEEDED, result=None, error=None
                )
            )

        start_info, end_info = self.captured
        self.assertEqual({}, start_info.operations)
        self.assertEqual({}, start_info.updated_operations)
        self.assertEqual({}, end_info.operations)

    def test_plugin_cannot_mutate_checkpointed_error(self):
        """A plugin must not be able to alter the error replayed to user code.

        ``ErrorObject.stack_trace`` is a mutable list and the checkpointed error
        is handed to user code on replay, so the plugin-facing view gets its own
        copy.
        """
        checkpoint_error = ErrorObject(
            message="boom", type="Error", data=None, stack_trace=["frame-A"]
        )
        operation = Operation(
            operation_id="op-1",
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name="failing",
            parent_id="root",
            status=OperationStatus.FAILED,
            start_timestamp=START_TS,
            end_timestamp=END_TS,
            step_details=StepDetails(attempt=1, error=checkpoint_error),
        )

        info = OperationInfo.from_operation(operation)

        # A distinct ErrorObject, and a distinct stack_trace list.
        self.assertIsNot(checkpoint_error, info.error)
        self.assertIsNot(checkpoint_error.stack_trace, info.error.stack_trace)
        self.assertEqual(["frame-A"], info.error.stack_trace)

        # Mutating the plugin's view leaves the checkpointed error untouched.
        info.error.stack_trace.append("injected-by-plugin")
        info.error.stack_trace.clear()
        self.assertEqual(["frame-A"], checkpoint_error.stack_trace)

    def test_no_plugins_never_invokes_the_operations_provider(self):
        """durable_execution() always passes a provider; an empty executor must
        not call it, at either hook."""
        calls = []

        def provider():
            calls.append(1)
            return {}

        executor = PluginExecutor(plugins=[])
        with executor.run():
            executor.on_invocation_start(
                execution_arn="arn:exec",
                lambda_context=LAMBDA_CTX,
                execution_start_time=START_TS,
                is_first_invocation=True,
                operations_provider=provider,
                updated_operation_ids=["op-1"],
            )
            executor.on_invocation_end(
                output=DurableExecutionInvocationOutput(
                    status=InvocationStatus.SUCCEEDED, result=None, error=None
                )
            )
            # The retained provider is dropped too, not just the snapshot.
            self.assertIsNone(executor._operations_provider)  # noqa: SLF001

        self.assertEqual([], calls)

    def test_snapshot_is_pinned_against_a_live_provider_mapping(self):
        """The snapshot must pin the point in time, whatever the provider returns.

        The SDK's own provider returns a fresh copy, but the eager snapshot is
        what actually pins the hook's view, so it must not depend on that.
        """
        live = {"op-1": self._operation("op-1")}
        seen: list[object] = []

        class _CapturingPlugin(DurableInstrumentationPlugin):
            def on_invocation_start(_self, info):  # noqa: N805
                seen.append(info)

        executor = PluginExecutor(plugins=[_CapturingPlugin()])
        with executor.run():
            executor.on_invocation_start(
                execution_arn="arn:exec",
                lambda_context=LAMBDA_CTX,
                execution_start_time=START_TS,
                is_first_invocation=True,
                operations_provider=lambda: live,
            )
            # Mutating the provider's own mapping after the hook must not change
            # what that hook reported.
            live["op-2"] = self._operation("op-2")

        (start_info,) = seen
        self.assertEqual(["op-1"], list(start_info.operations))

    def test_maps_are_plain_dicts_for_serialization(self):
        """The maps must be real dicts, not a custom Mapping.

        ``dataclasses.asdict()`` and ``pickle`` only traverse real dicts, so a
        custom Mapping here would leave the hook info unserializable for exactly
        the plugins these fields exist to serve.
        """
        with self.executor.run():
            self._start(
                operations={"op-1": self._operation("op-1")},
                updated_operation_ids=["op-1"],
            )
            self.executor.on_invocation_end(
                output=DurableExecutionInvocationOutput(
                    status=InvocationStatus.SUCCEEDED, result=None, error=None
                )
            )

        start_info, end_info = self.captured
        for info in (start_info, end_info):
            self.assertIs(dict, type(info.operations))
        self.assertIs(dict, type(start_info.updated_operations))

        # asdict recurses all the way down, so the values become nested dicts.
        as_dict = asdict(start_info)
        self.assertIs(dict, type(as_dict["operations"]))
        self.assertIs(dict, type(as_dict["operations"]["op-1"]))
        self.assertEqual("op-1", as_dict["operations"]["op-1"]["operation_id"])
        self.assertIs(dict, type(as_dict["updated_operations"]["op-1"]))

    def test_hook_infos_survive_a_pickle_round_trip(self):
        """Plugins hand infos to multiprocessing queues and caches."""
        with self.executor.run():
            self._start(
                operations={"op-1": self._operation("op-1")},
                updated_operation_ids=["op-1"],
            )
            self.executor.on_invocation_end(
                output=DurableExecutionInvocationOutput(
                    status=InvocationStatus.SUCCEEDED, result=None, error=None
                )
            )

        start_info, end_info = self.captured
        for info in (start_info, end_info):
            restored = pickle.loads(pickle.dumps(info))  # noqa: S301
            self.assertEqual(["op-1"], list(restored.operations))
            self.assertEqual("op-1", restored.operations["op-1"].operation_id)

        # And with the empty maps a plugin-free-style info carries.
        empty = InvocationStartInfo(
            request_id="req-1", execution_arn="arn:test", is_first_invocation=True
        )
        self.assertEqual({}, pickle.loads(pickle.dumps(empty)).operations)  # noqa: S301

    def test_conversion_happens_at_hook_time(self):
        """The map is built during the hook, pinning the point in time."""
        conversions: list[str] = []
        real_from_operation = OperationInfo.from_operation

        def counting(operation, **kwargs):
            conversions.append(operation.operation_id)
            return real_from_operation(operation, **kwargs)

        with (
            patch.object(OperationInfo, "from_operation", staticmethod(counting)),
            self.executor.run(),
        ):
            self._start(operations={"op-1": self._operation("op-1")})
            # Converted while the hook ran, not on first read afterwards.
            self.assertEqual(["op-1"], conversions)

        (start_info,) = self.captured
        self.assertEqual(["op-1"], list(start_info.operations))
        # Reading again does not reconvert.
        self.assertEqual(["op-1"], conversions)

    def test_end_info_reflects_operations_added_during_the_invocation(self):
        operations = {"op-1": self._operation("op-1")}

        with self.executor.run():
            self.executor.on_invocation_start(
                execution_arn="arn:exec",
                lambda_context=LAMBDA_CTX,
                execution_start_time=START_TS,
                is_first_invocation=True,
                operations_provider=lambda: operations,
            )
            operations["op-2"] = self._operation("op-2")
            self.executor.on_invocation_end(
                output=DurableExecutionInvocationOutput(
                    status=InvocationStatus.SUCCEEDED, result=None, error=None
                )
            )

        start_info, end_info = self.captured
        self.assertEqual(["op-1", "op-2"], sorted(end_info.operations))
        # The start map is snapshotted at its hook, so a later checkpoint does
        # not retroactively change what the start hook reported.
        self.assertEqual(["op-1"], sorted(start_info.operations))

    def test_failing_provider_degrades_to_an_empty_map(self):
        def provider():
            raise RuntimeError("boom")

        with (
            self.executor.run(),
            self.assertLogs(
                "aws_durable_execution_sdk_python.plugin", level="ERROR"
            ) as logs,
        ):
            self.executor.on_invocation_start(
                execution_arn="arn:exec",
                lambda_context=LAMBDA_CTX,
                execution_start_time=START_TS,
                is_first_invocation=True,
                operations_provider=provider,
                updated_operation_ids=["op-1"],
            )

        self.assertTrue(any("Failed to snapshot operations" in m for m in logs.output))
        (start_info,) = self.captured
        self.assertEqual(0, len(start_info.operations))
        self.assertEqual(0, len(start_info.updated_operations))

    def test_provider_is_released_when_the_run_scope_exits(self):
        with self.executor.run():
            self._start(operations={"op-1": self._operation("op-1")})
            self.assertIsNotNone(self.executor._operations_provider)  # noqa: SLF001

        self.assertIsNone(self.executor._operations_provider)  # noqa: SLF001


class TestInvocationInfoCopySafety(unittest.TestCase):
    """Invocation infos carrying the lazy operation map must stay copyable.

    ``dataclasses.asdict()`` deep-copies any field that is not a dict/list/tuple
    or dataclass, so anything uncopyable embedded in the lazy map would make
    every plugin that serializes an info raise -- and because plugin exceptions
    are swallowed, the hook would be silently dropped.
    """

    def setUp(self):
        self.operation = Operation(
            operation_id="op-1",
            operation_type=OperationType.STEP,
            sub_type=OperationSubType.STEP,
            name="name-op-1",
            parent_id="root",
            status=OperationStatus.SUCCEEDED,
            start_timestamp=START_TS,
            end_timestamp=END_TS,
            step_details=StepDetails(attempt=1),
        )
        self.captured: list[object] = []

        class _CapturingPlugin(DurableInstrumentationPlugin):
            def on_invocation_start(_self, info):  # noqa: N805
                self.captured.append(info)

            def on_invocation_end(_self, info):  # noqa: N805
                self.captured.append(info)

        self.executor = PluginExecutor(plugins=[_CapturingPlugin()])

    def _fire_hooks(self):
        with self.executor.run():
            self.executor.on_invocation_start(
                execution_arn="arn:exec",
                lambda_context=LAMBDA_CTX,
                execution_start_time=START_TS,
                is_first_invocation=False,
                operations_provider=lambda: {"op-1": self.operation},
                updated_operation_ids=["op-1"],
            )
            self.executor.on_invocation_end(
                output=DurableExecutionInvocationOutput(
                    status=InvocationStatus.SUCCEEDED, result=None, error=None
                )
            )
        return self.captured

    def test_asdict_on_invocation_start_info(self):
        start_info, _ = self._fire_hooks()

        as_dict = asdict(start_info)

        # Both lazy maps materialize as ordinary dicts.
        self.assertIsInstance(as_dict["operations"], dict)
        self.assertIsInstance(as_dict["updated_operations"], dict)
        self.assertEqual(["op-1"], list(as_dict["operations"]))
        self.assertEqual(["op-1"], list(as_dict["updated_operations"]))

    def test_asdict_on_invocation_end_info(self):
        _, end_info = self._fire_hooks()

        as_dict = asdict(end_info)

        self.assertIsInstance(as_dict["operations"], dict)
        self.assertEqual(["op-1"], list(as_dict["operations"]))

    def test_deepcopy_on_invocation_infos(self):
        start_info, end_info = self._fire_hooks()

        for info in (start_info, end_info):
            copied = deepcopy(info)
            self.assertIsInstance(copied.operations, dict)
            self.assertEqual(["op-1"], list(copied.operations))

    def test_deepcopy_is_independent_of_the_original(self):
        start_info, _ = self._fire_hooks()

        copied = deepcopy(start_info)
        copied.operations["op-2"] = OPERATION_END_INFO

        self.assertEqual(["op-1"], list(start_info.operations))

    def test_unresolved_map_is_still_copyable(self):
        """Copying must not require the map to have been read first."""
        start_info, _ = self._fire_hooks()
        fresh = InvocationStartInfo(
            request_id=start_info.request_id,
            execution_arn=start_info.execution_arn,
            is_first_invocation=start_info.is_first_invocation,
            operations=start_info.operations,
        )

        self.assertIsInstance(asdict(fresh)["operations"], dict)


class TestPluginExecutorOnOperationAction(unittest.TestCase):
    """Tests for PluginExecutor.on_operation_action."""

    def setUp(self):
        self.plugin = _TrackingPlugin()
        self.executor = PluginExecutor(plugins=[self.plugin])

    def test_start_action_fires_operation_start(self):
        captured: list[OperationStartInfo] = []

        class _CapturingPlugin(_TrackingPlugin):
            def on_operation_start(self, info: OperationStartInfo) -> None:
                super().on_operation_start(info)
                captured.append(info)

        self.plugin = _CapturingPlugin()
        self.executor = PluginExecutor(plugins=[self.plugin])
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
        self.assertEqual(captured[0].status, OperationStatus.STARTED)
        self.assertFalse(captured[0].is_replayed)

    def test_start_action_uses_server_start_timestamp(self):
        captured: list[OperationStartInfo] = []

        class _CapturingPlugin(_TrackingPlugin):
            def on_operation_start(self, info: OperationStartInfo) -> None:
                super().on_operation_start(info)
                captured.append(info)

        self.plugin = _CapturingPlugin()
        self.executor = PluginExecutor(plugins=[self.plugin])
        update = MagicMock()
        update.action = OperationAction.START
        update.operation_id = "op-1"
        update.operation_type = OperationType.STEP
        update.sub_type = OperationSubType.STEP
        update.name = "my-step"
        update.parent_id = "parent-1"

        operation = Operation(
            operation_id="op-1",
            operation_type=OperationType.STEP,
            status=OperationStatus.STARTED,
            start_timestamp=START_TS,
        )

        with self.executor.run():
            self.executor.on_operation_action(update, operation)

        self.assertEqual(captured[0].start_time, START_TS)

    def test_start_action_for_existing_operation_is_replayed(self):
        captured: list[OperationStartInfo] = []

        class _CapturingPlugin(_TrackingPlugin):
            def on_operation_start(self, info: OperationStartInfo) -> None:
                super().on_operation_start(info)
                captured.append(info)

        self.plugin = _CapturingPlugin()
        self.executor = PluginExecutor(plugins=[self.plugin])
        update = MagicMock()
        update.action = OperationAction.START
        update.operation_id = "op-1"
        update.operation_type = OperationType.STEP
        update.sub_type = OperationSubType.STEP
        update.name = "my-step"
        update.parent_id = "parent-1"

        current_operation = Operation(
            operation_id="op-1",
            operation_type=OperationType.STEP,
            status=OperationStatus.STARTED,
        )
        previous_operation = Operation(
            operation_id="op-1",
            operation_type=OperationType.STEP,
            status=OperationStatus.READY,
        )

        with self.executor.run():
            self.executor.on_operation_action(
                update,
                operation=current_operation,
                previous_operation=previous_operation,
            )

        self.assertTrue(captured[0].is_replayed)

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


class TestPluginExecutorOnOperationReplay(unittest.TestCase):
    """Tests for PluginExecutor.on_operation_replay."""

    def test_terminal_operation_does_not_fire_callbacks(self):
        terminal_statuses = [
            OperationStatus.SUCCEEDED,
            OperationStatus.FAILED,
            OperationStatus.TIMED_OUT,
            OperationStatus.CANCELLED,
            OperationStatus.STOPPED,
        ]

        for status in terminal_statuses:
            with self.subTest(status=status):
                plugin = _TrackingPlugin()
                executor = PluginExecutor(plugins=[plugin])
                operation = Operation(
                    operation_id="op-1",
                    operation_type=OperationType.STEP,
                    status=status,
                )

                with executor.run():
                    executor.on_operation_replay(operation)

                self.assertEqual(plugin.calls, [])

    def test_non_terminal_operation_fires_operation_start(self):
        plugin = _TrackingPlugin()
        executor = PluginExecutor(plugins=[plugin])
        operation = Operation(
            operation_id="op-1",
            operation_type=OperationType.WAIT,
            status=OperationStatus.STARTED,
        )

        with executor.run():
            executor.on_operation_replay(operation)

        self.assertEqual(plugin.calls, ["operation_start:op-1"])


class TestPluginExecutorOnChildContextEnd(unittest.TestCase):
    """Tests for locally determined child-context completion."""

    def test_builds_terminal_operation_info(self):
        captured: list[OperationEndInfo] = []

        class _CapturingPlugin(_TrackingPlugin):
            def on_operation_end(self, info: OperationEndInfo) -> None:
                super().on_operation_end(info)
                captured.append(info)

        plugin = _CapturingPlugin()
        executor = PluginExecutor(plugins=[plugin])
        identifier = OperationIdentifier(
            operation_id="context-1",
            sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
            parent_id="parent-1",
            name="book-trip",
        )
        before = datetime.datetime.now(datetime.UTC)

        with executor.run():
            executor.on_child_context_end(
                identifier,
                OperationStatus.FAILED,
                error=ERROR,
                is_replayed=True,
            )

        after = datetime.datetime.now(datetime.UTC)
        self.assertEqual(plugin.calls, ["operation_end:context-1"])
        self.assertEqual(len(captured), 1)
        info = captured[0]
        self.assertEqual(info.operation_id, "context-1")
        self.assertEqual(info.operation_type, OperationType.CONTEXT)
        self.assertEqual(info.sub_type, OperationSubType.RUN_IN_CHILD_CONTEXT)
        self.assertEqual(info.parent_id, "parent-1")
        self.assertEqual(info.name, "book-trip")
        self.assertEqual(info.status, OperationStatus.FAILED)
        self.assertEqual(info.error, ERROR)
        self.assertTrue(info.is_replayed)
        self.assertIsNone(info.start_time)
        assert info.end_time is not None
        self.assertLessEqual(before, info.end_time)
        self.assertLessEqual(info.end_time, after)


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


class TestPluginExecutorOnOperationChange(unittest.TestCase):
    """Tests for operation change notifications from on_operation_update."""

    def setUp(self):
        self.plugin = _TrackingPlugin()
        self.executor = PluginExecutor(plugins=[self.plugin])

    def test_operation_change_uses_invocation_and_operation_maps(self):
        updated_operation = Operation(
            operation_id="op-1",
            operation_type=OperationType.STEP,
            status=OperationStatus.SUCCEEDED,
            name="my-step",
            parent_id="parent-1",
            start_timestamp=START_TS,
            end_timestamp=END_TS,
            sub_type=OperationSubType.STEP,
            step_details=StepDetails(attempt=2, result='"done"', error=None),
        )
        other_operation = Operation(
            operation_id="op-2",
            operation_type=OperationType.WAIT,
            status=OperationStatus.STARTED,
            name="my-wait",
            sub_type=OperationSubType.WAIT,
        )
        captured: list[OperationChangeInfo] = []

        class _CapturingPlugin(_TrackingPlugin):
            def on_operation_change(self, info: OperationChangeInfo) -> None:
                super().on_operation_change(info)
                captured.append(info)

        self.plugin = _CapturingPlugin()
        self.executor = PluginExecutor(plugins=[self.plugin])

        with self.executor.run():
            self.executor.on_invocation_start(
                execution_arn="arn:exec",
                lambda_context=LAMBDA_CTX,
                execution_start_time=START_TS,
                is_first_invocation=False,
            )
            self.executor.on_operation_update(
                [updated_operation],
                {
                    "op-1": updated_operation,
                    "op-2": other_operation,
                },
                previous_operations={},
            )

        self.assertIn("operation_change:op-1", self.plugin.calls)
        self.assertEqual(captured[0].execution_arn, "arn:exec")
        self.assertEqual(set(captured[0].updated_operations), {"op-1"})
        self.assertEqual(set(captured[0].operations), {"op-1", "op-2"})

        updated_info = captured[0].updated_operations["op-1"]
        self.assertIsInstance(updated_info, OperationInfo)
        self.assertEqual(updated_info.status, OperationStatus.SUCCEEDED)
        self.assertEqual(updated_info.result, '"done"')
        self.assertEqual(updated_info.attempt, 2)
        self.assertEqual(updated_info.end_time, END_TS)
        self.assertFalse(updated_info.is_replayed)

    def test_operation_change_without_invocation_start_is_noop(self):
        operation = Operation(
            operation_id="op-1",
            operation_type=OperationType.STEP,
            status=OperationStatus.STARTED,
        )

        with self.executor.run():
            self.executor.on_operation_update(
                [operation],
                {"op-1": operation},
                previous_operations={},
            )

        self.assertEqual(self.plugin.calls, [])


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

    def on_operation_change(self, info: OperationChangeInfo) -> None:
        self.calls.append(
            "operation_change:" + ",".join(sorted(info.updated_operations))
        )

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

    def on_operation_change(self, info):
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
