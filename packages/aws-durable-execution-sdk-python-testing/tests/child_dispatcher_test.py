"""Unit tests for chained-invoke dispatchers."""

import io
import json
import re
from dataclasses import replace
import threading
from types import SimpleNamespace

import pytest
from typing import Any

from botocore.exceptions import ClientError, ReadTimeoutError  # type: ignore

from aws_durable_execution_sdk_python_testing.clock import RealClock
from aws_durable_execution_sdk_python_testing.child_dispatcher import (
    CHAINED_INVOKE_TIMEOUT_ERROR_TYPE,
    DEFAULT_CHILD_EXECUTION_TIMEOUT_SECONDS,
    DEFAULT_CHILD_RETENTION_PERIOD_DAYS,
    FUNCTION_NOT_FOUND_ERROR_TYPE,
    FUNCTION_TIMEOUT_ERROR_TYPE,
    MAX_CHAINED_INVOKE_PAYLOAD_BYTES,
    ChainedInvokeRequest,
    ChildOutcome,
    EndpointChildDispatcher,
    FunctionConfig,
    FunctionConfigs,
    FunctionRegistry,
    InProcessChildDispatcher,
    KnownOutcome,
    RunInvocation,
    StartChild,
    UnconfiguredChildDispatcher,
    invoke_function,
    is_valid_tenant_id,
    parse_function_target,
)


TIMEOUT = 5  # invocation timeout used by the dispatchers under test


def _request(function_name: str = "target", payload: str | None = None):
    return ChainedInvokeRequest(
        parent_execution_arn="parent-arn",
        operation_id="op-1",
        function_name=function_name,
        tenant_id=None,
        payload=payload,
        account_id="123456789012",
    )


def _fake_context(request=None) -> object:  # noqa: ARG001
    return SimpleNamespace(aws_request_id="req-1")


# region InProcessChildDispatcher


def test_in_process_unknown_function_fails_to_start():
    dispatcher = InProcessChildDispatcher(
        FunctionRegistry(), _fake_context, TIMEOUT, RealClock()
    )
    result = dispatcher.dispatch(_request("missing"))
    assert isinstance(result, KnownOutcome)
    assert result.outcome.error is not None
    assert result.outcome.error.type == FUNCTION_NOT_FOUND_ERROR_TYPE
    assert result.outcome.error.message is not None
    assert result.outcome.error.message.startswith("Function not found: missing.")


def test_in_process_preflight_resolves_registrations_without_invoking():
    """The preflight answers from the registry alone: a registered name
    (qualified or bare) passes, an unknown one fails as not found."""
    registry = FunctionRegistry()
    registry.register("child", lambda event, context: None, is_durable=False)
    registry.register("exact:prod", lambda event, context: None, is_durable=True)
    dispatcher = InProcessChildDispatcher(registry, _fake_context, TIMEOUT, RealClock())

    assert dispatcher.preflight(parse_function_target("child")) is None
    assert dispatcher.preflight(parse_function_target("child:staging")) is None
    assert dispatcher.preflight(parse_function_target("exact:prod")) is None

    outcome = dispatcher.preflight(parse_function_target("missing"))
    assert outcome is not None
    assert outcome.error is not None
    assert outcome.error.type == FUNCTION_NOT_FOUND_ERROR_TYPE
    assert outcome.error.message is not None
    assert outcome.error.message.startswith("Function not found: missing.")


def test_in_process_durable_function_returns_child_start():
    registry = FunctionRegistry()
    registry.register(
        "target",
        lambda event, context: None,
        is_durable=True,
        execution_timeout_seconds=42,
        retention_period_days=3,
    )
    dispatcher = InProcessChildDispatcher(registry, _fake_context, TIMEOUT, RealClock())
    result = dispatcher.dispatch(_request(payload='{"n": 1}'))
    assert isinstance(result, StartChild)
    assert result.child_start.function_name == "target"
    assert result.child_start.execution_timeout_seconds == 42
    assert result.child_start.execution_retention_period_days == 3
    assert result.child_start.input == '{"n": 1}'


def test_in_process_non_durable_function_runs_once():
    registry = FunctionRegistry()
    registry.register(
        "target",
        lambda event, context: {"total": event["a"] + event["b"]},
        is_durable=False,
    )
    dispatcher = InProcessChildDispatcher(registry, _fake_context, TIMEOUT, RealClock())
    result = dispatcher.dispatch(_request(payload='{"a": 2, "b": 3}'))
    assert isinstance(result, RunInvocation)
    outcome = result.invocation()
    assert isinstance(outcome, ChildOutcome)
    assert json.loads(outcome.result) == {"total": 5}


def test_in_process_non_durable_function_error_becomes_outcome():
    def exploding(event: Any, context: Any) -> None:
        msg: str = "boom"
        raise ValueError(msg)

    registry = FunctionRegistry()
    registry.register("target", exploding, is_durable=False)
    dispatcher = InProcessChildDispatcher(registry, _fake_context, TIMEOUT, RealClock())
    dispatched = dispatcher.dispatch(_request())
    assert isinstance(dispatched, RunInvocation)
    outcome = dispatched.invocation()
    assert isinstance(outcome, ChildOutcome)
    assert outcome.error is not None
    assert "boom" in (outcome.error.message or "")


def test_in_process_non_durable_function_exceeding_timeout_fails_as_lambda_does():
    """A plain target running past the invocation timeout ends FAILED with
    Lambda's function-timeout error, as the service reports it."""
    release = threading.Event()

    def slow(event, context):  # noqa: ARG001
        release.wait(5)
        return "late"

    registry = FunctionRegistry()
    registry.register("target", slow, is_durable=False)
    dispatcher = InProcessChildDispatcher(registry, _fake_context, 1, RealClock())
    dispatched = dispatcher.dispatch(_request())
    assert isinstance(dispatched, RunInvocation)
    try:
        outcome = dispatched.invocation()
    finally:
        release.set()
    assert outcome.timed_out is False
    assert outcome.result is None
    assert outcome.error is not None
    assert outcome.error.type == FUNCTION_TIMEOUT_ERROR_TYPE == "Sandbox.Timedout"
    assert re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z req-1 Task timed out after 1\.00 seconds",
        outcome.error.message or "",
    )


def test_in_process_non_durable_result_over_limit_fails():
    registry = FunctionRegistry()
    registry.register(
        "target",
        lambda event, context: "x" * MAX_CHAINED_INVOKE_PAYLOAD_BYTES,  # noqa: ARG005
        is_durable=False,
    )
    dispatcher = InProcessChildDispatcher(registry, _fake_context, TIMEOUT, RealClock())
    dispatched = dispatcher.dispatch(_request())
    assert isinstance(dispatched, RunInvocation)
    outcome = dispatched.invocation()
    assert outcome.error is not None
    assert outcome.error.type is None
    assert outcome.error.message == (
        "CHAINED_INVOKE output payload size must be less than or equal to "
        "1048576 bytes."
    )


# endregion

# region invoke_function (non-durable targets at a Lambda endpoint)


class _StubLambdaClient:
    def __init__(self, response: dict | None = None, error: Exception | None = None):
        self.response = response
        self.error = error
        self.calls: list[dict] = []

    def invoke(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.response


def _response(body: str = "", function_error: bool = False) -> dict:
    response: dict = {
        "StatusCode": 200,
        "Payload": io.BytesIO(body.encode()),
        "ResponseMetadata": {"HTTPHeaders": {}},
    }
    if function_error:
        response["FunctionError"] = "Unhandled"
    return response


def test_invoke_function_returns_body_as_result():
    client = _StubLambdaClient(response=_response(body='{"total": 5}'))
    result = invoke_function(client, "target", '{"n": 1}', None, TIMEOUT)
    assert result == ChildOutcome(result='{"total": 5}')
    assert client.calls[0]["InvocationType"] == "RequestResponse"
    assert client.calls[0]["FunctionName"] == "target"
    assert client.calls[0]["Payload"] == '{"n": 1}'


def test_invoke_function_passes_tenant_id_when_present():
    client = _StubLambdaClient(response=_response(body="1"))
    invoke_function(client, "target", "{}", "tenant-a", TIMEOUT)
    assert client.calls[0]["TenantId"] == "tenant-a"


def test_invoke_function_absent_payload_delivers_empty_object():
    client = _StubLambdaClient(response=_response(body="1"))
    invoke_function(client, "target", None, None, TIMEOUT)
    assert client.calls[0]["Payload"] == "{}"
    assert "TenantId" not in client.calls[0]


def test_invoke_function_empty_body_is_no_result():
    client = _StubLambdaClient(response=_response(body=""))
    result = invoke_function(client, "target", None, None, TIMEOUT)
    assert result == ChildOutcome(result=None)


def test_invoke_function_error_maps_to_error_outcome():
    body = json.dumps(
        {"errorMessage": "boom", "errorType": "ValueError", "stackTrace": []}
    )
    client = _StubLambdaClient(response=_response(body=body, function_error=True))
    result = invoke_function(client, "target", None, None, TIMEOUT)
    assert result.error is not None
    assert result.error.message == "boom"
    assert result.error.type == "ValueError"


def test_invoke_function_error_with_non_json_body_keeps_body_as_message():
    client = _StubLambdaClient(response=_response(body="not json", function_error=True))
    result = invoke_function(client, "target", None, None, TIMEOUT)
    assert result.error is not None
    assert result.error.message == "not json"


def test_invoke_function_api_error_surfaces_lambda_error_code():
    client = _StubLambdaClient(
        error=ClientError(
            {
                "Error": {
                    "Code": "ResourceNotFoundException",
                    "Message": "Function not found: arn:aws:lambda:x:1:function:target",
                }
            },
            "Invoke",
        )
    )
    result = invoke_function(client, "target", None, None, TIMEOUT)
    assert result.error is not None
    assert result.error.type == "ResourceNotFoundException"
    assert (
        result.error.message == "Function not found: arn:aws:lambda:x:1:function:target"
    )


def test_invoke_function_transport_error_fails_without_error_type():
    client = _StubLambdaClient(error=ConnectionError("refused"))
    result = invoke_function(client, "target", None, None, TIMEOUT)
    assert result.error is not None
    assert result.error.type is None
    assert result.error.message == "Failed to invoke target: refused"


def test_invoke_function_read_timeout_is_a_chained_invoke_timeout():
    """The endpoint held the Invoke past the invocation timeout."""
    client = _StubLambdaClient(
        error=ReadTimeoutError(endpoint_url="http://localhost:3001")
    )
    result = invoke_function(client, "target", None, None, 900)
    assert result.timed_out is True
    assert result.error is not None
    assert result.error.type == CHAINED_INVOKE_TIMEOUT_ERROR_TYPE
    assert result.error.message == "CHAINED_INVOKE timed out after 900 seconds."


def test_invoke_function_body_read_timeout_is_a_chained_invoke_timeout():
    """The endpoint answered with headers, then held the body past the
    invocation timeout. The body read times out on its own and is the
    same chained-invoke timeout, not a generic failure."""

    class _TimingOutBody:
        def read(self) -> bytes:
            raise ReadTimeoutError(endpoint_url="http://localhost:3001")

    client = _StubLambdaClient(response={"Payload": _TimingOutBody()})
    result = invoke_function(client, "target", None, None, 900)
    assert result.timed_out is True
    assert result.error is not None
    assert result.error.type == CHAINED_INVOKE_TIMEOUT_ERROR_TYPE
    assert result.error.message == "CHAINED_INVOKE timed out after 900 seconds."


def test_invoke_function_result_over_limit_fails():
    client = _StubLambdaClient(
        response=_response(body="x" * (MAX_CHAINED_INVOKE_PAYLOAD_BYTES + 1))
    )
    result = invoke_function(client, "target", None, None, TIMEOUT)
    assert result.result is None
    assert result.error is not None
    assert result.error.message == (
        "CHAINED_INVOKE output payload size must be less than or equal to "
        "1048576 bytes."
    )


def test_invoke_function_result_at_limit_is_accepted():
    body = "x" * MAX_CHAINED_INVOKE_PAYLOAD_BYTES
    client = _StubLambdaClient(response=_response(body=body))
    assert invoke_function(client, "target", None, None, TIMEOUT).result == body


def test_invoke_function_non_utf8_body_fails_with_service_message():
    response = _response()
    response["Payload"] = io.BytesIO(b"\xff\xfe")
    client = _StubLambdaClient(response=response)
    result = invoke_function(client, "target", None, None, TIMEOUT)
    assert result.error is not None
    assert (
        result.error.message == "CHAINED_INVOKE response could not be decoded as UTF-8."
    )


def test_invoke_function_error_keeps_error_data():
    body = json.dumps(
        {
            "errorMessage": "boom",
            "errorType": "ValueError",
            "errorData": '{"code": 7}',
            "stackTrace": ["frame"],
        }
    )
    client = _StubLambdaClient(response=_response(body=body, function_error=True))
    result = invoke_function(client, "target", None, None, TIMEOUT)
    assert result.error is not None
    assert result.error.data == '{"code": 7}'
    assert result.error.stack_trace == ["frame"]


# endregion

# region parse_function_target


def test_parse_function_target_forms():
    assert parse_function_target("child").name == "child"
    assert parse_function_target("child:prod").qualifier == "prod"
    partial = parse_function_target("123456789012:function:child")
    assert (partial.name, partial.account_id, partial.region) == (
        "child",
        "123456789012",
        None,
    )
    full = parse_function_target(
        "arn:aws:lambda:us-west-2:123456789012:function:child:$LATEST"
    )
    assert (full.name, full.qualifier, full.account_id, full.region) == (
        "child",
        "$LATEST",
        "123456789012",
        "us-west-2",
    )


def test_parse_function_target_accepts_invoke_forms():
    """Dotted names, the $LATEST.PUBLISHED qualifier, and 256 characters."""
    assert parse_function_target("namespace.child").name == "namespace.child"
    assert parse_function_target("child:$LATEST.PUBLISHED").qualifier == (
        "$LATEST.PUBLISHED"
    )
    assert parse_function_target("x" * 256).name == "x" * 256


def test_parse_function_target_rejects_malformed():
    for bad in (
        "",
        "a b",
        "arn:aws:lambda:us-west-2:function:child",
        "x" * 257,
        "child\n",
        "child:prod\n",
    ):
        with pytest.raises(ValueError, match=".*"):
            parse_function_target(bad)


def test_is_valid_tenant_id_requires_the_whole_value_to_match():
    assert is_valid_tenant_id("tenant-a")
    assert not is_valid_tenant_id("tenant\n")
    assert not is_valid_tenant_id("")


# endregion

# region qualifier resolution


def test_lookup_keys_prefer_the_qualified_registration():
    request = _request()
    assert request.lookup_keys() == ("target",)
    assert request.child_qualifier() == "$LATEST"
    qualified = ChainedInvokeRequest(
        parent_execution_arn="parent-arn",
        operation_id="op-1",
        function_name="target",
        qualifier="prod",
        tenant_id=None,
        payload=None,
        account_id="123456789012",
    )
    assert qualified.lookup_keys() == ("target:prod", "target")
    assert qualified.child_qualifier() == "prod"
    assert qualified.invoke_identifier() == "target:prod"
    assert request.invoke_identifier() == "target"
    assert replace(qualified, qualifier="$LATEST").invoke_identifier() == (
        "target:$LATEST"
    )


def _qualified_request(qualifier: str) -> ChainedInvokeRequest:
    return ChainedInvokeRequest(
        parent_execution_arn="parent-arn",
        operation_id="op-1",
        function_name="target",
        qualifier=qualifier,
        tenant_id=None,
        payload="{}",
        account_id="123456789012",
    )


def test_in_process_qualified_registration_selects_the_registration_only():
    """The child keeps the requested name and qualifier whichever key matched."""
    registry = FunctionRegistry()
    registry.register("target", lambda e, c: "latest", is_durable=True)  # noqa: ARG005
    registry.register(
        "target:prod",
        lambda e, c: "prod",  # noqa: ARG005
        is_durable=True,
        execution_timeout_seconds=7,
    )
    dispatcher = InProcessChildDispatcher(registry, _fake_context, TIMEOUT, RealClock())

    result = dispatcher.dispatch(_qualified_request("prod"))
    assert isinstance(result, StartChild)
    assert result.child_start.function_name == "target"
    assert result.child_start.function_qualifier == "prod"
    assert result.child_start.execution_timeout_seconds == 7

    fallback = dispatcher.dispatch(_qualified_request("3"))
    assert isinstance(fallback, StartChild)
    assert fallback.child_start.function_name == "target"
    assert fallback.child_start.function_qualifier == "3"
    assert fallback.child_start.execution_timeout_seconds != 7


def test_endpoint_invokes_the_requested_identifier_whichever_config_matched():
    client = _StubLambdaClient(response=_response(body="1"))
    dispatcher = EndpointChildDispatcher(
        FunctionConfigs({"target": FunctionConfig(is_durable=False)}),
        lambda _arn: client,
        TIMEOUT,
    )

    dispatched = dispatcher.dispatch(_qualified_request("staging"))
    assert isinstance(dispatched, RunInvocation)
    dispatched.invocation()
    assert client.calls[-1]["FunctionName"] == "target:staging"

    dispatched = dispatcher.dispatch(_request())
    assert isinstance(dispatched, RunInvocation)
    dispatched.invocation()
    assert client.calls[-1]["FunctionName"] == "target"


# endregion

# region UnconfiguredChildDispatcher


def test_unconfigured_dispatcher_fails_every_invoke_naming_the_option():
    result = UnconfiguredChildDispatcher().dispatch(_request("child"))
    assert isinstance(result, KnownOutcome)
    assert result.outcome.error is not None
    assert result.outcome.error.type is None
    assert result.outcome.error.message is not None
    assert result.outcome.error.message.startswith("Cannot invoke child:")
    assert "--function-configs" in result.outcome.error.message


def test_unconfigured_dispatcher_fails_the_preflight_of_every_target():
    outcome = UnconfiguredChildDispatcher().preflight(parse_function_target("child"))
    assert outcome is not None
    assert outcome.error is not None
    assert outcome.error.message is not None
    assert outcome.error.message.startswith("Cannot invoke child:")
    assert "--function-configs" in outcome.error.message


# endregion

# region EndpointChildDispatcher


def test_function_config_without_durable_config_is_plain():
    for entry in ({}, None, {"Timeout": 30}):
        config = FunctionConfig.from_dict(entry)
        assert config.is_durable is False


def test_function_config_with_empty_durable_config_is_durable_with_defaults():
    config = FunctionConfig.from_dict({"DurableConfig": {}})
    assert config.is_durable is True
    assert config.execution_timeout_seconds == DEFAULT_CHILD_EXECUTION_TIMEOUT_SECONDS
    assert config.retention_period_days == DEFAULT_CHILD_RETENTION_PERIOD_DAYS


def test_function_config_reads_durable_config_fields():
    config = FunctionConfig.from_dict(
        {"DurableConfig": {"ExecutionTimeout": 45, "RetentionPeriodInDays": 3}}
    )
    assert config.is_durable is True
    assert config.execution_timeout_seconds == 45
    assert config.retention_period_days == 3


@pytest.mark.parametrize(
    ("entry", "detail"),
    [
        ([], "must be a JSON object or null"),
        (False, "must be a JSON object or null"),
        (0, "must be a JSON object or null"),
        ("", "must be a JSON object or null"),
        ("durable", "must be a JSON object or null"),
        ({"DurableConfig": True}, "DurableConfig must be a JSON object"),
        ({"DurableConfig": []}, "DurableConfig must be a JSON object"),
        ({"DurableConfig": "yes"}, "DurableConfig must be a JSON object"),
        ({"DurableConfig": {"ExecutionTimeout": "60"}}, "ExecutionTimeout"),
        ({"DurableConfig": {"ExecutionTimeout": 0}}, "ExecutionTimeout"),
        ({"DurableConfig": {"ExecutionTimeout": True}}, "ExecutionTimeout"),
        ({"DurableConfig": {"RetentionPeriodInDays": 1.5}}, "RetentionPeriodInDays"),
    ],
)
def test_function_config_rejects_a_malformed_entry(entry, detail):
    """A typo must not turn a durable target plain, nor crash with a
    bare traceback: every wrong shape is a ValueError naming the function."""
    with pytest.raises(ValueError, match=detail) as exc_info:
        FunctionConfig.from_dict(entry, "process-payment")
    assert "'process-payment'" in str(exc_info.value)


def test_function_configs_name_the_malformed_function():
    with pytest.raises(ValueError, match="'lookup-price'.*JSON object or null"):
        FunctionConfigs.from_value('{"process-payment": {}, "lookup-price": []}')


def test_function_configs_parse_inline_json():
    configs = FunctionConfigs.from_value(
        '{"process-payment": {"DurableConfig": {"ExecutionTimeout": 60}}, "lookup-price": {}}'
    )
    assert configs.by_name["process-payment"] == FunctionConfig(
        is_durable=True, execution_timeout_seconds=60
    )
    assert configs.by_name["lookup-price"].is_durable is False


def test_function_configs_reject_a_non_object():
    with pytest.raises(ValueError, match="JSON object"):
        FunctionConfigs.from_value('["process-payment"]')


def test_function_configs_read_a_file_url(tmp_path):
    path = tmp_path / "function-configs.json"
    path.write_text(
        json.dumps(
            {
                "process-payment": {"DurableConfig": {"ExecutionTimeout": 60}},
                "lookup-price": {},
                "null_entry": None,
            }
        ),
        encoding="utf-8",
    )
    configs = FunctionConfigs.from_value(f"file://{path}")
    assert set(configs.by_name) == {"process-payment", "lookup-price", "null_entry"}
    assert configs.by_name["process-payment"] == FunctionConfig(
        is_durable=True, execution_timeout_seconds=60
    )
    assert configs.by_name["lookup-price"].is_durable is False
    assert configs.by_name["null_entry"].is_durable is False


def test_endpoint_preflight_resolves_configurations_without_the_endpoint():
    class _NoClient:
        def __call__(self, _arn):
            msg = "the preflight must not touch the endpoint"
            raise AssertionError(msg)

    configs = FunctionConfigs.from_value(
        '{"child": {}, "exact:prod": {"DurableConfig": {}}}'
    )
    dispatcher = EndpointChildDispatcher(configs, _NoClient(), TIMEOUT)

    assert dispatcher.preflight(parse_function_target("child")) is None
    assert dispatcher.preflight(parse_function_target("child:v2")) is None
    assert dispatcher.preflight(parse_function_target("exact:prod")) is None

    outcome = dispatcher.preflight(parse_function_target("missing"))
    assert outcome is not None
    assert outcome.error is not None
    assert outcome.error.type == FUNCTION_NOT_FOUND_ERROR_TYPE
    assert outcome.error.message == "Function not found: missing."


def test_endpoint_unknown_function_fails_to_start():
    dispatcher = EndpointChildDispatcher(
        FunctionConfigs({}), lambda _arn: _StubLambdaClient(), TIMEOUT
    )
    result = dispatcher.dispatch(_request("missing"))
    assert isinstance(result, KnownOutcome)
    assert result.outcome.error is not None
    assert result.outcome.error.type == FUNCTION_NOT_FOUND_ERROR_TYPE
    assert result.outcome.error.message == "Function not found: missing."


def test_endpoint_durable_function_returns_child_start_from_config():
    configs = {
        "target": FunctionConfig(
            is_durable=True, execution_timeout_seconds=30, retention_period_days=2
        )
    }
    dispatcher = EndpointChildDispatcher(
        FunctionConfigs(configs), lambda _arn: _StubLambdaClient(), TIMEOUT
    )
    result = dispatcher.dispatch(_request(payload='{"n": 1}'))
    assert isinstance(result, StartChild)
    start = result.child_start
    assert start.function_name == "target"
    assert start.account_id == "123456789012"
    assert start.execution_timeout_seconds == 30
    assert start.execution_retention_period_days == 2
    assert start.input == '{"n": 1}'
    assert start.lambda_endpoint is None


def test_endpoint_non_durable_function_uses_the_parent_execution_client():
    """The client is asked for per dispatch, for the parent execution, so a
    plain target goes where that execution's own invocations go."""
    clients = {
        "arn:parent-a": _StubLambdaClient(response=_response(body="1")),
        "arn:parent-b": _StubLambdaClient(response=_response(body="2")),
    }
    dispatcher = EndpointChildDispatcher(
        FunctionConfigs({"target": FunctionConfig(is_durable=False)}),
        clients.__getitem__,
        TIMEOUT,
    )

    for arn, expected in (("arn:parent-a", "1"), ("arn:parent-b", "2")):
        dispatched = dispatcher.dispatch(replace(_request(), parent_execution_arn=arn))
        assert isinstance(dispatched, RunInvocation)
        assert dispatched.invocation().result == expected
    assert len(clients["arn:parent-a"].calls) == 1
    assert len(clients["arn:parent-b"].calls) == 1


def test_endpoint_non_durable_function_invokes_at_endpoint():
    client = _StubLambdaClient(response=_response(body='{"total": 5}'))
    configs = {"target": FunctionConfig(is_durable=False)}
    dispatcher = EndpointChildDispatcher(
        FunctionConfigs(configs), lambda _arn: client, TIMEOUT
    )
    dispatched = dispatcher.dispatch(_request(payload='{"n": 1}'))
    assert isinstance(dispatched, RunInvocation)
    result = dispatched.invocation()
    assert isinstance(result, ChildOutcome)
    assert result.result == '{"total": 5}'
    assert client.calls[0]["FunctionName"] == "target"
    assert client.calls[0]["Payload"] == '{"n": 1}'


# endregion
