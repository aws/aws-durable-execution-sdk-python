"""Tests for invoker module."""

import io
import json
from unittest.mock import Mock, patch

import pytest
from botocore.awsrequest import AWSResponse  # type: ignore
from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationInput,
    DurableExecutionInvocationInputWithClient,
    DurableExecutionInvocationOutput,
    InitialExecutionState,
    InvocationStatus,
)

from aws_durable_execution_sdk_python.lambda_service import (
    ExecutionDetails,
    Operation,
    OperationStatus,
    OperationType,
)

from datetime import datetime, UTC

from aws_durable_execution_sdk_python_testing.checkpoint.processor import (
    DEFAULT_MAX_INVOCATION_PAGE_BYTES,
)
from aws_durable_execution_sdk_python_testing.execution import (
    Execution,
    OperationPaginatorState,
)
from aws_durable_execution_sdk_python_testing.invoker import (
    DEFAULT_LAMBDA_READ_TIMEOUT_SECONDS,
    INVOCATION_MARKER_HEADER,
    INVOCATION_MARKER_VALUE,
    LAMBDA_READ_TIMEOUT_HEADROOM_SECONDS,
    InProcessInvoker,
    LambdaInvoker,
    create_lambda_client,
    create_test_lambda_context,
    read_timeout_for,
)
from aws_durable_execution_sdk_python_testing.model import (
    LambdaContext,
    StartDurableExecutionInput,
)


def test_create_test_lambda_context():
    """Test creating a test lambda context."""
    context = create_test_lambda_context()

    assert (
        context.invoked_function_arn
        == "arn:aws:lambda:us-west-2:123456789012:function:test-function"
    )
    assert context.tenant_id is None  # no tenant given, as in Lambda
    assert context.client_context is not None


def test_create_test_lambda_context_reports_the_execution_values():
    context = create_test_lambda_context(
        region="eu-west-1",
        account_id="999999999999",
        function_name="child",
        tenant_id="tenant-a",
    )
    assert (
        context.invoked_function_arn
        == "arn:aws:lambda:eu-west-1:999999999999:function:child"
    )
    assert context.tenant_id == "tenant-a"


@pytest.mark.parametrize(
    ("identifier", "name", "version", "arn_suffix"),
    [
        ("child", "child", "$LATEST", ":function:child"),
        ("child:$LATEST", "child", "$LATEST", ":function:child:$LATEST"),
        ("child:7", "child", "7", ":function:child:7"),
        ("child:prod", "child", "$LATEST", ":function:child:prod"),
        (
            "child:$LATEST.PUBLISHED",
            "child",
            "$LATEST.PUBLISHED",
            ":function:child:$LATEST.PUBLISHED",
        ),
    ],
)
def test_create_test_lambda_context_fills_name_version_and_arn(
    identifier, name, version, arn_suffix
):
    """Lambda fills all three identity fields; target code reading them
    must see the same values locally. An alias's version is not known to
    the runner, so it reports $LATEST."""
    context = create_test_lambda_context(function_name=identifier)

    assert context.function_name == name
    assert context.function_version == version
    assert context.invoked_function_arn.endswith(arn_suffix)


def test_in_process_invoker_hands_the_function_identity_to_a_durable_handler():
    seen: dict = {}

    def handler(event, context):  # noqa: ARG001
        seen.update(
            name=context.function_name,
            version=context.function_version,
            arn=context.invoked_function_arn,
        )
        return {"Status": "SUCCEEDED", "Result": "ok"}

    invoker = InProcessInvoker(handler, Mock(), region="us-west-2")
    invoker.register("child:prod", handler)

    invoker.invoke("child:prod", _minimal_input())
    assert seen == {
        "name": "child",
        "version": "$LATEST",
        "arn": "arn:aws:lambda:us-west-2:123456789012:function:child:prod",
    }

    invoker.invoke("child", _minimal_input())
    assert (seen["name"], seen["version"]) == ("child", "$LATEST")
    assert seen["arn"].endswith(":function:child")


def _minimal_input() -> DurableExecutionInvocationInput:
    return DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",  # noqa: S106
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )


def test_in_process_invoker_hands_the_tenant_and_region_to_the_handler():
    seen: dict = {}

    def handler(event, context):  # noqa: ARG001
        seen["tenant_id"] = context.tenant_id
        seen["arn"] = context.invoked_function_arn
        return {"Status": "SUCCEEDED", "Result": "ok"}

    invoker = InProcessInvoker(handler, Mock(), region="eu-west-1")
    invoker.invoke(
        "child", _minimal_input(), tenant_id="tenant-a", account_id="999999999999"
    )
    assert seen == {
        "tenant_id": "tenant-a",
        "arn": "arn:aws:lambda:eu-west-1:999999999999:function:child",
    }

    invoker.invoke("child", _minimal_input())
    assert seen["tenant_id"] is None  # no tenant: none, as in Lambda
    assert seen["arn"] == "arn:aws:lambda:eu-west-1:123456789012:function:child"


def test_in_process_invoker_resolves_a_qualified_identifier_then_the_bare_name():
    seen: list = []

    def make(tag):
        def handler(event, context):  # noqa: ARG001
            seen.append((tag, context.invoked_function_arn))
            return {"Status": "SUCCEEDED", "Result": tag}

        return handler

    invoker = InProcessInvoker(make("root"), Mock(), region="us-west-2")
    invoker.register("child", make("latest"))
    invoker.register("child:prod", make("prod"))

    invoker.invoke("child:prod", _minimal_input())
    invoker.invoke("child:staging", _minimal_input())
    invoker.invoke("child", _minimal_input())
    invoker.invoke("other", _minimal_input())
    assert [tag for tag, _ in seen] == ["prod", "latest", "latest", "root"]
    assert seen[0][1].endswith(":function:child:prod")


def _stub_success_client() -> Mock:
    client = Mock()
    payload = Mock()
    payload.read.return_value = json.dumps({"Status": "SUCCEEDED"}).encode("utf-8")
    client.invoke.return_value = {
        "StatusCode": 200,
        "Payload": payload,
        "ResponseMetadata": {"HTTPHeaders": {}},
    }
    return client


def test_lambda_invoker_sends_tenant_id_when_the_execution_has_one():
    client = _stub_success_client()
    LambdaInvoker(client).invoke("child", _minimal_input(), tenant_id="tenant-a")
    assert client.invoke.call_args.kwargs["TenantId"] == "tenant-a"


def test_lambda_invoker_omits_tenant_id_when_the_execution_has_none():
    client = _stub_success_client()
    LambdaInvoker(client).invoke("child", _minimal_input())
    assert "TenantId" not in client.invoke.call_args.kwargs


def test_in_process_invoker_init():
    """Test InProcessInvoker initialization."""
    handler = Mock()
    service_client = Mock()

    invoker = InProcessInvoker(handler, service_client)

    assert invoker.handler is handler
    assert invoker.service_client is service_client


def test_in_process_invoker_create_invocation_input():
    """Test creating invocation input for in-process invoker."""
    handler = Mock()
    service_client = Mock()
    invoker = InProcessInvoker(handler, service_client)

    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation-id",
    )
    execution = Execution.new(input_data)
    execution.updated_operation_ids = ["wait-1"]

    invocation_input = invoker.create_invocation_input(execution)

    assert isinstance(invocation_input, DurableExecutionInvocationInputWithClient)
    assert invocation_input.durable_execution_arn == execution.durable_execution_arn
    assert invocation_input.checkpoint_token is not None
    assert isinstance(invocation_input.initial_execution_state, InitialExecutionState)
    assert invocation_input.updated_operation_ids == ["wait-1"]
    assert invocation_input.service_client is service_client


def test_in_process_invoker_invoke():
    """Test invoking function with in-process invoker."""
    # Mock handler that returns a valid response
    handler = Mock()
    handler.return_value = {"Status": "SUCCEEDED", "Result": "test-result"}

    service_client = Mock()
    invoker = InProcessInvoker(handler, service_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",  # noqa: S106
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    response = invoker.invoke("test-function", input_data)

    assert isinstance(response.invocation_output, DurableExecutionInvocationOutput)
    assert response.invocation_output.status == InvocationStatus.SUCCEEDED
    assert response.invocation_output.result == "test-result"
    assert isinstance(response.request_id, str)

    # Verify handler was called with correct arguments
    handler.assert_called_once()
    call_args = handler.call_args[0]
    assert isinstance(call_args[0], DurableExecutionInvocationInputWithClient)
    assert isinstance(call_args[1], LambdaContext)


def test_lambda_invoker_init():
    """Test LambdaInvoker initialization."""
    lambda_client = Mock()

    invoker = LambdaInvoker(lambda_client)

    assert invoker.lambda_client is lambda_client


def test_lambda_invoker_create():
    """Test creating LambdaInvoker with boto3 client."""
    with patch("aws_durable_execution_sdk_python_testing.invoker.boto3") as mock_boto3:
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        invoker = LambdaInvoker.create("http://localhost:3001", "us-west-2")

        assert isinstance(invoker, LambdaInvoker)
        assert invoker.lambda_client is mock_client
        mock_boto3.client.assert_called_once()
        kwargs = mock_boto3.client.call_args.kwargs
        assert mock_boto3.client.call_args.args == ("lambda",)
        assert kwargs["endpoint_url"] == "http://localhost:3001"
        assert kwargs["region_name"] == "us-west-2"
        assert kwargs["config"].read_timeout == DEFAULT_LAMBDA_READ_TIMEOUT_SECONDS
        assert kwargs["config"].retries == {"max_attempts": 0}


def test_read_timeout_outlasts_one_invocation():
    """The client waits one emulated function timeout plus headroom."""
    assert read_timeout_for(900) == 900 + LAMBDA_READ_TIMEOUT_HEADROOM_SECONDS
    assert DEFAULT_LAMBDA_READ_TIMEOUT_SECONDS == 960
    assert read_timeout_for(5400) == 5460


def test_create_lambda_client_applies_read_timeout():
    with patch("aws_durable_execution_sdk_python_testing.invoker.boto3") as mock_boto3:
        mock_boto3.client.return_value = Mock()
        create_lambda_client("http://localhost:3001", "us-west-2", 5460)
        assert mock_boto3.client.call_args.kwargs["config"].read_timeout == 5460


def _clients_created(mock_boto3) -> list[tuple[str, str]]:
    """(endpoint, region) of every client boto3 was asked for, in order."""
    return [
        (c.kwargs["endpoint_url"], c.kwargs["region_name"])
        for c in mock_boto3.client.call_args_list
    ]


def test_lambda_invoker_same_endpoint_in_a_new_region_recreates_both_client_kinds():
    """The region is the signing region, so the same URL in another region
    is another client, for handler invocations and for chained targets."""
    with patch("aws_durable_execution_sdk_python_testing.invoker.boto3") as mock_boto3:
        mock_boto3.client.side_effect = lambda *_a, **_k: Mock()
        invoker = LambdaInvoker.create("http://localhost:3001", "us-west-2")
        handler_before = invoker._get_client_for_execution("arn-1")  # noqa: SLF001
        unmarked_before = invoker.unmarked_client_for("arn-1")

        invoker.update_endpoint("http://localhost:3001", "eu-west-1")

        handler_after = invoker._get_client_for_execution("arn-2")  # noqa: SLF001
        unmarked_after = invoker.unmarked_client_for("arn-2")
        assert handler_after is invoker.lambda_client
        assert handler_after is not handler_before
        assert unmarked_after is not unmarked_before
        assert _clients_created(mock_boto3) == [
            ("http://localhost:3001", "us-west-2"),  # handler client
            ("http://localhost:3001", "us-west-2"),  # unmarked client
            ("http://localhost:3001", "eu-west-1"),  # handler client
            ("http://localhost:3001", "eu-west-1"),  # unmarked client
        ]
        # One client per (endpoint, region) and kind.
        assert invoker._get_client_for_execution("arn-2") is handler_after  # noqa: SLF001
        assert invoker.unmarked_client_for("arn-2") is unmarked_after
        for client in (unmarked_before, unmarked_after):
            client.meta.events.register.assert_not_called()


def test_lambda_invoker_keeps_a_running_execution_and_its_targets_on_its_endpoint():
    """An endpoint update applies to executions not yet pinned. An
    execution already invoked stays on its endpoint, and so do the
    chained targets it dispatches afterwards."""
    with patch("aws_durable_execution_sdk_python_testing.invoker.boto3") as mock_boto3:
        mock_boto3.client.side_effect = lambda *_a, **_k: Mock()
        invoker = LambdaInvoker.create("http://localhost:3001", "us-west-2")
        pinned_handler = invoker._get_client_for_execution("running")  # noqa: SLF001

        invoker.update_endpoint("http://localhost:3002", "us-west-2")

        assert invoker._get_client_for_execution("running") is pinned_handler  # noqa: SLF001
        running_unmarked = invoker.unmarked_client_for("running")
        new_handler = invoker._get_client_for_execution("started-later")  # noqa: SLF001
        new_unmarked = invoker.unmarked_client_for("started-later")
        assert new_handler is not pinned_handler
        assert new_unmarked is not running_unmarked
        assert _clients_created(mock_boto3) == [
            ("http://localhost:3001", "us-west-2"),  # handler client at create
            ("http://localhost:3002", "us-west-2"),  # handler client at update
            ("http://localhost:3001", "us-west-2"),  # unmarked, running execution
            ("http://localhost:3002", "us-west-2"),  # unmarked, later execution
        ]


def test_lambda_invoker_propagates_read_timeout_to_per_endpoint_clients():
    with patch("aws_durable_execution_sdk_python_testing.invoker.boto3") as mock_boto3:
        mock_boto3.client.return_value = Mock()
        invoker = LambdaInvoker.create(
            "http://localhost:3001", "us-west-2", read_timeout_seconds=5460
        )
        invoker.update_endpoint("http://localhost:3002", "us-west-2")
        invoker._get_client_for_execution("arn", "http://localhost:3003")  # noqa: SLF001
        timeouts = [
            call.kwargs["config"].read_timeout
            for call in mock_boto3.client.call_args_list
        ]
        assert timeouts == [5460, 5460, 5460]


def test_lambda_invoker_child_inherits_the_endpoint_its_parent_is_pinned_to():
    """A parent pinned to A, then an endpoint update to B: a child the
    parent starts afterwards is invoked at A, and its own chained
    targets go to A, while an unrelated execution started later goes to
    B."""
    with patch("aws_durable_execution_sdk_python_testing.invoker.boto3") as mock_boto3:
        mock_boto3.client.side_effect = lambda *_a, **_k: Mock()
        invoker = LambdaInvoker.create("http://a", "us-west-2")
        parent_handler = invoker._get_client_for_execution("parent")  # noqa: SLF001

        invoker.update_endpoint("http://b", "us-west-2")
        invoker.inherit_endpoint("child", "parent")

        assert invoker._get_client_for_execution("child") is parent_handler  # noqa: SLF001
        assert invoker.unmarked_client_for("child") is invoker.unmarked_client_for(
            "parent"
        )
        assert invoker._get_client_for_execution("later") is invoker.lambda_client  # noqa: SLF001
        assert _clients_created(mock_boto3) == [
            ("http://a", "us-west-2"),  # handler client at create
            ("http://b", "us-west-2"),  # handler client at update
            ("http://a", "us-west-2"),  # unmarked client, parent and child
        ]
        # A grandchild inherits the same pin through the child.
        invoker.inherit_endpoint("grandchild", "child")
        assert invoker._get_client_for_execution("grandchild") is parent_handler  # noqa: SLF001


def test_lambda_invoker_per_execution_endpoint_is_not_where_chained_work_goes():
    """An execution's own endpoint serves its function alone, so its
    chained targets and children go to the name-routing endpoint current
    at its first chained dispatch, and stay there across an update."""
    with patch("aws_durable_execution_sdk_python_testing.invoker.boto3") as mock_boto3:
        mock_boto3.client.side_effect = lambda *_a, **_k: Mock()
        invoker = LambdaInvoker.create("http://global", "us-west-2")
        global_handler = invoker.lambda_client
        own_handler = invoker._get_client_for_execution(  # noqa: SLF001
            "parent", "http://own", "us-west-2"
        )
        assert own_handler is not global_handler

        plain_target = invoker.unmarked_client_for("parent")
        invoker.inherit_endpoint("child", "parent")
        invoker.update_endpoint("http://later", "us-west-2")

        # The parent's handler still goes to its own endpoint; its plain
        # targets and its child go to the endpoint pinned at first dispatch.
        assert (
            invoker._get_client_for_execution("parent", "http://own", "us-west-2")  # noqa: SLF001
            is own_handler
        )
        assert invoker.unmarked_client_for("parent") is plain_target
        assert invoker.unmarked_client_for("child") is plain_target
        assert invoker._get_client_for_execution("child") is global_handler  # noqa: SLF001
        assert invoker._get_client_for_execution("later") is invoker.lambda_client  # noqa: SLF001
        assert _clients_created(mock_boto3) == [
            ("http://global", "us-west-2"),  # handler client at create
            ("http://own", "us-west-2"),  # the parent's own endpoint, its region
            ("http://global", "us-west-2"),  # unmarked client, parent and child
            ("http://later", "us-west-2"),  # handler client at update
        ]


def test_lambda_invoker_signs_an_own_endpoint_in_the_execution_region():
    """The signing region for an execution's own endpoint is the
    execution's region, so an endpoint update to another region does not
    change it. Without a region the historical default applies."""
    with patch("aws_durable_execution_sdk_python_testing.invoker.boto3") as mock_boto3:
        mock_boto3.client.side_effect = lambda *_a, **_k: Mock()
        invoker = LambdaInvoker.create("http://global", "us-west-2")
        own_handler = invoker._get_client_for_execution(  # noqa: SLF001
            "running", "http://own", "us-west-2"
        )

        invoker.update_endpoint("http://global", "eu-west-1")

        assert (
            invoker._get_client_for_execution("running", "http://own", "us-west-2")  # noqa: SLF001
            is own_handler
        )
        invoker._get_client_for_execution("no-region", "http://own")  # noqa: SLF001
        assert _clients_created(mock_boto3) == [
            ("http://global", "us-west-2"),  # handler client at create
            ("http://own", "us-west-2"),  # the running execution's own endpoint
            ("http://global", "eu-west-1"),  # handler client at update
            ("http://own", "us-east-1"),  # no region given: historical default
        ]


def test_lambda_invoker_invoke_uses_the_region_given_for_an_own_endpoint():
    """invoke() hands the execution's region to the client lookup."""
    with patch("aws_durable_execution_sdk_python_testing.invoker.boto3") as mock_boto3:

        def new_client(*_a, **_k):
            client = Mock()
            payload = Mock()
            payload.read.return_value = json.dumps({"Status": "SUCCEEDED"}).encode()
            client.invoke.return_value = {
                "StatusCode": 200,
                "Payload": payload,
                "ResponseMetadata": {"HTTPHeaders": {"x-amzn-RequestId": "r"}},
            }
            return client

        mock_boto3.client.side_effect = new_client
        invoker = LambdaInvoker.create("http://global", "us-west-2")
        input_data = DurableExecutionInvocationInput(
            durable_execution_arn="arn",
            checkpoint_token="token",  # noqa: S106
            initial_execution_state=InitialExecutionState(
                operations=[], next_marker=""
            ),
        )

        invoker.invoke(
            "fn", input_data, endpoint_url="http://own", region_name="eu-west-1"
        )

        assert _clients_created(mock_boto3) == [
            ("http://global", "us-west-2"),
            ("http://own", "eu-west-1"),
        ]
        own_client = invoker._endpoint_clients[("http://own", "eu-west-1")]  # noqa: SLF001
        own_client.invoke.assert_called_once()
        invoker.lambda_client.invoke.assert_not_called()


def test_in_process_invoker_has_no_endpoint_to_inherit():
    invoker = InProcessInvoker(Mock(), Mock())
    invoker.update_endpoint("http://ignored", "us-west-2")
    invoker.inherit_endpoint("child", "parent")


class _RawBody(io.BytesIO):
    """Minimal raw HTTP body for a fabricated botocore response."""

    def stream(self, *_args, **_kwargs):
        yield self.getvalue()


def _invoke_headers(monkeypatch, **client_kwargs) -> dict:
    """Create a client, send one Invoke, and return the HTTP headers it put on the wire.

    Dummy credentials are set before the client is created because boto3
    resolves them at creation time. The request is answered at
    ``before-send``, after botocore has built and signed it, so every
    ``before-call`` hook (including the client's own) has already run.
    """
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    client = create_lambda_client("http://localhost:3001", "us-west-2", **client_kwargs)
    captured: dict = {}

    def answer(request, **_kwargs):
        # botocore stores prepared header values as bytes.
        captured.update(
            {
                k: (v.decode() if isinstance(v, bytes) else v)
                for k, v in request.headers.items()
            }
        )
        return AWSResponse(
            request.url, 200, {"content-type": "application/json"}, _RawBody(b"{}")
        )

    client.meta.events.register("before-send.lambda.Invoke", answer)
    client.invoke(FunctionName="target", InvocationType="RequestResponse", Payload="{}")
    return captured


def test_handler_invocation_client_marks_its_invokes(monkeypatch):
    """Every handler invocation identifies itself to the endpoint."""
    headers = _invoke_headers(monkeypatch)
    assert headers[INVOCATION_MARKER_HEADER] == INVOCATION_MARKER_VALUE
    assert INVOCATION_MARKER_HEADER == "X-Dex-Handler-Invoke"
    assert INVOCATION_MARKER_VALUE == "true"


def test_unmarked_client_sends_plain_invokes(monkeypatch):
    """A chained invoke of a non-durable target looks like any caller's Invoke."""
    assert INVOCATION_MARKER_HEADER not in _invoke_headers(
        monkeypatch, mark_invocations=False
    )


def test_lambda_invoker_create_invocation_input():
    """Test creating invocation input for lambda invoker."""
    lambda_client = Mock()
    invoker = LambdaInvoker(lambda_client)

    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation",
    )
    execution = Execution.new(input_data)

    invocation_input = invoker.create_invocation_input(execution)

    assert isinstance(invocation_input, DurableExecutionInvocationInput)
    assert invocation_input.durable_execution_arn == execution.durable_execution_arn
    assert invocation_input.checkpoint_token is not None
    assert isinstance(invocation_input.initial_execution_state, InitialExecutionState)


def test_lambda_invoker_invoke_success():
    """Test successful lambda invocation."""
    lambda_client = Mock()

    # Mock successful response
    mock_payload = Mock()
    mock_payload.read.return_value = json.dumps(
        {"Status": "SUCCEEDED", "Result": "lambda-result"}
    ).encode("utf-8")

    lambda_client.invoke.return_value = {
        "StatusCode": 200,
        "Payload": mock_payload,
        "ResponseMetadata": {"HTTPHeaders": {"x-amzn-RequestId": "test-request-id"}},
    }

    invoker = LambdaInvoker(lambda_client)

    mock_operation = Operation(
        operation_id="op-1",
        parent_id=None,
        name="test-execution",
        start_timestamp=datetime.now(UTC),
        end_timestamp=datetime.now(UTC),
        operation_type=OperationType.EXECUTION,
        status=OperationStatus.SUCCEEDED,
        execution_details=ExecutionDetails(input_payload='{"test": "data"}'),
    )

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",  # noqa: S106
        initial_execution_state=InitialExecutionState(
            operations=[mock_operation], next_marker=""
        ),
    )

    response = invoker.invoke("test-function", input_data)

    assert isinstance(response.invocation_output, DurableExecutionInvocationOutput)
    assert response.invocation_output.status == InvocationStatus.SUCCEEDED
    assert response.invocation_output.result == "lambda-result"
    assert response.request_id == "test-request-id"

    # Verify lambda client was called correctly
    lambda_client.invoke.assert_called_once_with(
        FunctionName="test-function",
        InvocationType="RequestResponse",
        Payload=json.dumps(input_data.to_json_dict()),
    )


def test_lambda_invoker_invoke_failure():
    """Test lambda invocation failure."""
    from aws_durable_execution_sdk_python_testing.exceptions import (
        DurableFunctionsTestError,
    )

    lambda_client, _ = _create_mock_lambda_client_with_exceptions()

    # Mock failed response
    mock_payload = Mock()
    lambda_client.invoke.return_value = {
        "StatusCode": 500,
        "Payload": mock_payload,
    }

    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",  # noqa: S106
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    with pytest.raises(
        DurableFunctionsTestError,
        match="Lambda invocation failed with status code: 500",
    ):
        invoker.invoke("test-function", input_data)


def test_in_process_invoker_invoke_with_execution_operations():
    """Test in-process invoker with execution that has operations."""
    handler = Mock()
    handler.return_value = {"Status": "SUCCEEDED", "Result": None}

    service_client = Mock()
    invoker = InProcessInvoker(handler, service_client)

    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation",
    )
    execution = Execution.new(input_data)
    execution.start()  # This adds operations

    invocation_input = invoker.create_invocation_input(execution)
    response = invoker.invoke("test-function", invocation_input)

    assert isinstance(response.invocation_output, DurableExecutionInvocationOutput)
    assert isinstance(response.request_id, str)
    assert response.invocation_output.status == InvocationStatus.SUCCEEDED
    assert len(invocation_input.initial_execution_state.operations) > 0


def test_lambda_invoker_create_invocation_input_with_operations():
    """Test lambda invoker creating input with execution operations."""
    lambda_client = Mock()
    invoker = LambdaInvoker(lambda_client)

    input_data = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation",
    )
    execution = Execution.new(input_data)
    execution.start()  # This adds operations

    invocation_input = invoker.create_invocation_input(execution)

    assert isinstance(invocation_input, DurableExecutionInvocationInput)
    assert len(invocation_input.initial_execution_state.operations) > 0
    # A single page that fits carries an empty next_marker (no
    # continuation), never a real marker.
    assert invocation_input.initial_execution_state.next_marker == ""


def test_lambda_invoker_invoke_empty_function_name():
    """Test lambda invocation with empty function name."""
    from aws_durable_execution_sdk_python_testing.exceptions import (
        InvalidParameterValueException,
    )

    lambda_client = Mock()
    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    with pytest.raises(
        InvalidParameterValueException, match="Function name is required"
    ):
        invoker.invoke("", input_data)


def test_lambda_invoker_invoke_whitespace_function_name():
    """Test lambda invocation with whitespace-only function name."""
    from aws_durable_execution_sdk_python_testing.exceptions import (
        InvalidParameterValueException,
    )

    lambda_client = Mock()
    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    with pytest.raises(
        InvalidParameterValueException, match="Function name is required"
    ):
        invoker.invoke("   ", input_data)


def test_lambda_invoker_invoke_status_202():
    """Test lambda invocation with status code 202."""
    lambda_client = Mock()

    mock_payload = Mock()
    mock_payload.read.return_value = json.dumps(
        {"Status": "SUCCEEDED", "Result": "async-result"}
    ).encode("utf-8")

    lambda_client.invoke.return_value = {
        "StatusCode": 202,
        "Payload": mock_payload,
        "ResponseMetadata": {
            "HTTPHeaders": {"x-amzn-RequestId": "test-request-id-202"}
        },
    }

    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    response = invoker.invoke("test-function", input_data)
    assert isinstance(response.invocation_output, DurableExecutionInvocationOutput)
    assert response.request_id == "test-request-id-202"


def test_lambda_invoker_invoke_function_error():
    """Test lambda invocation with function error."""
    from aws_durable_execution_sdk_python_testing.exceptions import (
        DurableFunctionsTestError,
    )

    lambda_client, _ = _create_mock_lambda_client_with_exceptions()

    mock_payload = Mock()
    mock_payload.read.return_value = b'{"errorMessage": "Function failed"}'

    lambda_client.invoke.return_value = {
        "StatusCode": 200,
        "FunctionError": "Unhandled",
        "Payload": mock_payload,
    }

    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    with pytest.raises(
        DurableFunctionsTestError, match="Lambda invocation failed with status 200"
    ):
        invoker.invoke("test-function", input_data)


def _create_mock_lambda_client_with_exceptions():
    """Helper to create mock lambda client with all exception types."""
    lambda_client = Mock()

    class MockException(Exception):
        pass

    exceptions_mock = Mock()
    for exc_name in [
        "ResourceNotFoundException",
        "InvalidParameterValueException",
        "TooManyRequestsException",
        "ServiceException",
        "ResourceConflictException",
        "InvalidRequestContentException",
        "RequestTooLargeException",
        "UnsupportedMediaTypeException",
        "InvalidRuntimeException",
        "InvalidZipFileException",
        "ResourceNotReadyException",
        "SnapStartTimeoutException",
        "SnapStartNotReadyException",
        "SnapStartException",
        "RecursiveInvocationException",
        "InvalidSecurityGroupIDException",
        "EC2ThrottledException",
        "EFSMountConnectivityException",
        "SubnetIPAddressLimitReachedException",
        "EC2UnexpectedException",
        "InvalidSubnetIDException",
        "EC2AccessDeniedException",
        "EFSIOException",
        "ENILimitReachedException",
        "EFSMountTimeoutException",
        "EFSMountFailureException",
        "KMSAccessDeniedException",
        "KMSDisabledException",
        "KMSNotFoundException",
        "KMSInvalidStateException",
    ]:
        setattr(exceptions_mock, exc_name, MockException)

    lambda_client.exceptions = exceptions_mock
    return lambda_client, MockException


def test_lambda_invoker_invoke_resource_not_found():
    """Test lambda invocation with ResourceNotFoundException."""
    from aws_durable_execution_sdk_python_testing.exceptions import (
        ResourceNotFoundException,
    )

    lambda_client, _ = _create_mock_lambda_client_with_exceptions()

    # Create specific exception for ResourceNotFoundException
    class MockResourceNotFoundException(Exception):
        pass

    lambda_client.exceptions.ResourceNotFoundException = MockResourceNotFoundException

    lambda_client.invoke.side_effect = MockResourceNotFoundException(
        "Function not found"
    )

    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    with pytest.raises(
        ResourceNotFoundException, match="Function not found: test-function"
    ):
        invoker.invoke("test-function", input_data)


def test_lambda_invoker_invoke_invalid_parameter():
    """Test lambda invocation with InvalidParameterValueException."""
    from aws_durable_execution_sdk_python_testing.exceptions import (
        InvalidParameterValueException,
    )

    lambda_client, MockException = _create_mock_lambda_client_with_exceptions()

    # Override specific exception for this test
    class MockInvalidParameterValueException(Exception):
        pass

    lambda_client.exceptions.InvalidParameterValueException = (
        MockInvalidParameterValueException
    )

    lambda_client.invoke.side_effect = MockInvalidParameterValueException(
        "Invalid param"
    )

    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    with pytest.raises(InvalidParameterValueException, match="Invalid parameter"):
        invoker.invoke("test-function", input_data)


def test_lambda_invoker_invoke_service_exception():
    """Test lambda invocation with ServiceException."""
    from aws_durable_execution_sdk_python_testing.exceptions import (
        DurableFunctionsTestError,
    )

    lambda_client, _ = _create_mock_lambda_client_with_exceptions()

    # Create specific exception for ServiceException
    class MockServiceException(Exception):
        pass

    lambda_client.exceptions.ServiceException = MockServiceException

    lambda_client.invoke.side_effect = MockServiceException("Service error")

    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    with pytest.raises(DurableFunctionsTestError, match="Lambda invocation failed"):
        invoker.invoke("test-function", input_data)


def test_lambda_invoker_invoke_ec2_exception():
    """Test lambda invocation with EC2 exception."""
    from aws_durable_execution_sdk_python_testing.exceptions import (
        DurableFunctionsTestError,
    )

    lambda_client, _ = _create_mock_lambda_client_with_exceptions()

    # Create specific exception for EC2AccessDeniedException
    class MockEC2Exception(Exception):
        pass

    lambda_client.exceptions.EC2AccessDeniedException = MockEC2Exception

    lambda_client.invoke.side_effect = MockEC2Exception("Access denied")

    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    with pytest.raises(DurableFunctionsTestError, match="Lambda infrastructure error"):
        invoker.invoke("test-function", input_data)


def test_lambda_invoker_invoke_kms_exception():
    """Test lambda invocation with KMS exception."""
    from aws_durable_execution_sdk_python_testing.exceptions import (
        DurableFunctionsTestError,
    )

    lambda_client, _ = _create_mock_lambda_client_with_exceptions()

    # Create specific exception for KMSAccessDeniedException
    class MockKMSException(Exception):
        pass

    lambda_client.exceptions.KMSAccessDeniedException = MockKMSException

    lambda_client.invoke.side_effect = MockKMSException("KMS access denied")

    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    with pytest.raises(DurableFunctionsTestError, match="Lambda KMS error"):
        invoker.invoke("test-function", input_data)


def test_lambda_invoker_invoke_durable_execution_already_started():
    """Test lambda invocation with DurableExecutionAlreadyStartedException."""
    from aws_durable_execution_sdk_python_testing.exceptions import (
        DurableFunctionsTestError,
    )

    lambda_client, _ = _create_mock_lambda_client_with_exceptions()

    class MockDurableExecutionAlreadyStartedException(Exception):
        pass

    MockDurableExecutionAlreadyStartedException.__name__ = (
        "DurableExecutionAlreadyStartedException"
    )

    lambda_client.invoke.side_effect = MockDurableExecutionAlreadyStartedException(
        "Already started"
    )

    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    with pytest.raises(
        DurableFunctionsTestError, match="Durable execution already started"
    ):
        invoker.invoke("test-function", input_data)


def test_lambda_invoker_invoke_unexpected_exception():
    """Test lambda invocation with unexpected exception."""
    from aws_durable_execution_sdk_python_testing.exceptions import (
        DurableFunctionsTestError,
    )

    lambda_client, _ = _create_mock_lambda_client_with_exceptions()
    lambda_client.invoke.side_effect = RuntimeError("Unexpected error")

    invoker = LambdaInvoker(lambda_client)

    input_data = DurableExecutionInvocationInput(
        durable_execution_arn="test-arn",
        checkpoint_token="test-token",
        initial_execution_state=InitialExecutionState(operations=[], next_marker=""),
    )

    with pytest.raises(
        DurableFunctionsTestError, match="Unexpected error during Lambda invocation"
    ):
        invoker.invoke("test-function", input_data)


def _make_execution_with_ops(op_ids: list[str]) -> Execution:
    """Build a started execution and append STEP ops for the given ids."""
    start_input = StartDurableExecutionInput(
        account_id="123456789012",
        function_name="test-function",
        function_qualifier="$LATEST",
        execution_name="test-execution",
        execution_timeout_seconds=300,
        execution_retention_period_days=7,
        invocation_id="test-invocation",
    )
    execution = Execution.new(start_input)
    for op_id in op_ids:
        execution.operations.append(
            Operation(
                operation_id=op_id,
                operation_type=OperationType.STEP,
                status=OperationStatus.STARTED,
                start_timestamp=datetime.now(UTC),
            )
        )
    return execution


def test_in_process_invoker_pages_oversized_initial_state():
    """When the operation list exceeds the page budget, the invocation
    input carries a partial page plus a real continuation marker, and
    the marker round-trips through the paginator to the remaining ops.
    """
    op_ids = ["op-0", "op-1", "op-2", "op-3", "op-4"]
    execution = _make_execution_with_ops(op_ids)

    # Budget of 2 bytes with floor-1 sizing forces a split after 2 ops.
    invoker = InProcessInvoker(Mock(), Mock(), max_page_bytes=2)
    invocation_input = invoker.create_invocation_input(execution)

    first_page = invocation_input.initial_execution_state
    assert len(first_page.operations) < len(op_ids)
    assert first_page.next_marker
    assert first_page.next_marker != ""

    # The marker must resolve against a fresh pin of the same execution
    # and yield exactly the remaining ops in creation order.
    combined_ids: list[str] = [op.operation_id for op in first_page.operations]
    marker: str | None = first_page.next_marker
    paginator = OperationPaginatorState.pin(execution)
    while marker:
        ops, marker = paginator.page(marker, 2)
        combined_ids += [op.operation_id for op in ops]

    assert combined_ids == op_ids


def test_in_process_invoker_single_page_has_empty_marker():
    """A state that fits in one page carries an empty next_marker."""
    execution = _make_execution_with_ops(["op-0", "op-1"])

    invoker = InProcessInvoker(
        Mock(), Mock(), max_page_bytes=DEFAULT_MAX_INVOCATION_PAGE_BYTES
    )
    invocation_input = invoker.create_invocation_input(execution)

    assert [
        op.operation_id for op in invocation_input.initial_execution_state.operations
    ] == [
        "op-0",
        "op-1",
    ]
    assert invocation_input.initial_execution_state.next_marker == ""
