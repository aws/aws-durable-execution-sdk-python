# AWS Durable Execution Testing SDK for Python

[![PyPI - Version](https://img.shields.io/pypi/v/aws-durable-execution-sdk-python-testing.svg)](https://pypi.org/project/aws-durable-execution-sdk-python-testing)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/aws-durable-execution-sdk-python-testing.svg)](https://pypi.org/project/aws-durable-execution-sdk-python-testing)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/aws/aws-durable-execution-sdk-python-testing/badge)](https://scorecard.dev/viewer/?uri=github.com/aws/aws-durable-execution-sdk-python-testing)

Test Python durable functions locally, before you deploy them.

## Install

```console
pip install aws-durable-execution-sdk-python-testing
```

## Quick start

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
    durable_step,
)
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner


@durable_step
def greet(step_ctx: StepContext, name: str) -> str:
    return f"Hello, {name}"


@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    greeting = context.step(greet(event["name"]))
    context.wait(Duration.from_hours(1), name="cool-down")
    return greeting


def test_handler():
    with DurableFunctionTestRunner(handler=handler) as runner:
        result = runner.run(input='{"name": "Jane"}', execution_timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == '"Hello, Jane"'
    assert result.get_step("greet").result == '"Hello, Jane"'
```

The runner skips waits by default, so the 1-hour wait ends at once. Pass `skip_time=False` to `DurableFunctionTestRunner` to wait in real time.

`result.result` and each operation's `result` hold the serialized value. `get_step`, `get_wait`, `get_context`, `get_callback`, and `get_invoke` find an operation by name.

## Common uses

### Complete a callback

Start the execution with `run_async`, then send the callback result:

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner


def notify(callback_id: str, context) -> None:
    ...  # Send callback_id to the approver.


@durable_execution
def approval(event: dict, context: DurableContext) -> dict:
    decision = context.wait_for_callback(notify, name="approval")
    return {"decision": decision}


def test_approval():
    with DurableFunctionTestRunner(handler=approval) as runner:
        execution_arn = runner.run_async(input="{}", execution_timeout=10)
        callback_id = runner.wait_for_callback(execution_arn)
        runner.send_callback_success(callback_id, b"approved")
        result = runner.wait_for_result(execution_arn, timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == '{"decision": "approved"}'
```

### Test functions that invoke other functions

Register each `context.invoke` target with the runner. `register_durable_function` registers a durable target, which runs as a child durable execution. `register_function` registers a plain function, which runs once. An unregistered target fails the invoke with `ResourceNotFoundException`.

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner


@durable_execution
def process_payment(event: dict, context: DurableContext) -> dict:
    return {"charged": event["amount"]}


def lookup_price(event: dict, context) -> dict:
    return {"price": 25}


@durable_execution
def place_order(event: dict, context: DurableContext) -> dict:
    price = context.invoke("lookup-price", {"sku": event["sku"]}, name="price")
    payment = context.invoke(
        "process-payment", {"amount": price["price"]}, name="payment"
    )
    return {"sku": event["sku"], "charged": payment["charged"]}


def test_place_order():
    with DurableFunctionTestRunner(handler=place_order) as runner:
        runner.register_durable_function(
            "process-payment", process_payment, execution_timeout=60
        )
        runner.register_function("lookup-price", lookup_price)
        result = runner.run(input='{"sku": "book-1"}')

    assert result.result == '{"sku": "book-1", "charged": 25}'
    assert result.get_invoke("payment").status.value == "SUCCEEDED"
```

The runner matches a target by name, not by version. An invoke of `child:prod` runs the handler registered as `child:prod` if there is one, and otherwise the one registered as `child`.

## Advanced

- `DurableFunctionCloudTestRunner(function_name=..., region=...)` runs a test against a deployed function. It returns the same `DurableFunctionTestResult`.
- The local runner returns errors in the same JSON format as the AWS service, so boto3 error handling works the same way. See [docs/error-responses.md](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python-testing/docs/error-responses.md).

## Reference

- [Testing guide](https://docs.aws.amazon.com/durable-execution/testing/) in the AWS Durable Execution documentation.
- [docs/architecture.md](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python-testing/docs/architecture.md): the components, the worker model, the checkpoint flow, and a map of the code.
- [docs/error-responses.md](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python-testing/docs/error-responses.md): error formats, and how callback failures without diagnostics are stored.
- [Source](https://github.com/aws/aws-durable-execution-sdk-python/tree/main/packages/aws-durable-execution-sdk-python-testing/src/aws_durable_execution_sdk_python_testing).
- [Contributing guide](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/CONTRIBUTING.md): the test commands. The repo uses `hatch` to manage virtual environments.

## License

This project is licensed under the [Apache-2.0 License](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/LICENSE).
