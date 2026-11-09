# AWS Durable Execution SDK for Python

[![Build](https://github.com/aws/aws-durable-execution-sdk-python/actions/workflows/ci.yml/badge.svg)](https://github.com/aws/aws-durable-execution-sdk-python/actions/workflows/ci.yml)
[![PyPI - Version](https://img.shields.io/pypi/v/aws-durable-execution-sdk-python.svg)](https://pypi.org/project/aws-durable-execution-sdk-python)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/aws-durable-execution-sdk-python.svg)](https://pypi.org/project/aws-durable-execution-sdk-python)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/aws/aws-durable-execution-sdk-python/badge)](https://scorecard.dev/viewer/?uri=github.com/aws/aws-durable-execution-sdk-python)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/LICENSE)

Write AWS Lambda durable functions in Python: workflows with checkpointed steps, waits, callbacks, and parallel work.

## Install

```console
pip install aws-durable-execution-sdk-python
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


@durable_step
def validate_order(step_ctx: StepContext, order_id: str) -> dict:
    step_ctx.logger.info("Validating order", extra={"order_id": order_id})
    return {"order_id": order_id, "valid": True}


@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    order_id = event["order_id"]
    validation = context.step(validate_order(order_id))
    if not validation["valid"]:
        return {"status": "rejected", "order_id": order_id}

    # The function stops while it waits, so no Lambda compute is billed.
    context.wait(Duration.from_seconds(5), name="cool-down")
    return {"status": "approved", "order_id": order_id}
```

`@durable_execution` makes `handler` the Lambda handler. Each `context.step` result is checkpointed. After a wait or a failure, the function runs again from the start, and completed steps return their checkpointed result.

## Common uses

### Wait for an external system

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution


def send_approval_request(callback_id: str, context) -> None:
    ...  # Send callback_id to the approver.


@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    decision = context.wait_for_callback(send_approval_request, name="approval")
    return {"decision": decision}
```

The approver completes the callback with the Lambda `SendDurableExecutionCallbackSuccess` API and the callback ID.

### Process a list of items

```python
from collections.abc import Sequence

from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import MapConfig


def resize(ctx: DurableContext, image: str, index: int, images: Sequence[str]) -> str:
    return ctx.step(lambda _: f"small-{image}", name=f"resize-{index}")


@durable_execution
def handler(event: dict, context: DurableContext) -> list[str]:
    batch = context.map(
        event["images"], resize, name="resize", config=MapConfig(max_concurrency=5)
    )
    return batch.get_results()
```

`context.parallel` runs a list of different functions the same way.

### Group operations in a child context

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.config import Duration


@durable_with_child_context
def ship(ctx: DurableContext, order_id: str) -> str:
    label = ctx.step(lambda _: f"label-{order_id}", name="label")
    ctx.wait(Duration.from_seconds(1), name="pickup")
    return label


@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    return context.run_in_child_context(ship(event["order_id"]), name="ship")
```

### Call another function

```python
from aws_durable_execution_sdk_python import DurableContext, durable_execution


@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    payment = context.invoke("process-payment", {"amount": 25}, name="payment")
    return {"charged": payment["charged"]}
```

The function stops while it waits for the result. The target can be a durable function or a plain Lambda function.

## Advanced

### Retries and errors

A step retries on failure according to its retry strategy. After the last attempt, the step raises `StepError`.

```python
from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    StepError,
    durable_execution,
    durable_step,
)
from aws_durable_execution_sdk_python.config import Duration, StepConfig
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)

RETRY = StepConfig(
    retry_strategy=create_retry_strategy(
        RetryStrategyConfig(max_attempts=3, initial_delay=Duration.from_seconds(2))
    )
)


@durable_step
def charge(step_ctx: StepContext, amount: int) -> dict:
    # Call the payment service here. An exception makes the step retry.
    return {"status": "charged", "amount": amount}


@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    try:
        return context.step(charge(event["amount"]), config=RETRY)
    except StepError as error:
        context.logger.warning("Charge failed", extra={"error": str(error)})
        return {"status": "failed"}
```

`RetryPresets` in `aws_durable_execution_sdk_python.retries` holds ready-made strategies.

### Replay rules

The function runs again from the start after each wait, retry, or resume. So:

1. Code outside steps must be deterministic. Put timestamps, random values, and API calls in a step.
2. Do not call durable operations inside a step. Use a child context to group operations.
3. Return values from steps. A step's changes to variables outside it are lost on replay.
4. Use `context.logger` for logs. It does not repeat log lines on replay.

### Instrumentation plugins

Set `DURABLE_EXECUTION_PLUGINS` to load installed plugins at cold start, in order:

```text
DURABLE_EXECUTION_PLUGINS=otel-invocation
```

You can also pass plugin factories to `@durable_execution(plugins=[...])`. See [docs/plugins.md](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/docs/plugins.md) for the factory contract, the OpenTelemetry views, and trace propagation.

## Reference

- [AWS Durable Execution documentation](https://docs.aws.amazon.com/durable-execution/): concepts, operations, and API reference.
- [Lambda durable functions guide](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html): deployment, permissions, and quotas.
- [docs/plugins.md](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/docs/plugins.md): instrumentation plugin contract.
- [Migration guide, 2.x to 3.0](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/docs/migration-2.x-to-3.0.md).
- [Testing SDK](https://github.com/aws/aws-durable-execution-sdk-python/tree/main/packages/aws-durable-execution-sdk-python-testing): run durable functions in local tests.
- [Source](https://github.com/aws/aws-durable-execution-sdk-python/tree/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python).

## Feedback and support

- [Bug report](https://github.com/aws/aws-durable-execution-sdk-python/issues/new?template=bug_report.yml)
- [Feature request](https://github.com/aws/aws-durable-execution-sdk-python/issues/new?template=feature_request.yml)
- [Documentation feedback](https://github.com/aws/aws-durable-execution-sdk-python/issues/new?template=documentation.yml)
- [Contributing guide](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/CONTRIBUTING.md)

## License

See the [LICENSE](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/LICENSE) file for our project's licensing.
