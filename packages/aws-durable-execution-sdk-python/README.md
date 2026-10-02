# AWS Durable Execution SDK for Python

[![Build](https://github.com/aws/aws-durable-execution-sdk-python/actions/workflows/ci.yml/badge.svg)](https://github.com/aws/aws-durable-execution-sdk-python/actions/workflows/ci.yml)
[![PyPI - Version](https://img.shields.io/pypi/v/aws-durable-execution-sdk-python.svg)](https://pypi.org/project/aws-durable-execution-sdk-python)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/aws-durable-execution-sdk-python.svg)](https://pypi.org/project/aws-durable-execution-sdk-python)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/aws/aws-durable-execution-sdk-python/badge)](https://scorecard.dev/viewer/?uri=github.com/aws/aws-durable-execution-sdk-python)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/LICENSE)

-----

Build reliable, long-running AWS Lambda workflows with checkpointed steps, waits, callbacks, and parallel execution.

## ✨ Key Features

- **Automatic checkpointing** - Resume execution after Lambda pauses or restarts
- **Durable steps** - Run work with retry strategies and deterministic replay
- **Waits and callbacks** - Pause for time or external signals without blocking Lambda
- **Parallel and map operations** - Fan out work with configurable completion criteria
- **Child contexts** - Structure complex workflows into isolated subflows
- **Replay-safe logging** - Use `context.logger` for structured, de-duplicated logs
- **Local and cloud testing** - Validate workflows with the testing SDK

## 📦 Packages

| Package | Description | Version |
| --- | --- | --- |
| `aws-durable-execution-sdk-python` | Execution SDK for Lambda durable functions | [![PyPI - Version](https://img.shields.io/pypi/v/aws-durable-execution-sdk-python.svg)](https://pypi.org/project/aws-durable-execution-sdk-python) |
| `aws-durable-execution-sdk-python-testing` | Local/cloud test runner and pytest helpers | [![PyPI - Version](https://img.shields.io/pypi/v/aws-durable-execution-sdk-python-testing.svg)](https://pypi.org/project/aws-durable-execution-sdk-python-testing) |

## Dynamic instrumentation plugins

Instrumentation plugins can be selected at Lambda cold start without importing
them in the function artifact. Install a provider package in the function or a
Lambda layer, then set an ordered allow-list:

```text
DURABLE_EXECUTION_PLUGINS=otel-invocation,example_audit
```

The SDK resolves those names from the `aws_durable_execution.plugins` Python
entry-point group when the decorated handler is initialized. An unset or blank
variable preserves the existing behavior. The decorator's `plugins` argument
remains supported; explicit plugins run first and take precedence when a
dynamic provider creates the same concrete plugin type.

Provider packages expose a versioned factory:

```python
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    DurableInstrumentationPluginProvider,
)


class AuditPlugin(DurableInstrumentationPlugin):
    pass


AUDIT_PLUGIN_PROVIDER = DurableInstrumentationPluginProvider(
    plugin_type=AuditPlugin,
    factory=AuditPlugin,
    plugin_api_version=1,
)
```

Register the provider in the package's `pyproject.toml`:

```toml
[project.entry-points."aws_durable_execution.plugins"]
example_audit = "example_audit:AUDIT_PLUGIN_PROVIDER"
```

Set `plugin_api_version` to the literal API version the provider implements.
Update it only after verifying the provider against that API version.

Provider names must be unique across installed distributions. Missing,
ambiguous, incompatible, or invalid providers raise `PluginLoadError` during
handler initialization with the provider and distribution details.

### Draft chained-invoke propagation contract

This branch prepares a model-independent plugin contract for [#751](https://github.com/aws/aws-durable-execution-sdk-python/issues/751).
The production invoke START path does not consume it and no propagation header
is transmitted yet. Public Lambda models currently lack
`ChainedInvokeOptions.XAmznTraceId` and `DistributedMapOptions`.

`aws_durable_execution_sdk_python.plugin` defines frozen `PropagationInput`
(`execution_arn`, `operation_id`, optional `parent_operation_id`, and
`target_function_name`) and frozen `PropagationMetadata` with optional
`x_amzn_trace_id`. Neither type depends on OpenTelemetry or generated service
models. Plugins can override the optional synchronous
`provide_propagation_metadata(info)` method; its default returns `None`.

`PluginExecutor.provide_propagation_metadata` collects supported members in
configured order. First non-null wins, equal values do not conflict, and unequal
later values log both plugin identities and a running conflict count. Ordinary
hook, result/getter, and diagnostic failures are isolated. Cancellation and
other `BaseException` control signals retain the existing dispatch policy.
The input and result are immutable; an asynchronous hook is unsupported.

Before this draft can ship, integrate the coordinated core 2.1 / OTel 1.1 minor
release from [#753](https://github.com/aws/aws-durable-execution-sdk-python/pull/753)
and document the matching-core prerequisite for this optional capability.
Existing supported core/plugin combinations must retain their prior tracing
behavior; no broad minimum-core rejection is required for this groundwork. Production START consumption, supported generated client
serialization, backend capability rollout, replay/failed-checkpoint integration
and deployed topology validation remain pending. The full feature in #751 remains pending.

## 🚀 Quick Start

Install the execution SDK:

```console
pip install aws-durable-execution-sdk-python
```

Create a durable Lambda handler:

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
    context.logger.info("Starting workflow", extra={"order_id": order_id})

    validation = context.step(validate_order(order_id), name="validate_order")
    if not validation["valid"]:
        return {"status": "rejected", "order_id": order_id}

    # simulate approval (real world: use wait_for_callback)
    context.wait(duration=Duration.from_seconds(5), name="await_confirmation")

    return {"status": "approved", "order_id": order_id}
```

## 📚 Documentation

The complete documentation for the AWS Durable Execution SDK for Python lives on the AWS Documentation site:

- **[AWS Durable Execution Documentation](https://docs.aws.amazon.com/durable-execution/)** - Concepts, getting started, core operations, advanced topics, and API reference
- **[AWS Lambda Durable Functions Guide](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html)** - How durable functions work on Lambda

## 💬 Feedback & Support

- [Bug report](https://github.com/aws/aws-durable-execution-sdk-python/issues/new?template=bug_report.yml)
- [Feature request](https://github.com/aws/aws-durable-execution-sdk-python/issues/new?template=feature_request.yml)
- [Documentation feedback](https://github.com/aws/aws-durable-execution-sdk-python/issues/new?template=documentation.yml)
- [Contributing guide](CONTRIBUTING.md)

## 📄 License

See the [LICENSE](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/LICENSE) file for our project's licensing.
