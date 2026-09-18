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
remains supported; explicit factories run first, and a factory passed to the
decorator is not registered a second time through the environment.

A plugin is registered as a *factory*, not as an instance. A factory is an object
with a `create_plugin(info)` method taking the invocation's `InvocationStartInfo`
and returning a `DurableInstrumentationPlugin`; the SDK calls that method once per
invocation, so the instance it returns serves that one invocation only and can
hold per-execution state in ordinary attributes.

A factory is an object with a method rather than a plain callable so the
registration type can grow a second, optional member later -- a process-level
flush on execution-environment shutdown, for example -- without a second breaking
change to this surface.

Write a small factory class and construct the plugin in its `create_plugin`. The
factory holds what outlives an invocation, such as an exporter or a resolved
configuration, and setup work that can fail belongs in the factory's own
constructor rather than the plugin's:

```python
class AuditPluginFactory:
    def __init__(self, sink):
        self._sink = sink

    def create_plugin(self, info):
        return AuditPlugin(self._sink)


plugins=[AuditPluginFactory(sink)]
```

A plugin class is not a factory, and neither is a bare callable. `plugins=[MyPlugin]`
and `plugins=[lambda info: MyPlugin(sink)]` raise `PluginLoadError` during handler
initialization, because neither carries `create_plugin`. A class that declares
`create_plugin` as a `@classmethod` is accepted, since the requirement is the
member and not the kind of object.

Provider packages expose such a factory:

```python
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationStartInfo,
)


class AuditPlugin(DurableInstrumentationPlugin):
    pass


class AuditPluginFactory:
    def create_plugin(self, info: InvocationStartInfo) -> AuditPlugin:
        return AuditPlugin()


AUDIT_PLUGIN_FACTORY = AuditPluginFactory()
```

Register the factory instance in the package's `pyproject.toml`:

```toml
[project.entry-points."aws_durable_execution.plugins"]
example_audit = "example_audit:AUDIT_PLUGIN_FACTORY"
```

Provider names must be unique across installed distributions. Missing,
ambiguous, or wrongly shaped providers raise `PluginLoadError` during handler
initialization with the provider and distribution details.

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
