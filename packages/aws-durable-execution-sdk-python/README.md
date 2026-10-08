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

### Selecting an OpenTelemetry view

Enable at most one of `InvocationOtelPlugin` and `ExecutionOtelPlugin`. This
constraint applies to the combined `plugins=[...]` argument and
`DURABLE_EXECUTION_PLUGINS=otel-invocation` or `otel-execution` selection.
Core 2.1+ with OTel 1.1+ rejects both at cold start with `PluginLoadError` naming the
conflicting views; keep only one. Choose Invocation for work within each Lambda
invocation or Execution for logical operations across the durable execution.
Unrelated instrumentation plugins can run alongside either view. Existing valid
registrations remain supported with OTel 1.1 on older core 2.0.x; those cores do
not implement the new exclusivity validation.

Plugin authors explicitly opt in by declaring
`__durable_registration_api__ = 1` on a plugin class. That class and its subclasses
have their optional `exclusive_group` metadata validated and their
optional `on_registration_result(registered)` callback invoked. A missing group
or `None` adds no exclusivity constraint. The callback can release rejected
constructor resources and must preserve resources from earlier accepted use.

The first class declaring the marker in the method resolution order gates
registration and selects the callback contract. An explicit marker other than
the integer `1` shadows an ancestor's opt-in and disables this capability.
Once enabled, every class in the MRO explicitly declaring the integer `1`
contributes its resolved `exclusive_group`. These constraints accumulate:
repeating the marker with a new group adds to inherited groups rather than
replacing them, and `None` does not erase an inherited constraint. A group is
counted once per plugin registration even if several ancestors declare it.
Unmarked classes' coincidental same-named legacy fields/helpers remain inert.
Subclasses of the bundled OTel views therefore retain the view group when they
add their own group. Repeat the marker to add a group or customize the callback;
the callback is still notified once per plugin. Plugins with no marker anywhere
in their hierarchy retain their existing attributes/helpers. The generic plugin base defines
neither attribute nor hook, and provider API version 1 and existing plugin
lifecycle order are unchanged.

### Optional handler context scopes

A plugin can provide `handler_context(info)` returning a context manager. The
updated core looks up this optional method normally, so inherited methods work
without extra declarations. A missing or non-callable attribute is ignored.
The core enters these scopes around the top-level handler on its worker thread,
in registration order, and closes them in reverse order. Cleanup receives no
handler exception and cannot suppress or replace its outcome. Invocation hooks
retain their original thread and order; failed lookup, setup or entry bindings
are discarded, and successful scope cleanup stays in the context that owns its
tokens.

`handler_context` is an optional plugin API name: callable methods or attributes
with that name are invoked. The generic plugin base does not require a default
method. Older cores ignore this optional API and retain their existing behavior.
The provider API version and dependency requirements are unchanged.

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
