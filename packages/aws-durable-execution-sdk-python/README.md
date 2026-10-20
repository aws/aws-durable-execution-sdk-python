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
hold **per-invocation** state in ordinary attributes.

Per-invocation is narrower than per-execution, and the difference matters. A
durable execution spans as many invocations as it waits, retries or resumes, and
the instance is dropped when each of those returns — so anything a plugin keeps in
its attributes is gone by the next invocation of the same execution. State that
has to survive that has two honest homes: rebuild it from the operation map the
invocation hooks carry (`InvocationStartInfo.operations` is a full snapshot,
which is how the bundled Telemetry exporter reports operations that completed in an
earlier invocation), or put it on the factory, which outlives every invocation —
keyed by execution ARN, and pruned by the owner, because the SDK will not tell the
factory when an execution ends for good.

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

### Selecting an OpenTelemetry view

Enable at most one of `InvocationOtelPlugin` and `ExecutionOtelPlugin`. This
constraint applies to the combined `plugins=[...]` argument and
`DURABLE_EXECUTION_PLUGINS=otel-invocation` or `otel-execution` selection.
Core 2.1+ with OTel 1.1+ rejects both at cold start with `PluginLoadError` naming the
conflicting views; keep only one. Choose Invocation for work within each Lambda
invocation or Execution for logical operations across the durable execution.
Unrelated instrumentation plugins can run alongside either view. OTel 1.1 requires core 2.1 or later for the invocation-worker lifecycle as well
as view-exclusivity validation. Release core 2.1 before releasing OTel 1.1.

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

### Invocation worker lifecycle

The existing `on_invocation_start` and `on_invocation_end` hooks run on the
handler's invocation worker, both in registration order. Start hooks finish
before background checkpoint processing begins. The same worker runs the handler
(including its `finally` blocks), prepares the existing serialized output or
error, checkpoints large results when necessary, joins registered branches while
checkpointing is available, stops and waits for checkpoint processing, then calls
End before returning the outcome. The caller shuts down the handler executor.
There is no separate handler-context hook or context-manager plugin API.

When plugins are registered, the worker begins with a copy of the caller's
context-variable bindings. Successful Start bindings flow into later Start hooks,
the handler, serialization and resource cleanup. A later successful Start may
replace an earlier binding. If a Start hook raises, its new bindings are discarded
for subsequent work; its End still runs in the original Context so its tokens can
be reset. End hooks retain forward registration order, not reverse stack order,
so they must not rely on observing a stack-like unwind of other plugins' contexts.
Each SDK-managed `map` or `parallel` branch admission, including an in-process
resume, receives a fresh copy of the coordinator's bindings when plugins are
registered. Branch changes remain local even when pool threads are reused.
User-created threads retain normal Python context-variable behavior; without
registered plugins, SDK branch submission keeps its existing behavior.
The worker's invocation context is discarded on return, including after plugin
cleanup failures, leaving the host's bindings unchanged. This isolates bindings,
not mutations to shared objects or external side effects. Without plugins, the
handler retains its existing fresh-worker context behavior.

These lifecycle guarantees require core 2.1.0 or later. The OTel 1.1 plugin
requires that core version and uses the existing Start/End hooks. Upgrading the
core alone with OTel 1.0 isolates worker bindings, but does not add the newer
Invocation-view fallback to that older plugin; upgrade both packages for it.
Provider API version 1 and the independent registration API are unchanged.

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
