# Migrating from 2.x to 3.0

`3.0` is a major release. Its breaking change is the
[per-invocation plugin factory contract](#register-plugin-factories).
Upgrade OpenTelemetry instrumentation to `2.x` at the same time as the core SDK
to `3.x`, including packages delivered through Lambda layers.

Applications that register no plugins, select none through
`DURABLE_EXECUTION_PLUGINS`, and ship no plugin providers can update their core
dependency without changing their handler or durable operation calls. If you are
upgrading from `1.x`, first apply the [1.x to 2.0 migration guide](migration-1.x-to-2.0.md).

## Why upgrade to 3.0

- **Plugin state is isolated between invocations.** Each Lambda invocation gets
  a fresh plugin instance, including concurrent invocations in Lambda Managed
  Instances. Invocation identity, spans, and buffers no longer have to share one
  plugin object.
- **Resource ownership is explicit.** Factories can share thread-safe exporters,
  providers, and configuration while keeping invocation-specific state on the
  plugin they create.
- **Registration mistakes fail early.** The SDK validates factory registrations
  when the decorated handler initializes, and rejects incompatible providers or
  conflicting OTel views with `PluginLoadError`.

## Porting to 3.0

The changes most likely to touch your code are the `plugins` argument,
custom entry-point providers, and OTel registrations. Use
[Finding affected code](#finding-affected-code) to locate them before upgrading.

| Core 2.x / OTel 1.x | Core 3.x / OTel 2.x |
| --- | --- |
| `plugins=[plugin_instance]` | `plugins=[factory_instance]` |
| `DurableInstrumentationPluginProvider(plugin_type=..., factory=..., plugin_api_version=1)` | An object with `create_plugin(info: InvocationStartInfo)` |
| `InvocationOtelPlugin(config)` registration | `InvocationOtelPluginFactory(config)` registration |
| `ExecutionOtelPlugin(config)` registration | `ExecutionOtelPluginFactory(config)` registration |
| State on a shared plugin instance | Invocation state on each new plugin; shared resources on the factory |

### Upgrade dependencies together

For the initial coordinated release, use:

| Package | Release version | Compatibility |
| --- | --- | --- |
| `aws-durable-execution-sdk-python` | `3.0.0` | Python 3.11 or later |
| `aws-durable-execution-sdk-python-otel` | `2.0.0` | Core `>=3.0.0,<4` |
| `aws-durable-execution-sdk-python-testing` | `1.3.0` | Declares Core `>=2.1.0`, including Core 3; no testing major bump is required |

After these versions are published, update your requirements and lock file:

```bash
python -m pip install --upgrade \
  "aws-durable-execution-sdk-python==3.0.0" \
  "aws-durable-execution-sdk-python-otel==2.0.0"
python -m pip check
```

Install OTel only if you use it. Its base package relies on the OpenTelemetry
distribution supplied by your ADOT/community layer. For an application that
configures its own provider, use
`aws-durable-execution-sdk-python-otel[standalone]==2.0.0` and configure that
provider and its exporter; installing the extra alone does not export traces.
See the [OTel installation guide](../packages/aws-durable-execution-sdk-python-otel/README.md#installation).

Rebuild function bundles and custom plugin distributions, and update any layer
versions that contain Core, OTel, or custom providers. Upgrading a function's
requirements does not upgrade its attached layers. Core 3 rejects the old OTel
1.x registrations; OTel 2 cannot use the Core 2 plugin contract. Check the
versions actually imported by the deployed function as well as the lock file,
because function packages take precedence over layer packages.

### Register plugin factories

The decorator's `plugins` list now takes factory objects. Each factory has a
synchronous `create_plugin(info)` method that returns a fresh
`DurableInstrumentationPlugin` instance. The factory receives the same
`InvocationStartInfo` object that the plugin's `on_invocation_start` hook receives.

For a custom plugin, replace the instance registration:

```python
# Core 2.x
@durable_execution(plugins=[AuditPlugin()])
def handler(event: dict, context: DurableContext) -> str:
    return context.step(lambda _: "ok", name="work")
```

With a factory instance:

```python
# Core 3.0
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    DurableInstrumentationPluginFactory,
    InvocationStartInfo,
)


class AuditPlugin(DurableInstrumentationPlugin):
    def __init__(self, execution_arn: str | None) -> None:
        self.execution_arn = execution_arn


class AuditPluginFactory:
    def create_plugin(self, info: InvocationStartInfo) -> AuditPlugin:
        return AuditPlugin(info.execution_arn)


audit_factory: DurableInstrumentationPluginFactory = AuditPluginFactory()


def workflow(event: dict, context: DurableContext) -> str:
    return context.step(lambda _: "ok", name="work")


handler = durable_execution(workflow, plugins=[audit_factory])
```

The factory protocol is structural: inheriting from it is optional. The returned
plugin must inherit from `DurableInstrumentationPlugin`; a duck-typed object
with similarly named hooks is not accepted.

The decorator form `@durable_execution(plugins=[audit_factory])` accepts the same
factory list as the direct wrapper form shown above.

Do not replace the old registration with a bare lambda, a plugin class, or an
uninstantiated factory class with an ordinary instance method. For example,
`plugins=[lambda info: AuditPlugin(info.execution_arn)]` and
`plugins=[AuditPluginFactory]` fail at handler initialization. Use
`plugins=[AuditPluginFactory()]`. A class with a correctly bound class/static
`create_plugin` method is a separate supported shape.

Factory shape errors raise `PluginLoadError` at initialization. Failures while
calling an otherwise valid factory happen later: ordinary exceptions, `None`,
and invalid return values are logged and that plugin is skipped for the
invocation. Thread-control failures retain their propagation semantics. Verify
plugin output explicitly; a successful workflow alone does not prove that its
instrumentation ran. Keep `create_plugin` synchronous and return the plugin
itself, never a coroutine.

### Replace custom entry-point providers

`DurableInstrumentationPluginProvider` is removed. Replace the old wrapper and
its `plugin_type`, `factory`, and `plugin_api_version` fields with the factory
object described above. Removing the version field alone is not a migration:
the old zero-argument factory must become `create_plugin(info)`, and each call
must construct a new plugin.

```python
# Core 2.x, in example_audit/__init__.py
from aws_durable_execution_sdk_python.plugin import DurableInstrumentationPluginProvider

AUDIT_PROVIDER = DurableInstrumentationPluginProvider(
    plugin_type=AuditPlugin,
    factory=AuditPlugin,
    plugin_api_version=1,
)
```

```python
# Core 3.0, alongside the AuditPluginFactory definition above
AUDIT_PLUGIN_FACTORY = AuditPluginFactory()
```

Update the distribution's entry point to resolve to that object:

```toml
[project.entry-points."aws_durable_execution.plugins"]
example_audit = "example_audit:AUDIT_PLUGIN_FACTORY"
```

Rebuild and reinstall the distribution so its entry-point metadata changes too.
The group name and selection variable are unchanged:

```text
DURABLE_EXECUTION_PLUGINS=example_audit
```

Provider names must be unique across installed distributions. Explicit factories
run before environment-selected factories, and selecting the same factory object
through both paths deduplicates it by identity. Separate factory instances are
separate registrations, even if they construct the same plugin class. See
[dynamic instrumentation plugins](../packages/aws-durable-execution-sdk-python/README.md#dynamic-instrumentation-plugins).

### Migrate OpenTelemetry registrations

Replace plugin construction with the corresponding factory. The optional
`OtelPluginConfig` is still passed to the constructor, now the factory's:

```python
# Core 2.x / OTel 1.x
from aws_durable_execution_sdk_python_otel import InvocationOtelPlugin

@durable_execution(plugins=[InvocationOtelPlugin()])
def handler(event: dict, context: DurableContext) -> str:
    return context.step(lambda _: "ok", name="work")
```

```python
# Core 3.0 / OTel 2.0
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python_otel import InvocationOtelPluginFactory


def workflow(event: dict, context: DurableContext) -> str:
    return context.step(lambda _: "ok", name="work")


handler = durable_execution(workflow, plugins=[InvocationOtelPluginFactory()])
```

Use `ExecutionOtelPluginFactory` instead for the execution-rooted view. Register
only one view across the decorator and environment; combining the two fails
initialization. `DURABLE_EXECUTION_PLUGINS=otel-invocation` and `otel-execution`
keep their names, but must resolve to the new OTel 2 distribution or layer.

The plugin classes remain available for direct plugin tests. They are no longer
valid entries in `plugins`. Shared tracer providers should not be shut down by
an individual invocation's cleanup. See the
[OTel guide](../packages/aws-durable-execution-sdk-python-otel/README.md).

The optional Telemetry Exporter's `durable_execution_telemetry(config)` helper
already returns a factory in this release, so pass its result directly to
`plugins`. Do not wrap or cache a plugin returned by that factory. See the
[Telemetry Exporter guide](../packages/aws-durable-execution-sdk-python-telemetry-exporter/README.md).

### Move registration metadata to the factory

Optional exclusivity metadata belongs on the factory class. Custom factories
can opt in with `__durable_registration_api__ = 1` and `exclusive_group`;
unmarked fields are ignored. The optional `on_registration_result(registered)`
callback concerns registration resources, not invocation finalization. This
registration API is distinct from the removed provider `plugin_api_version`.

### Finding affected code

Search application code, plugin packages, deployment configuration, and tests:

```bash
rg -n 'plugins\s*=|DURABLE_EXECUTION_PLUGINS|aws_durable_execution\.plugins' .
rg -n 'DurableInstrumentationPluginProvider|plugin_api_version|plugin_type' .
rg -n 'InvocationOtelPlugin|ExecutionOtelPlugin|create_plugin' .
rg -n 'aws-durable-execution-sdk-python|Layers:|layer_version_arn' .
```

These searches find registration and packaging candidates. They cannot prove
that plugin state has the correct lifetime.

## Plugin state and invocation lifecycle

Move invocation identity, spans, operation maps, and pending records onto each
new plugin instance. Keep reusable resources such as thread-safe exporters and
connection pools outside it, typically on the factory. Do not return a cached
plugin from `create_plugin`; doing so recreates the shared-state problem.

One durable execution can have many invocations. A suspension, retry, or resume
creates a new instance; instance fields are not checkpointed. Reconstruct
execution-wide information from `InvocationStartInfo.operations` and stable
execution identity. If a factory retains an execution-keyed cache, its owner
must synchronize and prune it; it is neither durable storage nor guaranteed to
survive replacement of the execution environment.

Operation and user-function hooks can run on different threads within one
invocation, so mutable collections and shared resources still need appropriate
synchronization. Invocation Start and End run on the handler's invocation
worker, in registration order. End follows handler unwinding, admitted branch
cleanup, output preparation, and checkpoint shutdown; it is not a reverse-order
context stack. See the [invocation worker lifecycle](../packages/aws-durable-execution-sdk-python/README.md#invocation-worker-lifecycle)
for context-variable handling.

## Recommended validation after upgrading

1. Resolve Core 3 and OTel 2 together, regenerate the lock file, and run
   `python -m pip check` in the built application environment. Verify attached
   provider layers contain the intended versions.
2. Initialize the handler with each explicit and environment-selected factory.
   Confirm that it produces plugin output, not just a successful handler result.
3. Exercise a workflow that suspends and resumes. Confirm that a fresh plugin
   observes each invocation and that completed steps do not repeat their side
   effects. Preserve operation names, order, nesting, and serialization when
   upgrading workflows with in-flight executions.
4. Run concurrent invocations and a workflow with `map` or `parallel`. Check
   that plugin identity, spans, and buffers stay with their own invocation and
   that shared resources tolerate concurrent use.
5. Exercise successful, failed, and retried workflows. Check cleanup and telemetry
   for each outcome; look for `Plugin factory` / `plugin ignored` diagnostics.
6. For OTel, verify spans and log correlation for the chosen view in the target
   environment, including resumed executions. Keep shared providers alive across
   invocations and confirm the deployed function and layers use the same major
   contract before promoting the new function version.
