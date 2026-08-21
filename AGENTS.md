# AWS Durable Execution SDK for Python - Agent Guide

This repository contains the AWS Durable Execution SDK for Python and its
companion packages, used to author AWS Lambda durable functions.

## Use the developer guide, not this file, for APIs

Do not rely on this file for method signatures, configuration objects, or
code examples. The canonical reference is the
[AWS Durable Execution SDK Developer Guide](https://docs.aws.amazon.com/durable-execution/),
which is maintained alongside SDK releases and covers TypeScript, Python,
and Java. Key sections:

| Topic | Link |
| --- | --- |
| Key concepts and quickstart | <https://docs.aws.amazon.com/durable-execution/getting-started/key-concepts/> |
| Steps | <https://docs.aws.amazon.com/durable-execution/sdk-reference/operations/step/> |
| Waits | <https://docs.aws.amazon.com/durable-execution/sdk-reference/operations/wait/> |
| Wait for condition (polling) | <https://docs.aws.amazon.com/durable-execution/sdk-reference/operations/wait-for-condition/> |
| Callbacks and wait for callback | <https://docs.aws.amazon.com/durable-execution/sdk-reference/operations/callback/> |
| Invoke (function chaining) | <https://docs.aws.amazon.com/durable-execution/sdk-reference/operations/invoke/> |
| Parallel | <https://docs.aws.amazon.com/durable-execution/sdk-reference/operations/parallel/> |
| Map | <https://docs.aws.amazon.com/durable-execution/sdk-reference/operations/map/> |
| Child contexts | <https://docs.aws.amazon.com/durable-execution/sdk-reference/operations/child-context/> |
| Errors and retries | <https://docs.aws.amazon.com/durable-execution/sdk-reference/error-handling/errors/> |
| Serialization | <https://docs.aws.amazon.com/durable-execution/sdk-reference/state/serialization/> |
| Logging and plugins | <https://docs.aws.amazon.com/durable-execution/sdk-reference/observability/logging/> |
| Python language guide | <https://docs.aws.amazon.com/durable-execution/sdk-reference/languages/python/> |
| Testing (local runner, assertions) | <https://docs.aws.amazon.com/durable-execution/testing/> |
| Best practices (determinism, idempotency, state) | <https://docs.aws.amazon.com/durable-execution/patterns/best-practices/> |

For Lambda service topics such as deployment, infrastructure as code,
invocation, IAM permissions, and quotas, see the
[Lambda durable functions guide](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html).

## Critical rules: the replay model

Durable functions use checkpoint and replay. After a wait, failure, or
resume, code re-runs from the beginning. Completed steps return their
checkpointed results without re-executing, and code outside steps runs
again on every replay. This implies four rules:

1. **Code outside steps must be deterministic.** Wrap timestamps, random
   values, UUID generation, API calls, and any other non-deterministic
   work in a step.
2. **Never call durable operations inside a step.** Use a child context
   to group operations.
3. **Closure mutations inside steps are lost on replay.** Return values
   from steps instead of mutating enclosing scope.
4. **Side effects outside steps repeat on every replay.** Put side effects
   in steps. `context.logger` is the exception: it is replay-aware and
   safe anywhere.

See [Determinism and Replay](https://docs.aws.amazon.com/durable-execution/patterns/best-practices/determinism/)
for worked examples.

## Security-sensitive parsing

- Do not use `yaml.load()` or `yaml.load_all()`, including with `Loader=`
  arguments or suppression comments. CodeQL and security scanners flag the
  unsafe call form even when the loader subclasses `yaml.SafeLoader`.
- Use `yaml.safe_load()` or `yaml.safe_load_all()` for plain YAML.
- For CloudFormation or SAM templates with short-form tags such as `!Ref`,
  `!Sub`, `!GetAtt`, or `!If`, use a `yaml.SafeLoader` subclass with explicit
  tag constructors so the tags are preserved.
- For custom safe loaders, instantiate the loader directly, call
  `get_single_data()` or the equivalent multi-document API, and call
  `dispose()` in a `finally` block. This matches what `yaml.safe_load()` does
  internally while preserving custom tag support.
- Before finishing YAML-related changes, check Python code for unsafe call
  forms:
  `rg -n --glob '*.py' "yaml\\.load\\b|yaml\\.load_all\\b|from yaml import load\\b|from yaml import load_all\\b" .`

## Testing Requirements

All changes MUST include related tests. At minimum, include **unit tests**. Include **e2e integration tests** (in the `tests/e2e/` directory) when the change affects cross-component behavior, public API surfaces, or end-to-end workflows. For isolated bug fixes where a unit test alone sufficiently covers the fix, integration tests are not required.

Do NOT add or modify **conformance tests** without coordinating with the team. Conformance test requirements and the runner live in a separate repository ([aws-durable-execution-conformance-tests](https://github.com/aws/aws-durable-execution-conformance-tests)). If a change warrants a new conformance test, note it in the PR description or [open an issue](https://github.com/aws/aws-durable-execution-conformance-tests/issues/new?template=new_requirement.yml) in that repository.

## Working in this repository

- Packages live under `packages/`: the core SDK
  (`aws-durable-execution-sdk-python`), the testing library
  (`aws-durable-execution-sdk-python-testing`), the OpenTelemetry plugin
  (`aws-durable-execution-sdk-python-otel`), and examples
  (`aws-durable-execution-sdk-python-examples`).
- Read [CONTRIBUTING.md](CONTRIBUTING.md) before making changes. Use
  `hatch` for all tests, type checks, and formatting (for example
  `hatch run dev-core:test`, `hatch run dev-core:typecheck`, and
  `hatch fmt --check` from a package directory).
- When the developer guide and the installed SDK source disagree, trust
  the source in this repository and report the discrepancy.
