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
