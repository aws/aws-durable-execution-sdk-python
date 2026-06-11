# AWS Durable Execution Testing SDK for Python

[![PyPI - Version](https://img.shields.io/pypi/v/aws-durable-execution-sdk-python-testing.svg)](https://pypi.org/project/aws-durable-execution-sdk-python-testing)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/aws-durable-execution-sdk-python-testing.svg)](https://pypi.org/project/aws-durable-execution-sdk-python-testing)


[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/aws/aws-durable-execution-sdk-python-testing/badge)](https://scorecard.dev/viewer/?uri=github.com/aws/aws-durable-execution-sdk-python-testing)

-----

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Documentation](#documentation)
- [Developer Guide](#developers)
- [License](#license)

## Installation

```console
pip install aws-durable-execution-sdk-python-testing
```

## Overview

Use the AWS Durable Execution Testing SDK for Python to test your Python durable functions locally.

The test framework contains a local runner, so you can run and test your durable function locally
before you deploy it.

## Quick Start

### A durable function under test

```python
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_step,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.config import Duration


@durable_step
def one(a: int, b: int) -> str:
    return f"{a} {b}"

@durable_step
def two_1(a: int, b: int) -> str:
    return f"{a} {b}"

@durable_step
def two_2(a: int, b: int) -> str:
    return f"{b} {a}"

@durable_with_child_context
def two(ctx: DurableContext, a: int, b: int) -> str:
    two_1_result: str = ctx.step(two_1(a, b))
    two_2_result: str = ctx.step(two_2(a, b))
    return f"{two_1_result} {two_2_result}"

@durable_step
def three(a: int, b: int) -> str:
    return f"{a} {b}"

@durable_execution
def function_under_test(event: Any, context: DurableContext) -> list[str]:
    results: list[str] = []

    result_one: str = context.step(one(1, 2))
    results.append(result_one)

    context.wait(Duration.from_seconds(1))

    result_two: str = context.run_in_child_context(two(3, 4))
    results.append(result_two)

    result_three: str = context.step(three(5, 6))
    results.append(result_three)

    return results
```

### Your test code

```python
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python_testing.runner import (
    ContextOperation,
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
    StepOperation,
)

def test_my_durable_functions():
    with DurableFunctionTestRunner(handler=function_under_test) as runner:
        result: DurableFunctionTestResult = runner.run(input="input str", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == '["1 2", "3 4 4 3", "5 6"]'

    one_result: StepOperation = result.get_step("one")
    assert one_result.result == '"1 2"'

    two_result: ContextOperation = result.get_context("two")
    assert two_result.result == '"3 4 4 3"'

    three_result: StepOperation = result.get_step("three")
    assert three_result.result == '"5 6"'
```
## Architecture
![Durable Functions Python Test Framework Architecture](assets/dar-python-test-framework-architecture.svg)

## Event Flow
![Event Flow Sequence Diagram](assets/dar-python-test-framework-event-flow.svg)

1. **DurableTestRunner** starts execution via **Executor**
2. **Executor** creates **Execution** and schedules initial invocation
3. During execution, checkpoints are processed by **CheckpointProcessor**
4. **Individual Processors** transform operation updates and may trigger events
5. **ExecutionNotifier** broadcasts events to **Executor** (observer)
6. **Executor** updates **Execution** state based on events
7. **Execution** completion triggers final event notifications
8. **DurableTestRunner** run() blocks until it receives completion event, and then returns `DurableFunctionTestResult`.

## Major Components

### Core Execution Flow
- **DurableTestRunner** - Main entry point that orchestrates test execution
- **Executor** - Manages execution lifecycle. Mutates Execution.
- **Execution** - Represents the state and operations of a single durable execution

### Service Client Integration
- **InMemoryServiceClient** - Replaces AWS Lambda service client for local testing. Injected into SDK via `DurableExecutionInvocationInputWithClient`

### Checkpoint Processing Pipeline
- **CheckpointProcessor** - Orchestrates operation transformations and validation
- **Individual Validators** - Validate operation updates and state transitions
- **Individual Processors** - Transform operation updates into operations (step, wait, callback, context, execution)

### Execution status changes (Observer Pattern)
- **ExecutionNotifier** - Notifies observers of execution events
- **ExecutionObserver** - Interface for receiving execution lifecycle events
- **Executor** implements `ExecutionObserver` to handle completion events

## Component Relationships

### 1. DurableTestRunner → Executor → Execution
- **DurableTestRunner** serves as the main API entry point and sets up all components
- **Executor** manages the execution lifecycle, handling invocations and state transitions
- **Execution** maintains the state of operations and completion status

### 2. Service Client Injection
- **DurableTestRunner** creates **InMemoryServiceClient** with **CheckpointProcessor**
- **InProcessInvoker** injects the service client into SDK via `DurableExecutionInvocationInputWithClient`
- When durable functions call checkpoint operations, they're intercepted by **InMemoryServiceClient**
- **InMemoryServiceClient** delegates to **CheckpointProcessor** for local processing

### 3. CheckpointProcessor → Individual Validators → Individual Processors
- **CheckpointProcessor** orchestrates the checkpoint processing pipeline
- **Individual Validators** (CheckpointValidator, TransitionsValidator, and operation-specific validators) ensure operation updates are valid
- **Individual Processors** (StepProcessor, WaitProcessor, etc.) transform `OperationUpdate` into `Operation`

### 4. Observer Pattern Flow
The observer pattern enables loose coupling between checkpoint processing and execution management:

1. **CheckpointProcessor** processes operation updates
2. **Individual Processors** detect state changes (completion, failures, timer scheduling)
3. **ExecutionNotifier** broadcasts events to registered observers
4. **Executor** (as ExecutionObserver) receives notifications and updates **Execution** state
5. **Execution** complete_* methods finalize the execution state


## Documentation

### Error Handling

The testing framework implements AWS-compliant error responses that match the exact format expected by boto3 and AWS services. For detailed information about error response formats, exception types, and troubleshooting, see:

- [Error Response Documentation](docs/error-responses.md)

Key features:
- **AWS-compliant JSON format**: Matches boto3 expectations exactly
- **Smithy model compliance**: Field names follow AWS Smithy definitions  
- **HTTP status code mapping**: Standard AWS service status codes
- **Boto3 compatibility**: Seamless integration with boto3 error handling

## Developers
Please see [CONTRIBUTING.md](CONTRIBUTING.md). It contains the testing guide, sample commands and instructions
for how to contribute to this package.

tldr; use `hatch` and it will manage virtual envs and dependencies for you, so you don't have to do it manually.

## License

This project is licensed under the [Apache-2.0 License](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/LICENSE).
