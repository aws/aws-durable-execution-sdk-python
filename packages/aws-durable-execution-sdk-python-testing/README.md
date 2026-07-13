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

See [docs/architecture.md](docs/architecture.md) for framework
internals. It covers the components, the worker model, the checkpoint
flow, pagination, and a map of the code.


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
