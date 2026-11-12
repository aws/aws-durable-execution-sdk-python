# AWS Durable Execution SDK for Python: extras

> This package is experimental. Its API can change in any release.

Run jobs from a durable function in AWS Lambda MicroVMs, with the `microvm` and `microvm_session` operations.

## Install

```console
pip install aws-durable-execution-sdk-python-extras
```

## Quick start

```python
import os

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python_extras.compute.microvm import MicrovmConfig, microvm

CONFIG = MicrovmConfig(
    image_identifier=os.environ["MICROVM_IMAGE_ARN"],
    execution_role_arn=os.environ["MICROVM_ROLE_ARN"],
    timeout=Duration.from_hours(1),
    heartbeat_timeout=Duration.from_minutes(5),
)


@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    return microvm(context, "build", job_input={"repo": event["repo"]}, config=CONFIG)
```

`microvm` launches a [Lambda MicroVM](https://docs.aws.amazon.com/lambda/latest/dg/lambda-microvms-guide.html), sends it the job, waits for the result, and terminates the MicroVM. The function invocation ends while the MicroVM works. So no Lambda compute is billed during the wait.

`job_input` and `config` are keyword-only. Inside a child context or a `map` item, pass that context as the first argument.

The image runs the [MicroVM worker](https://github.com/aws/aws-durable-execution-sdk-python/tree/main/packages/aws-durable-execution-sdk-python-compute-worker). The worker receives the job and reports its result.

## Common uses

### Run several jobs in one MicroVM

A session keeps one MicroVM for several jobs. A later job can use the files that an earlier job wrote. The session terminates the MicroVM when its handler returns or raises.

```python
import os
from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python_extras.compute.microvm import (
    MicrovmInvokeOptions,
    MicrovmSession,
    MicrovmSessionConfig,
    microvm_session,
)

SESSION = MicrovmSessionConfig(
    image_identifier=os.environ["MICROVM_IMAGE_ARN"],
    execution_role_arn=os.environ["MICROVM_ROLE_ARN"],
    timeout=Duration.from_hours(1),
)


def pipeline(vm: MicrovmSession, ctx: DurableContext) -> Any:
    build = vm.invoke(
        ctx,
        "build",
        job_input={"repo": "org/app"},
        options=MicrovmInvokeOptions(
            timeout=Duration.from_minutes(15),
            heartbeat_timeout=Duration.from_minutes(2),
        ),
    )
    return vm.invoke(
        ctx,
        "test",
        job_input=build,
        options=MicrovmInvokeOptions(
            timeout=Duration.from_minutes(10),
            heartbeat_timeout=Duration.from_minutes(2),
        ),
    )


@durable_execution
def handler(event: dict, context: DurableContext) -> Any:
    return microvm_session(context, "pipeline", config=SESSION, handler=pipeline)
```

Pass `vm.invoke` the context that the session handler receives, or a context nested in it. Set a `heartbeat_timeout` on every job. A job that still runs when the MicroVM ends then fails at its heartbeat timeout.

### Send a job to a named route

This code runs inside the quick start's `handler`:

```python
import dataclasses

from aws_durable_execution_sdk_python_extras.compute.microvm import MicrovmRequestConfig

config = dataclasses.replace(CONFIG, request=MicrovmRequestConfig(path="/clone-build"))
result = microvm(context, "build", job_input={"repo": "org/app"}, config=config)
```

In a session, set `MicrovmInvokeOptions(path="/clone-build", ...)` instead. Register the same path in the worker's `routes`.

### Handle a failed job

Every MicroVM failure is a `MicrovmError`, and every `MicrovmError` is a `ComputeError`. Catch `ComputeError` for a failure of any compute type, or a subclass for one kind of failure. This code runs inside the quick start's `handler`:

```python
from aws_durable_execution_sdk_python_extras.compute.microvm import (
    MicrovmError,
    MicrovmJobFailedError,
    MicrovmTimeoutError,
)

try:
    result = microvm(context, "build", job_input={"repo": "org/app"}, config=CONFIG)
except MicrovmJobFailedError:
    ...  # The job handler in the MicroVM raised an error.
except MicrovmTimeoutError:
    ...  # No result or no heartbeat in time. A crashed MicroVM ends this way.
except MicrovmError:
    ...  # The launch or the delivery failed.
```

A `microvm` call inside your own child context, `map` item, or `parallel` item reaches the code around it as `ChildContextError`. Its `__cause__` is the MicroVM error.

## Advanced

### Suspend an idle MicroVM

A suspended MicroVM pays only for snapshot storage, and keeps its memory and files. Suspending is off by default.

The SDK never calls `ResumeMicrovm`. The service resumes a suspended MicroVM on the next request, but only if the MicroVM's idle policy sets `autoResumeEnabled`. Setting the idle policy is your job. So every session that suspends must set `idle_policy` with `"autoResumeEnabled": True`. Without it, every job after a suspend fails with `MicrovmDeliveryError`.

```python
SESSION = MicrovmSessionConfig(
    image_identifier=os.environ["MICROVM_IMAGE_ARN"],
    execution_role_arn=os.environ["MICROVM_ROLE_ARN"],
    timeout=Duration.from_hours(1),
    auto_suspend_on_idle=True,
    idle_policy={
        "autoResumeEnabled": True,
        # Above maximumDurationInSeconds: the 1-hour timeout plus 5 minutes.
        "maxIdleDurationSeconds": 4 * 3600,
        "suspendedDurationSeconds": 3600,
    },
)
```

- `auto_suspend_on_idle=True` makes the worker suspend its MicroVM after 60 seconds without a job. `auto_suspend_idle_time` changes the time.
- Set `maxIdleDurationSeconds` above the MicroVM's `maximumDurationInSeconds`, which is the session `timeout` plus 5 minutes. A shorter value can suspend a running job.
- The idle policy needs all three fields. `maxIdleDurationSeconds` must be at least 60.
- Enable the `suspend` and `resume` hooks in the image.

### Suspend after a job

`MicrovmInvokeOptions(suspend_after=True)` makes the durable function suspend the MicroVM after that job. Use it on the job before a long wait, such as a callback. This code runs inside a session handler, such as `pipeline` above:

```python
build = vm.invoke(
    ctx,
    "build",
    job_input=event,
    options=MicrovmInvokeOptions(
        timeout=Duration.from_minutes(15),
        heartbeat_timeout=Duration.from_seconds(30),
        suspend_after=True,
    ),
)
approval = ctx.create_callback(name="approval").result()  # A long wait.
vm.invoke(
    ctx,
    "deploy",
    job_input=build,
    options=MicrovmInvokeOptions(
        timeout=Duration.from_minutes(10),
        heartbeat_timeout=Duration.from_seconds(120),
    ),
)
```

- The session needs the same `idle_policy` with `"autoResumeEnabled": True`.
- The suspend is not a durable operation, and the session adds no guard around it. So parallel operations on the same session are your responsibility. A job that runs next to the suspended job (`map`, `parallel`) freezes with the MicroVM and can reach its heartbeat timeout.
- Give the next job a `heartbeat_timeout` of at least its `retry_window` plus 30 seconds.
- The function's role needs `lambda:SuspendMicrovm` on the image.
- Change `suspend_after` only through a new function version.

### Retries

The operations retry throttling, server errors, and network errors with `default_microvm_retry_strategy`. Set `retry_strategy` in the config to replace it. In a custom strategy, match an AWS error by `error.response["Error"]["Code"]`, not by its message.

### Limits and timeouts

- `timeout` is between 1 second and 8 hours. The MicroVM lives at most `timeout` plus 5 minutes.
- `job_input` must be JSON-serializable and at most `MAX_JOB_INPUT_BYTES` (6 MiB minus 8 KiB) as UTF-8 JSON. Pass larger data by reference, for example as an S3 key.
- A small `microvm` job goes in the `RunMicrovm` request. A large job, a job with `request.path`, and every session job go over HTTP. A job over HTTP needs a function timeout of at least `retry_window` plus 11 seconds: 71 seconds by default.
- `heartbeat_timeout` starts before the launch. So it must cover the launch, the boot, and the delivery.

### Deploys

Invoke the durable function through a published version or an alias, not `$LATEST`. An execution on `$LATEST` replays on new code after a deploy. A launch that retries on new code can fail and leave a MicroVM running until its maximum duration.

### Permissions

The function's execution role needs:

- `lambda:RunMicrovm` and `lambda:TerminateMicrovm` on the MicroVM image.
- `lambda:CreateMicrovmAuthToken` and `lambda:GetMicrovm`, for jobs over HTTP.
- `lambda:PassNetworkConnector` on the network connectors, and `iam:PassRole` on the MicroVM role.
- `lambda:SuspendMicrovm` on the image, for `suspend_after`.

The function's role does not need `lambda:ResumeMicrovm`.

The MicroVM's execution role needs `lambda:SendDurableExecutionCallbackSuccess`, `lambda:SendDurableExecutionCallbackFailure`, and `lambda:SendDurableExecutionCallbackHeartbeat` on the function ARN. With `auto_suspend_on_idle`, it also needs `lambda:SuspendMicrovm` on the image.

## Reference

- [docs/microvm.md](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python-extras/docs/microvm.md): job delivery, the durable operations each call records, all errors, the retry tiers, the state check, every permission, and best practices.
- [MicroVM worker](https://github.com/aws/aws-durable-execution-sdk-python/tree/main/packages/aws-durable-execution-sdk-python-compute-worker): the code that runs in the MicroVM.
- [Source](https://github.com/aws/aws-durable-execution-sdk-python/tree/main/packages/aws-durable-execution-sdk-python-extras/src/aws_durable_execution_sdk_python_extras/compute/microvm): config fields and docstrings.
- [`@aws/durable-execution-sdk-js-extras`](https://github.com/aws/aws-durable-execution-sdk-js/tree/main/packages/aws-durable-execution-sdk-js-extras): the JavaScript counterpart.
- [Lambda MicroVMs guide](https://docs.aws.amazon.com/lambda/latest/dg/lambda-microvms-guide.html).

## License

This project is licensed under the [Apache-2.0 License](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/LICENSE).
