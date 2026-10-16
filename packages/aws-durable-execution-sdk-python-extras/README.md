# AWS Durable Execution SDK for Python: extras

> This package is experimental. Its API can change in any release.

Custom operations for the [AWS Durable Execution SDK for Python](../aws-durable-execution-sdk-python). Each operation composes the SDK's built-in operations and calls AWS APIs. The operations live in this package, so the core SDK does not depend on those APIs' clients.

The package is the Python counterpart of [`@aws/durable-execution-sdk-js-extras`](https://github.com/aws/aws-durable-execution-sdk-js/tree/main/packages/aws-durable-execution-sdk-js-extras).

## `microvm`: run a job in an AWS Lambda MicroVM

`microvm` launches a new [Lambda MicroVM](https://docs.aws.amazon.com/lambda/latest/dg/lambda-microvms-guide.html), delivers the job to it, waits for the job's result, and terminates the MicroVM. The function invocation ends while the MicroVM works, so no Lambda compute is billed during the wait.

```python
import os

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python_extras.microvm import MicrovmConfig, microvm

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

The first argument is the context that the call runs in. Inside a child context or a `map` item, pass that context, so the operation's steps belong to it. `job_input` and `config` are keyword-only. A job can be a string, and so is the name. So positional arguments in the wrong order would pass validation and run the wrong job.

The code in the MicroVM receives the job and reports its result. The worker package [`aws-durable-execution-sdk-python-microvm-worker`](../aws-durable-execution-sdk-python-microvm-worker) does this, and so does the JavaScript worker. The job document is the same in every SDK language.

### How the job reaches the MicroVM

The operation picks the delivery from the input size and the config:

1. A job whose `run` hook payload fits in 4,096 Unicode code points goes in the `RunMicrovm` request. Lambda passes it to the worker's `run` lifecycle hook. The payload also holds the callback ID, so an input of about 3,600 code points fits. The MicroVM gets the `NO_INGRESS` connector, so nothing can reach it.
2. A larger job goes over HTTP. The launch request carries no job, and a request step POSTs the job to the MicroVM's endpoint after it starts. The Python worker accepts a request body of at most 6 MiB. The MicroVM gets the `ALL_INGRESS` connector, and every request carries a short-lived auth token that the operation creates.

The worker package runs its `handler` for both deliveries. So the code in the MicroVM does not change with the input size.

The HTTP delivery waits inside the Lambda invocation until the MicroVM's endpoint accepts the job, usually one to two seconds. The `run` hook delivery adds no Lambda time for the boot.

An application that already serves HTTP routes can receive every job on its own route:

```python
import dataclasses

from aws_durable_execution_sdk_python_extras.microvm import MicrovmRequestConfig

config = dataclasses.replace(CONFIG, request=MicrovmRequestConfig(path="/clone-build"))
microvm(context, "build", job_input=job_input, config=config)
```

A `path` sends every job over HTTP, whatever its size. The route must answer with a 2xx status at once and run the job in the background. `port` names a port other than 8080. `retry_window` sets how long the request step retries inside one attempt, 60 seconds by default.

The same job can arrive again, also after it ended, so ignore a second request for a callback ID that you already ran. The worker package does this. A route served without it must do it itself. A job arrives again in these cases:

1. A request reaches the route, and its answer is lost.
2. The request step stops waiting for a request after at most 10 seconds, but the request keeps running in its thread and reaches the route. The next attempt sends the job again.
3. The invocation ends after the route accepted the job, and before the request step checkpointed its success. The step then runs again in the next invocation.

The path must be an RFC 3986 path without a `.` or `..` segment, and the operation sends it exactly as written. A dot segment is rejected also when the dot is written as `%2e`, because the JavaScript SDK's `fetch` resolves such a segment, and a proxy or server can too. So both SDKs accept the same paths. Percent-encode any other character, for example a space as `%20`. The workers match the raw request path, so register the same string as the worker's route. An invalid path raises `ValidationError` before any durable operation starts.

### What it does

The operation runs its durable operations in one child context named after the second argument:

1. `<name>.callback` creates a durable callback. The service generates the callback ID, and the SDK checkpoints it. So the ID is unique to this call and stays the same on replay.
2. `<name>.launch` calls `RunMicrovm`. The client token is the SHA-256 hex digest of the callback ID. So a retried or replayed launch returns the same MicroVM instead of starting a second one. With `run` hook delivery, the launch request contains the job. The step checkpoints plain JSON, `{"microvmId": "...", "endpoint": "...", "delivery": "http"}`, as the JavaScript operation does. The `endpoint` is present only for HTTP delivery. So a reader of the execution history finds the MicroVM ID there.
3. With HTTP delivery, `<name>.request` creates an auth token and POSTs the job.
4. The child context waits for the MicroVM to complete the callback.
5. `<name>.terminate` calls `TerminateMicrovm`. It runs after success, failure, and timeout alike, and after a failed delivery.

Replay follows the delivery that the launch checkpointed, not a new choice. A deploy between the launch and a replay can change the input or the config. The launch result comes from the checkpoint, so the job still goes the way the MicroVM was launched for.

A terminate failure does not fail the operation. The job's outcome is already recorded at that point. So the operation logs an error instead of raising. The MicroVM then keeps running, and is billed, until the platform ends it at its maximum duration: the operation timeout plus 5 minutes, at most 8 hours.

The terminate step runs only when the operation sees the job's outcome. In these cases it never does, and the MicroVM also runs until its maximum duration:

- A `map` or `parallel` completes early, for example with `min_successful`, while an item still waits for its MicroVM. The SDK never runs that item again.
- The execution fails, times out, or is stopped while the operation waits.
- Every launch attempt timed out or lost its connection, but the service created the MicroVM anyway. The operation never learned the MicroVM's ID.

### Errors

Every MicroVM failure is a `MicrovmError`. Catch the base class for all of them, or a subclass for one kind:

| Error | When |
|---|---|
| `MicrovmLaunchError` | `RunMicrovm` failed after all retries. When the last attempt timed out or lost its connection, the service may have created the MicroVM anyway, and it then runs the job. So running the job again after this error can run it twice. |
| `MicrovmDeliveryError` | HTTP delivery failed after all retries, or the route answered a 4xx status such as 404, or a redirect. |
| `MicrovmNotRunningError` | A session job found its MicroVM terminating, terminated, or removed. It is a `MicrovmDeliveryError`. See [Suspending an idle MicroVM](#suspending-an-idle-microvm). |
| `MicrovmJobFailedError` | The job handler in the MicroVM failed, or the MicroVM sent a result that is not valid JSON. The message includes the error type that the MicroVM reported. |
| `MicrovmTimeoutError` | No result arrived within `timeout`, or no heartbeat within `heartbeat_timeout`. A MicroVM that crashed ends this way. |

```python
from aws_durable_execution_sdk_python_extras.microvm import (
    MicrovmError,
    MicrovmTimeoutError,
)

try:
    result = microvm(context, "build", job_input=job_input, config=CONFIG)
except MicrovmTimeoutError:
    ...  # The MicroVM crashed, or took too long.
except MicrovmError:
    ...  # Any other MicroVM failure.
```

The class is the same on the first run and on every replay, so code that branches on it stays deterministic. A `microvm` call inside your own child context, or inside a `map` or `parallel` item, reaches the code around that context as the SDK's `ChildContextError`. Its `__cause__` is the MicroVM error, so check `isinstance(error.__cause__, MicrovmTimeoutError)` there. An invalid name or config raises the SDK's `ValidationError`, and an input that is not JSON-serializable raises `TypeError`, before any durable operation starts. `Duration` raises the same `ValidationError` for a negative value.

When the error escapes the handler, the execution's error type is the class's fully-qualified name, such as `aws_durable_execution_sdk_python_extras.microvm.errors.MicrovmJobFailedError`. The SDK uses that name to rebuild the class on replay. The JavaScript operation reports the short name, `MicrovmJobFailedError`. So a rule that matches the error type of both SDKs, such as a Step Functions `Catch` or a metric filter, must match both forms. For example, match on the suffix.

### Retries in two tiers

A step retry costs one durable operation and one more Lambda invocation. Most failures here are short, so the steps retry in two tiers:

1. **Inside one step attempt.** The boto3 client retries throttling and server errors of `RunMicrovm`, `CreateMicrovmAuthToken`, and `TerminateMicrovm`. The request step retries the POST itself: connection errors, timeouts, 429, and 5xx, with a delay from 250 milliseconds to 4 seconds. It retries one 401 or 403 with a new auth token. The endpoint answers this way until the MicroVM's `run` hook returns. This tier ends after `retry_window`, or 10 seconds before the invocation times out, whichever comes first.
2. **The step retry strategy.** When the first tier gives up, the step retries after a longer delay, outside the invocation. `default_microvm_retry_strategy` is the default, unless the config sets `retry_strategy`. It retries throttling, server errors, connection failures, timeouts, and the end of the first tier, for at most 5 attempts, with a delay of 2, 4, 8, and 16 seconds. Every other error, such as `ValidationException`, `AccessDeniedException`, or `InsufficientCapacityException`, fails the step at once.

A route that answers another 4xx status fails the operation without a retry, with `MicrovmDeliveryError`. So does a redirect, 3xx. The operation does not follow redirects, and the message names the `Location`. Set the path that the route serves, for example with its trailing slash. The JavaScript operation follows redirects.

The POST ignores the proxy settings in the environment, as the JavaScript SDK's `fetch` does. boto3 reads `HTTPS_PROXY`. So in a network that reaches the MicroVM endpoint only through a proxy, the launch succeeds and every POST fails. Pass an `opener` in the config for that network, for example `urllib.request.build_opener(urllib.request.ProxyHandler({"https": "http://proxy.internal:3128"}))`. When an opener follows a redirect, the delivery still fails at once with `MicrovmDeliveryError`. The followed request carries no auth token, because the operation adds its headers as unredirected headers.

The 10-second reserve keeps time for the step to record its failure. A Lambda timeout during a step attempt records no outcome for the attempt. So every call of the first tier must end before the reserve. boto3 cannot cancel a call, so a `CreateMicrovmAuthToken` call runs in a separate thread, and the step stops waiting for it at the reserve. Each POST runs in a separate thread too, and the step waits for it at most 10 seconds, or until the reserve. A socket timeout alone would not bound it, because each byte received starts the socket timeout again.

### Defaults and limits

- `timeout` must be between 1 second and 8 hours, because a MicroVM lives at most 8 hours. The MicroVM's `maximumDurationInSeconds` is `timeout` plus 5 minutes, at most 8 hours. So a timeout above 7 hours 55 minutes leaves the MicroVM less than 5 minutes beyond the callback's timeout.
- `job_input` is sent as JSON. So the MicroVM receives what `json.dumps` writes: a tuple arrives as a list, and a dict key that is not a string arrives as a string.
- The MicroVM gets the `INTERNET_EGRESS` connector, because it needs egress to reach the Lambda callback APIs. For ingress, it gets `NO_INGRESS` with `run` hook delivery and `ALL_INGRESS` with HTTP delivery. If you set `ingress_network_connectors`, allow ingress whenever an input can exceed the `run` hook limit. An empty tuple sends no connector.
- `heartbeat_timeout` starts when the callback is created, before the launch. The worker sends its first heartbeat when the job arrives. So the first period must cover the launch, the boot, and, for HTTP delivery, the request step, each with its retries. A throttled launch can take several minutes, and the request step retries for up to `retry_window` in each attempt. With a shorter heartbeat timeout, the operation fails with `MicrovmTimeoutError` before the MicroVM sends its first heartbeat.
- The request step needs at least 11 seconds left in the invocation when it starts. It keeps a 10-second reserve before the invocation times out, and it starts a call only with at least 1 second left before the reserve. The launch step can run earlier in the same invocation, so the function timeout must cover it too. A function timeout of `retry_window` plus 11 seconds, 71 seconds by default, leaves the first retry tier about its whole window. With less than 11 seconds left, every attempt fails with `MicrovmEndpointUnavailableError`, and the message of the final `MicrovmDeliveryError` names the function timeout. The Lambda default timeout is 3 seconds. With it, `microvm` runs small jobs in the `run` hook and fails every job that goes over HTTP: a job too large for the hook, and a job with `request.path`.
- The Region comes from the durable execution ARN, or from `AWS_REGION`.
- Without a `client`, the operation creates one `lambda-microvms` client per Region, on first use. It has a 5-second connect timeout, a 30-second read timeout, and 3 attempts in botocore's standard retry mode. So one launch or terminate attempt takes at most about 2 minutes. A call that runs past the function's timeout records no outcome for its step attempt, and the step runs again. The launch is idempotent through its client token, so it is repeated, not doubled.

### Permissions

The function's execution role needs:

- `lambda:RunMicrovm` and `lambda:TerminateMicrovm` on the MicroVM image. Without `lambda:TerminateMicrovm`, every operation still returns its result, but every MicroVM runs until its timeout plus 5 minutes, at most 8 hours. The only sign is an error in the function's log.
- `lambda:CreateMicrovmAuthToken`, for HTTP delivery. Grant it unless every input is small and no config sets a `path`.
- `lambda:PassNetworkConnector` on the connectors that the MicroVM uses. `RunMicrovm` authorizes each connector separately. With the defaults, that is `arn:aws:lambda:<region>:aws:network-connector:aws-network-connector:*`.
- `iam:PassRole` on the MicroVM execution role.

The MicroVM execution role (`execution_role_arn`) needs `lambda:SendDurableExecutionCallbackSuccess`, `lambda:SendDurableExecutionCallbackFailure`, and `lambda:SendDurableExecutionCallbackHeartbeat` on the function ARN (`arn:aws:lambda:<region>:<account>:function:<name>:*`).

## `microvm_session`: run several jobs in one MicroVM

`microvm_session` launches one MicroVM and runs a handler that sends jobs to it. The jobs share the MicroVM's files and processes. So a later job can use what an earlier job wrote. The MicroVM is terminated when the handler returns or raises.

```python
import os
from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python_extras.microvm import (
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
        "clone-build",
        job_input={"repo": "org/app"},
        options=MicrovmInvokeOptions(
            path="/clone-build",
            timeout=Duration.from_minutes(15),
            heartbeat_timeout=Duration.from_minutes(2),
        ),
    )
    ctx.wait(Duration.from_minutes(5), name="cool-down")
    return vm.invoke(
        ctx,
        "test",
        job_input=build,
        options=MicrovmInvokeOptions(timeout=Duration.from_minutes(10)),
    )


@durable_execution
def handler(event: dict, context: DurableContext) -> Any:
    return microvm_session(context, "pipeline", config=SESSION, handler=pipeline)
```

The operation runs its durable operations in one child context named `name`:

```
pipeline                       child context
├── pipeline.session           step: generates the session ID
├── pipeline.launch            step: RunMicrovm
├── clone-build                child context, one per job
│   ├── clone-build.callback   callback
│   └── clone-build.request    step: POSTs the job
├── cool-down                  wait
├── test
│   ├── test.callback
│   └── test.request
└── pipeline.terminate         step: TerminateMicrovm
```

- The client token of the launch is the SHA-256 hex digest of the session ID. The session ID is random, and it is checkpointed. So a retried or replayed launch returns the same MicroVM.
- Every job goes over HTTP. The MicroVM gets the `ALL_INGRESS` connector. A job without a `path` goes to the worker's `handler`.
- The first job is sent right after the launch. The MicroVM endpoint refuses requests until the `run` hook returns, and the request step retries a refused request inside its attempt. So the session needs no separate readiness wait.
- `vm.invoke` takes the context that the call runs in, as `microvm` does. Inside a `map` or `parallel` item, pass the item's context. A job must be created inside the session's child context. A job created in the context that `microvm_session` was called with runs, and the next replay then fails the execution with `NonDeterministicExecutionError`. So `vm.invoke` raises `TypeError` for that context. The core SDK does not check other contexts outside the session.
- `config.timeout` bounds the MicroVM's lifetime: it ends at `timeout` plus 5 minutes, at most 8 hours. Each job waits for its own `timeout`. So set a `heartbeat_timeout` on every job. A job still running when the MicroVM ends then fails at its heartbeat timeout.

A failed `vm.invoke` raises its MicroVM error inside the handler. The handler can catch it and send another job to the same MicroVM. An error that leaves the handler ends the session:

1. A MicroVM error that the handler raises itself reaches the caller with its own class, such as `MicrovmJobFailedError`.
2. Any other error reaches the caller as the SDK's `ChildContextError`, as from `run_in_child_context`. This includes a MicroVM error that leaves a child context that the handler created: that child context records `ChildContextError` as its error type. The caller then gets the MicroVM error's message, but not its class. Inside the handler, the `ChildContextError` of that child context has the MicroVM error as its `__cause__`. An SDK error that leaves the handler, such as the `StepError` of a failed step, also reaches the caller as `ChildContextError`, with the SDK error as its `__cause__`. This rule matches `run_in_child_context`. The JavaScript operation differs here: it passes an SDK error to the caller with its own class.

An invalid job raises `ValidationError` or `TypeError` from `vm.invoke`, before the job's durable operations start. The handler can catch it too.

The handler's return value is checkpointed as the session's result. So it must be serializable by the SDK, and it counts toward the checkpoint size limit.

### Suspending an idle MicroVM

A running MicroVM pays compute charges. A suspended MicroVM pays only for snapshot storage, and it keeps its memory and files. A session can suspend its MicroVM between jobs in two ways:

1. `auto_suspend_on_idle=True` asks the worker in the MicroVM to suspend its own MicroVM when no job has run for `auto_suspend_idle_time`. The default idle time is 60 seconds, or the session `timeout` if that is shorter. The idle time must be at least 10 seconds.
2. `idle_policy` sets the `RunMicrovm` idle policy. The service then suspends the MicroVM when its endpoint receives no traffic. A running job receives no traffic after its delivery. So `maxIdleDurationSeconds` must be longer than the longest job.

With either setting, each request step calls `GetMicrovm` before it sends the job. It resumes a suspended MicroVM, and it sends the job to the endpoint that `GetMicrovm` reports. The MicroVM can suspend itself between that check and the request. The endpoint then answers 502, or the worker answers 503. So after a 502, 503, or 504, or a request without an answer, the step checks the state again before it retries. The wait for a resume counts toward the request's `retry_window`.

A MicroVM that is terminating, terminated, or removed fails the job at once with `MicrovmNotRunningError`, a subclass of `MicrovmDeliveryError`. A retry finds the same state, so the job is not retried. Without either setting, the session makes no `GetMicrovm` call. A terminated MicroVM's endpoint answers 502, and the request step retries a 502 as a MicroVM that is still starting. So the job fails with a plain `MicrovmDeliveryError`, after the retries of both tiers.

Suspending is off by default, because it needs more permissions and changes what the MicroVM does between jobs:

- The function's role needs `lambda:GetMicrovm` and `lambda:ResumeMicrovm`. Without them, every job fails.
- With `auto_suspend_on_idle`, the MicroVM's role needs `lambda:SuspendMicrovm` on the image. That permission lets code in any MicroVM of the image suspend any other. So use one image per trust boundary.
- Background processes that an earlier job started stop while the MicroVM is suspended. The worker counts only its running jobs.

The worker must support `auto_suspend_on_idle`. The JavaScript worker [`@aws/durable-execution-sdk-js-microvm-worker`](https://github.com/aws/aws-durable-execution-sdk-js/tree/main/packages/aws-durable-execution-sdk-js-microvm-worker) and the Python worker [`aws-durable-execution-sdk-python-microvm-worker`](../aws-durable-execution-sdk-python-microvm-worker) both do. `idle_policy` works with both workers. Enable the `suspend` and `resume` hooks in the image. The worker then refuses jobs while the MicroVM freezes, and accepts them again as soon as it resumes. Without the `resume` hook, the first job after a resume can wait a little over 30 seconds, until the worker's refusal ends and the request step delivers again.

### Session permissions

A session needs the permissions of `microvm` with HTTP delivery: `lambda:RunMicrovm`, `lambda:TerminateMicrovm`, `lambda:CreateMicrovmAuthToken`, `lambda:PassNetworkConnector`, and `iam:PassRole`. Suspending adds the permissions listed above.

### Subtypes

The JavaScript operations record MicroVM subtypes, such as `Microvm` on the child context, `MicrovmSession` on a session, and `MicrovmLaunch` on the launch step. This package records the SDK's default subtypes for now, because the Python SDK does not accept custom subtypes yet.

On replay, the SDK compares the subtype of each operation with its checkpoint. A mismatch fails the execution with `NonDeterministicExecutionError`. So a release that changed the subtypes would fail every execution that waits on a MicroVM during the deploy. So this package is published only after the SDK accepts custom subtypes, and it records the MicroVM subtypes from its first release.
