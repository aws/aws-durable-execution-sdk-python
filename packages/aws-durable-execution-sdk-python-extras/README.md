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

1. A `microvm` job goes in the `RunMicrovm` request when its `run` hook payload fits in 4,096 Unicode code points and its config sets no `request.path`. The payload holds the input, the callback ID, and a few other fields. Lambda passes the payload to the worker's `run` lifecycle hook. The MicroVM gets the `NO_INGRESS` connector, so nothing can reach it.
2. Every other job goes over HTTP:
   - A `microvm` job whose `run` hook payload is over 4,096 code points.
   - A `microvm` job whose config sets `request.path`, whatever its size.
   - Every session job.

   The launch request carries no job, and a request step POSTs the job to the MicroVM's endpoint after it starts. The Python worker accepts a request body of at most 6 MiB. The MicroVM gets the `ALL_INGRESS` connector, and every request carries a short-lived auth token that the operation creates.

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
- A launch attempt that timed out or lost its connection can still have created a MicroVM. When the launch step still fails, the operation never learns that MicroVM's ID. So it cannot terminate that MicroVM.

### Errors

Every MicroVM failure is a `MicrovmError`. Catch the base class for all of them, or a subclass for one kind:

| Error | When |
|---|---|
| `MicrovmLaunchError` | `RunMicrovm` failed after all retries. A MicroVM can still run the job. The list below the table says when. |
| `MicrovmDeliveryError` | HTTP delivery failed after all retries, or the route answered a 4xx status such as 404, or a redirect. |
| `MicrovmNotRunningError` | A `MicrovmDeliveryError`: the MicroVM is terminating, terminated, or removed. It is not retried. A session that checks the MicroVM state before each job reports it. Any job over HTTP also reports it when `CreateMicrovmAuthToken` reports the MicroVM removed. See [Suspending an idle MicroVM](#suspending-an-idle-microvm). |
| `MicrovmJobFailedError` | The job handler in the MicroVM failed, or the MicroVM sent a result that is not valid JSON. The message includes the error type that the MicroVM reported. |
| `MicrovmTimeoutError` | No result arrived within `timeout`, or no heartbeat within `heartbeat_timeout`. A MicroVM that crashed ends this way. |

After `MicrovmLaunchError`, the delivery decides whether a MicroVM runs the job:

1. With `run` hook delivery, the launch request contains the job. A launch attempt that timed out or lost its connection can still have created a MicroVM. That MicroVM then runs the job. So running the job again after this error can run it twice.
2. With HTTP delivery, the launch request contains no job. The request step starts only after a successful launch. So when you invoke through a version or an alias, no MicroVM runs the job after an HTTP launch fails.

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

A failed AWS call keeps its request ID in the error message, in the form `<Operation> request ID: <id>.`, for example `RunMicrovm request ID: <id>.`. A support case can quote it. This applies to `RunMicrovm`, `GetMicrovm`, `ResumeMicrovm`, `CreateMicrovmAuthToken`, and `TerminateMicrovm`. The SDK checkpoints the message of a failed step, and not its `__cause__`. So the request ID is in the message, the handler sees it on the first run and on replay, and the execution history keeps it. A failed `TerminateMicrovm` call does not fail the operation. So a `TerminateMicrovm` request ID appears only in the terminate log entry and in the terminate step record. A message has no request ID when the answer carries none, for example after a connection failure. A message also has no request ID when the error is not the answer of the Lambda MicroVMs call. For example, a `client` with role-assuming credentials calls STS `AssumeRole` inside each call, and an STS error carries the request ID of STS. The operation recognizes such an error by its `operation_name`. The step raises a copy of the botocore error, of the same class, with the same `response`, and with the request ID in its message. So `default_microvm_retry_strategy` decides as before. A custom `retry_strategy` receives the error that botocore raised, without the request ID. So a strategy that matches the message, such as `create_retry_strategy(RetryStrategyConfig(retryable_errors=[...]))`, decides on the message of the service. A `ResourceNotFoundException` from `GetMicrovm`, `ResumeMicrovm`, or `CreateMicrovmAuthToken` is the exception. It means that the MicroVM no longer exists. So the operation raises `MicrovmStateError` instead, and the strategy receives that error. Its `__cause__` is the botocore error. An error that the operation creates itself, such as `MicrovmStateError` or "RunMicrovm returned no microvmId", has the request ID in its message when the strategy receives it. A custom client can raise one error object for several calls. Each step message then names only its own call, and the object keeps its message.

When the error escapes the handler, the execution's error type is the class's fully-qualified name, such as `aws_durable_execution_sdk_python_extras.microvm.errors.MicrovmJobFailedError`. The SDK uses that name to rebuild the class on replay. The JavaScript operation reports the short name, `MicrovmJobFailedError`. So a rule that matches the error type of both SDKs, such as a Step Functions `Catch` or a metric filter, must match both forms. For example, match on the suffix.

### Retries in two tiers

A step retry costs one durable operation and one more Lambda invocation. Most failures here are short, so the steps retry in two tiers:

1. **Inside one step attempt.** The boto3 client retries throttling and server errors of `RunMicrovm`, `CreateMicrovmAuthToken`, and `TerminateMicrovm`. The request step retries the POST itself: connection errors, timeouts, 429, and 5xx, with a delay from 250 milliseconds to 4 seconds. It retries one 401 or 403 with a new auth token. The endpoint answers this way until the MicroVM's `run` hook returns. This tier ends after `retry_window`, or 10 seconds before the invocation times out, whichever comes first.
2. **The step retry strategy.** When the first tier gives up, the step retries after a longer delay, outside the invocation. `default_microvm_retry_strategy` is the default, unless the config sets `retry_strategy`. It retries throttling, server errors, connection failures, timeouts, and the end of the first tier, for at most 5 attempts, with a delay of 2, 4, 8, and 16 seconds. Every other error, such as `ValidationException`, `AccessDeniedException`, or `InsufficientCapacityException`, fails the step at once.

A route that answers another 4xx status fails the operation without a retry, with `MicrovmDeliveryError`. So does a redirect, 3xx. The operation does not follow redirects. The execution history stores the error message. A query in the `Location` can hold a secret. So the message names the path, and the status of a 3xx answer. It omits the `Location`, and the URL that a followed redirect reached. Set the path that the route serves, for example with its trailing slash. The JavaScript operation also fails every 3xx at once, with `MicrovmDeliveryError`.

The POST ignores the proxy settings in the environment, as the JavaScript SDK's `fetch` does. boto3 reads `HTTPS_PROXY`. So in a network that reaches the MicroVM endpoint only through a proxy, the launch succeeds and every POST fails. Pass an `opener` in the config for that network, for example `urllib.request.build_opener(urllib.request.ProxyHandler({"https": "http://proxy.internal:3128"}))`. When an opener follows a redirect, the delivery still fails at once with `MicrovmDeliveryError`. The followed request carries no auth token, because the operation adds its headers as unredirected headers.

The 10-second reserve keeps time for the step to record its failure. A Lambda timeout during a step attempt records no outcome for the attempt. So every call of the first tier must end before the reserve. boto3 cannot cancel a call, so a `CreateMicrovmAuthToken` call runs in a separate thread, and the step stops waiting for it at the reserve. Each POST runs in a separate thread too, and the step waits for it at most 10 seconds, or until the reserve. A socket timeout alone would not bound it, because each byte received starts the socket timeout again.

### Defaults and limits

- `timeout` must be between 1 second and 8 hours, because a MicroVM lives at most 8 hours. The MicroVM's `maximumDurationInSeconds` is `timeout` plus 5 minutes, at most 8 hours. So a timeout above 7 hours 55 minutes leaves the MicroVM less than 5 minutes beyond the callback's timeout.
- `job_input` is sent as JSON. So the MicroVM receives what `json.dumps` writes: a tuple arrives as a list, and a dict key that is not a string arrives as a string.
- The MicroVM gets the `INTERNET_EGRESS` connector, because it needs egress to reach the Lambda callback APIs. For ingress, it gets `NO_INGRESS` with `run` hook delivery and `ALL_INGRESS` with HTTP delivery. If you set `ingress_network_connectors`, allow ingress for every job that goes over HTTP. A `microvm` job goes over HTTP when its `run` hook payload is over 4,096 code points, or when its config sets `request.path`. Every session job goes over HTTP. An empty tuple sends no connector.
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

1. `auto_suspend_on_idle=True` asks the worker in the MicroVM to suspend its own MicroVM when no job has run for `auto_suspend_idle_time`. The default idle time is 60 seconds, or the session `timeout` if that is shorter. The idle time must be at least 10 seconds. So a session whose `timeout` is under 10 seconds does not suspend by default, even with `auto_suspend_on_idle=True`.
2. `idle_policy` sets the `RunMicrovm` idle policy. The service then suspends the MicroVM when its endpoint receives no traffic. A running job receives no traffic after its delivery. So `maxIdleDurationSeconds` must be longer than the longest job.

For a session that launched with either setting, each request step calls `GetMicrovm` before it sends the job. It resumes a suspended MicroVM, and it sends the job to the endpoint that `GetMicrovm` reports. The MicroVM can suspend itself between that check and the request. The endpoint then answers 502, or the worker answers 503. So after a 502, 503, or 504, or a request without an answer, the step checks the state again before it retries. The wait for a resume counts toward the request's `retry_window`.

The worker reads its idle time once, from the run hook payload of the launch. `RunMicrovm` sets the idle policy once, at the launch. So a deploy that changes `auto_suspend_on_idle` or `idle_policy` does not change what a running MicroVM does. The resume mode says whether each job checks the MicroVM state, and resumes a suspended MicroVM, before delivery. The launch checkpoint records the resume mode in its `resumeBeforeDelivery` field. Replay follows the recorded resume mode, not the current config. So a deploy changes the resume mode only for sessions that launch after it.

A MicroVM lives at most its `timeout` plus 5 minutes, and never longer than 8 hours. What the next `vm.invoke` reports after the MicroVM ends depends on the state check:

- For a session that launched with `auto_suspend_on_idle=True`, or with an `idle_policy`, the request step calls `GetMicrovm` before each job. A MicroVM that is terminating, terminated, or removed then fails the job at once with `MicrovmNotRunningError`, a subclass of `MicrovmDeliveryError`. A retry finds the same state, so the job is not retried. The message ends with the request ID of the call that reported the state, for example `GetMicrovm request ID: <id>.`.
- Every session, and every `microvm` job over HTTP, calls `CreateMicrovmAuthToken` before the first request, and again after a 401 or 403. The API model lists `ResourceNotFoundException` for that call. That answer fails the job at once with `MicrovmNotRunningError`, whose message ends with `CreateMicrovmAuthToken request ID: <id>.`. The live service's answer for a removed MicroVM is not verified.
- A session that launched with neither setting makes no `GetMicrovm` call. When `CreateMicrovmAuthToken` returns a token, the request goes to the endpoint. A terminated MicroVM's endpoint answers 502, and the request step retries a 502 as a MicroVM that is still starting. So the job fails with a plain `MicrovmDeliveryError`, after the retries of both tiers.

Suspending is off by default, because it needs more permissions and changes what the MicroVM does between jobs:

- The function's role needs `lambda:GetMicrovm` and `lambda:ResumeMicrovm`. Without them, every job fails. A later deploy that turns suspending off does not change the resume mode of a session that launched with it. So keep `lambda:GetMicrovm` and `lambda:ResumeMicrovm` until every session that launched with `auto_suspend_on_idle=True` or an `idle_policy` has ended.
- With `auto_suspend_on_idle`, the MicroVM's role needs `lambda:SuspendMicrovm` on the image. That permission lets code in any MicroVM of the image suspend any other. So use one image per trust boundary.
- Background processes that an earlier job started stop while the MicroVM is suspended. The worker counts only its running jobs.

The worker must support `auto_suspend_on_idle`. The JavaScript worker [`@aws/durable-execution-sdk-js-microvm-worker`](https://github.com/aws/aws-durable-execution-sdk-js/tree/main/packages/aws-durable-execution-sdk-js-microvm-worker) and the Python worker [`aws-durable-execution-sdk-python-microvm-worker`](../aws-durable-execution-sdk-python-microvm-worker) both do. `idle_policy` works with both workers. Enable the `suspend` and `resume` hooks in the image. The worker then refuses jobs while the MicroVM freezes, and accepts them again as soon as it resumes. Without the `resume` hook, the first job after a resume can wait a little over 30 seconds, until the worker's refusal ends and the request step delivers again.

### Session permissions

A session needs the permissions of `microvm` with HTTP delivery: `lambda:RunMicrovm`, `lambda:TerminateMicrovm`, `lambda:CreateMicrovmAuthToken`, `lambda:PassNetworkConnector`, and `iam:PassRole`. Suspending adds the permissions listed above.

### Subtypes

The JavaScript operations record MicroVM subtypes, such as `Microvm` on the child context, `MicrovmSession` on a session, and `MicrovmLaunch` on the launch step. This package records the SDK's default subtypes for now, because the Python SDK does not accept custom subtypes yet.

On replay, the SDK compares the subtype of each operation with its checkpoint. A mismatch fails the execution with `NonDeterministicExecutionError`. So a release that changed the subtypes would fail every execution that waits on a MicroVM during the deploy. So this package is published only after the SDK accepts custom subtypes, and it records the MicroVM subtypes from its first release.

## Best practices for MicroVM operations

The MicroVM operations handle the network, the service, and the worker lifecycle. They do not check every setting. This section lists what your code and your deploy must do. Each rule states what happens if you break it.

### Deploys and versions

1. Invoke the durable function through a published version or an alias, not `$LATEST`.
   - An execution started through a version or an alias is pinned to that version. Every replay runs the same code and the same configuration, including the environment variables.
   - An execution started through `$LATEST` is not pinned. After a deploy, it resumes on the new code. See [Invoking durable Lambda functions](https://docs.aws.amazon.com/lambda/latest/dg/durable-invoking.html).
   - So under `$LATEST`, a launch step that retries after a deploy can fail with `MicrovmLaunchError`. A MicroVM that an earlier attempt of that step created can then keep running, and be billed, until its maximum duration: the operation timeout plus 5 minutes, at most 8 hours.
2. Do not read MicroVM settings from a source that can change during an execution, such as an SSM parameter, AppConfig, or an S3 object. Code outside a step runs again on every replay. So it reads the value again. A value that changes between two attempts of a launch step can fail the launch, as in rule 1. Read such a value inside a step. A replay then uses the checkpointed value.
3. The MicroVM packages are experimental. A release can change the durable operations that the MicroVM operations record. [Subtypes](#subtypes) describes one such change. An execution that replays its history on such a release fails with `NonDeterministicExecutionError`. So let executions that started on an earlier build end on that build. Under rule 1, this needs no extra step: a new version starts only new executions, and running executions continue on their own version. Under `$LATEST`, let the running executions end before you deploy a new build.

### Function timeout

1. Set the function timeout so that each request step starts with at least 11 seconds left in the invocation. With less time left, the step attempt fails at once. A function timeout under 11 seconds fails every attempt. So every job over HTTP fails with `MicrovmDeliveryError`, and its message says to raise the function timeout. Every session job goes over HTTP. [Defaults and limits](#defaults-and-limits) explains the 11 seconds and gives the recommended timeout.

### Job inputs

1. Pass a JSON-serializable input with finite numbers. Any other input raises `TypeError` before any durable operation starts. See [Errors](#errors).
2. Keep the serialized job request at or under 6 MiB. The job request holds the input and a few job fields. Pass larger data by reference, for example as an S3 key.
   - The Python worker reads a job request body of at most 6 MiB. It answers a larger body with HTTP 400.
   - The operation does not check the size before it sends the job. So a larger body reaches the worker, and the job fails with `MicrovmDeliveryError`. It fails at once when the 400 reaches the operation. It fails only after the retries of both tiers when the connection is reset instead, and the MicroVM is billed meanwhile. [What the worker does](../aws-durable-execution-sdk-python-microvm-worker/README.md#what-the-worker-does) says when each case happens.
3. Expect a job to go over HTTP in three cases:
   - The `run` hook payload of a `microvm` job is over 4,096 code points. The payload holds the input, the callback ID, and a few other fields.
   - The `microvm` config sets `request.path`. The job then goes over HTTP for any input size.
   - The job is a session job. Every session job goes over HTTP.

   A job over HTTP needs four things that a `run` hook job does not:
   - `lambda:CreateMicrovmAuthToken` in the function's role. Without it, the job fails with `MicrovmDeliveryError`. See [Permissions](#permissions).
   - The function timeout of [Function timeout](#function-timeout). With a shorter timeout, the job fails with `MicrovmDeliveryError`.
   - Ingress to the MicroVM. The operation gives such a MicroVM the `ALL_INGRESS` connector. If you set `ingress_network_connectors`, include a connector that allows ingress. Without ingress, the request step cannot reach the MicroVM's endpoint. So the job fails.
   - `lambda:PassNetworkConnector` on the `ALL_INGRESS` connector in the function's role. Its ARN is `arn:aws:lambda:<region>:aws:network-connector:aws-network-connector:ALL_INGRESS`. `RunMicrovm` authorizes each connector separately. So a role that allows `NO_INGRESS` and `INTERNET_EGRESS`, but not `ALL_INGRESS`, fails every HTTP launch with `MicrovmLaunchError`.

   A `microvm` job whose payload fits the `run` hook, and whose config has no `request.path`, needs none of these four. So a function that passed its tests with such jobs can fail every job over HTTP. See [How the job reaches the MicroVM](#how-the-job-reaches-the-microvm).

### Job handlers

1. Follow [Best practices for job handlers](../aws-durable-execution-sdk-python-microvm-worker/README.md#best-practices-for-job-handlers) in the worker README for the code that runs each job in the MicroVM.
2. Make the side effects of a job safe to repeat.
   - The worker ignores a second delivery of a callback ID. It remembers the callback IDs of its last 1,000 finished jobs, and it forgets them when its process restarts.
   - Each `microvm` call and each `vm.invoke` creates a new callback ID. So when your code runs a job again after a failure, the worker runs it again, also in the same MicroVM.
   - A failure can follow a run that already changed something. After `MicrovmLaunchError` with `run` hook delivery, the MicroVM can still run the job. An HTTP launch request contains no job. The request step starts only after the launch succeeds. So when you invoke through a version or an alias, no MicroVM runs the job after `MicrovmLaunchError` with HTTP delivery. After `MicrovmTimeoutError`, the job can have done part of its work. See [Errors](#errors).

### The HTTP job route

1. Answer the job request itself with a 2xx status at once, and run the job in the background. The worker package does this for its routes.
   - The request step waits at most 10 seconds for an answer, and then sends the job again. So a route that answers only when a longer job ends receives the job again and again. The delivery then fails with `MicrovmDeliveryError`, after the retries of both tiers.
   - The operation does not follow redirects. It fails the job at once with `MicrovmDeliveryError` for a 3xx answer, and for a 4xx answer other than 401, 403, and 429. A proxy in front of the route that redirects, for example to add a trailing slash, causes the same failure. See [Retries in two tiers](#retries-in-two-tiers).
2. Ignore a second request for a callback ID that the route already ran. The same job can arrive more than once. A route served without the worker package must do this itself, or it runs the job again. See [How the job reaches the MicroVM](#how-the-job-reaches-the-microvm).
3. Give `path` a value that follows the path rules in [How the job reaches the MicroVM](#how-the-job-reaches-the-microvm). The same check rejects a query, a fragment, and a path that starts with `/aws/lambda-microvms/`, where Lambda sends the lifecycle hooks. An invalid path raises `ValidationError` when `microvm` or `vm.invoke` is called, before its durable operations start. Register the same string as the worker's route. The worker answers a path that it does not serve with HTTP 404, and the operation then fails the job at once with `MicrovmDeliveryError`.

### Sessions

1. Pass `vm.invoke` the context that the session handler receives, or a context nested in it. A job created in another context runs, and the next replay fails the execution with `NonDeterministicExecutionError`. `vm.invoke` raises `TypeError` only for the context that `microvm_session` was called with. It does not detect any other wrong context. See [`microvm_session`](#microvm_session-run-several-jobs-in-one-microvm).
2. If you set `auto_suspend_idle_time`, meet three conditions:
   - `auto_suspend_on_idle` is `True`.
   - The idle time is at least 10 seconds.
   - The idle time is at most the session `timeout`.

   A value that breaks any one condition raises `ValidationError` when `microvm_session` is called, before any durable operation starts. See [Suspending an idle MicroVM](#suspending-an-idle-microvm).
3. Change `auto_suspend_on_idle` or `idle_policy` only through a new version, as in [Deploys and versions](#deploys-and-versions). The launch records the resume mode, and a deploy changes it only for sessions that launch after it. Every job of a session that launched with suspending needs `lambda:GetMicrovm` and `lambda:ResumeMicrovm`, also after a deploy turns suspending off. So remove them from the function's role only as [Suspending an idle MicroVM](#suspending-an-idle-microvm) describes.

### Retry strategies

1. Keep `default_microvm_retry_strategy` unless you need another policy. It decides by the error class, the error code, and the HTTP status. A strategy that retries less fails a job on a throttle or a network error. A strategy that retries more repeats errors that a retry cannot change, such as `AccessDeniedException`, and the job fails later. See [Retries in two tiers](#retries-in-two-tiers).
2. In a custom `retry_strategy`, match an AWS error by its code, `error.response["Error"]["Code"]`. Match an error of the operation by its class, such as `MicrovmEndpointUnavailableError`. The API model names each error code. It defines each message only as a string. So the service can change a message without an API change. A strategy that matches the message then decides differently, and nothing reports the change.

### Custom clients and tests

1. A `client` that you pass must behave as a boto3 `lambda-microvms` client.
   - It returns outputs in the boto3 shape, with `ResponseMetadata`. A `RunMicrovm` output without `microvmId`, or without `endpoint` for HTTP delivery, fails the launch with `MicrovmLaunchError`. An output without `ResponseMetadata.RequestId` gives error messages without a request ID.
   - It raises botocore `ClientError` with a `response` that holds `Error.Code` and `ResponseMetadata.HTTPStatusCode`. The default strategy reads both. It fails the step at once for an error of any other class, unless the error is a timeout or a connection failure.
   - It sets connect and read timeouts, such as the 5 and 30 seconds of the default client. botocore's own defaults are 60 seconds each. A `RunMicrovm` or `TerminateMicrovm` call that runs past the function timeout records no outcome for its step attempt. So the step runs again. A `CreateMicrovmAuthToken`, `GetMicrovm`, or `ResumeMicrovm` call that reaches the invocation reserve fails its attempt. Its thread keeps running until the call returns. So the call can still reach the service after the step stopped waiting for it.
2. Pass an `opener` when the network reaches the MicroVM endpoint only through a proxy. The default opener ignores the proxy settings in the environment. So without an `opener`, the launch succeeds, and every POST fails. The job then fails with `MicrovmDeliveryError`, after the retries of both tiers. See [Retries in two tiers](#retries-in-two-tiers).
3. In tests, wrap a real `lambda-microvms` client in botocore's `Stubber`. `Stubber` checks each stubbed output against the API model. Its `add_client_error` raises `ClientError` with `Error.Code` and `ResponseMetadata.HTTPStatusCode`. So the operation and the default strategy read the same fields as from the service. A fake with another shape tests paths that the service never produces. Add `ResponseMetadata` with a `RequestId` to a stubbed output, or pass `response_meta` to `add_client_error`, to test the request IDs in messages. moto has no `lambda-microvms` backend, as of moto 5.2.4.

### Permissions and monitoring

1. Give the function's execution role every permission in [Permissions](#permissions), including `lambda:TerminateMicrovm`. Without it, every operation still returns its result, and every MicroVM runs, and is billed, until its maximum duration.
2. Create an alarm on the log line that starts with `MicroVM "<name>": terminate failed for <id>`, for example with a CloudWatch Logs metric filter that matches `terminate failed for`. A failed terminate does not fail the operation. The operation logs this error instead. The MicroVM then keeps running, and is billed, until its maximum duration: the operation timeout plus 5 minutes, at most 8 hours. This line is the only sign. See [What it does](#what-it-does).
