# AWS Durable Execution SDK for Python: MicroVM worker

> This package is experimental. Its API can change in any release.

This package runs inside an AWS Lambda MicroVM. It receives jobs from a durable function, sends heartbeats while each job runs, and reports each job's result or error to the durable function's callback.

The package does not depend on the durable execution SDK, because it runs in the MicroVM and not in the durable function. The job document is the same in every SDK language. So a durable function in any language can send jobs to this worker.

## Usage

```python
import logging

from aws_durable_execution_sdk_python_microvm_worker import start_microvm_worker


def run_build(job_input, context):
    # context.cancelled is set when the durable function stops waiting.
    return {"passed": build(job_input["repo"], stop=context.cancelled)}


def clone_build(job_input, context):
    return clone_and_build(job_input)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    worker = start_microvm_worker(
        # Jobs that name no route. A small job arrives in the run hook, and a
        # large one over HTTP. The handler receives both the same way.
        handler=run_build,
        # Jobs sent to a named route.
        routes={"/clone-build": clone_build},
    )
    worker.wait()
```

The worker needs `handler`, `routes`, or both. A handler is a plain function. An `async def` function, or an object whose `__call__` is `async def`, raises `TypeError`. A plain function that returns a coroutine fails its job with `ResultSerializationError`, because a coroutine is not JSON-serializable. Python then warns that the coroutine was never awaited. The handler's return value becomes the durable function's result, and must be JSON-serializable. An exception fails the durable operation with the exception's class name and message. `context.cancelled` is set when the durable function stops waiting, for example after a heartbeat timeout, and when Lambda calls the `terminate` hook. Python cannot stop a running thread, so the handler should check the event and stop its work.

`start_microvm_worker` serves in a daemon thread and returns. So the entry point ends with `worker.wait()`, which blocks until `worker.close()`.

## Image configuration

Start the worker when the image's process starts. Lambda snapshots the process after the `ready` hook, so the worker must already be listening.

The image needs these hook settings in `CreateMicrovmImage`:

```json
{
  "hooks": {
    "port": 8080,
    "microvmHooks": {
      "run": "ENABLED",
      "runTimeoutInSeconds": 10,
      "resume": "ENABLED",
      "resumeTimeoutInSeconds": 10,
      "suspend": "ENABLED",
      "suspendTimeoutInSeconds": 10,
      "terminate": "ENABLED",
      "terminateTimeoutInSeconds": 10
    },
    "microvmImageHooks": { "ready": "ENABLED" }
  }
}
```

- The `run` hook delivers the job, or tells the worker that jobs arrive over HTTP. So it must be enabled.
- The `resume` hook tells a session's worker that its MicroVM runs again after a suspend. It is optional, and recommended for sessions. See [Suspending when idle](#suspending-when-idle).
- The `suspend` hook tells the worker that Lambda is about to freeze the MicroVM. The worker then refuses new jobs until the `resume` hook. It is optional, and recommended for sessions.
- The `terminate` hook tells the worker that Lambda is about to end the MicroVM. The worker then fails the callback of each running job, so the durable function fails the job at once instead of at its heartbeat timeout or its timeout. It is optional, and recommended. Give it at least 10 seconds, because the worker reports before it answers, for at most 5 seconds. A hook timeout left unset ended the hook after about 2 seconds in testing.
- The service requires the `ready` hook whenever a lifecycle hook is enabled. The worker answers it with HTTP 200.
- `port` must match the worker's port. The worker listens on 8080 by default, which is also the port that the MicroVM endpoint forwards to. Pass `port=` for another port.

The worker listens on IPv6 and IPv4 at once when the host supports it. Lambda calls the hooks on `localhost`, and that name can resolve to either family.

## What the worker does

On `POST /aws/lambda-microvms/runtime/v1/run` the worker:

1. Validates the run hook payload.
2. Answers HTTP 200 at once, because a slow or failed `run` hook can terminate the MicroVM.
3. Runs `handler` for the payload's job, when it has one. A payload without a job belongs to a MicroVM whose jobs arrive over HTTP.

The worker acts only on the first valid `run` hook. The MicroVM endpoint forwards a request to a hook path from any caller that holds an auth token for the MicroVM. So a later `run` hook gets HTTP 200, and is ignored.

On `POST <route>` the worker validates the job request, answers HTTP 202 at once, and runs the route's handler. `POST /durable-execution/v1/job` (`MICROVM_JOB_PATH`) runs `handler`. That path is reserved, so a route cannot use it. A job request can arrive before the `run` hook. Until the `run` hook arrives, the worker takes the MicroVM ID for `context.microvm_id` from the first job request that has a `microvmId`. The `run` hook's ID then replaces it for later jobs. Without either, `context.microvm_id` is `"unknown"`.

Each job runs in its own thread. For every job, the worker:

1. Sends heartbeats while the job runs and while its outcome is reported, when the job has a heartbeat timeout. See [Heartbeats](#heartbeats).
2. Sets `context.cancelled` when a heartbeat finds the callback gone while the handler runs. Python cannot stop a running thread, so the handler should check the event and stop its work. The worker then does not report the outcome.
3. Completes the callback with the handler's result, or fails it with the handler's exception. A `SystemExit` from the handler is reported as a failure too.
4. Reports a result that is over 256 KB, or that is not JSON-serializable, as a failure. So the durable function learns why at once, instead of at its timeout.

A second delivery of a job is answered like the first and ignored, while the job runs and after it ended. The durable function can deliver the same job again: it retries a delivery whose answer it did not receive, and its request step runs again on replay when its result was not checkpointed. The worker remembers the callback IDs of the last 1,000 finished jobs. When a job ends and the worker already holds 1,000 IDs, it removes the oldest one. So a job runs once, unless at least 1,000 other jobs ended in between, or the worker process restarted.

The worker creates a Lambda client for each job, after the job arrives, not at image build. A client created before the snapshot would carry build-time state into every MicroVM. A client kept across jobs would keep connections that a suspend of the MicroVM can leave dead. The worker closes each job's default client when the job ends. It never closes a client from `create_client`.

A document that does not match the contract gets HTTP 400 with the validation message. If the document named a callback, the worker also fails that callback, so the durable function fails at once instead of at its timeout. A request whose body cannot be read gets HTTP 400 with a generic message, and the error is logged. The worker accepts a `run` hook body of at most 64 KB and a job request body of at most 6 MB, with a `Content-Length` or with chunked transfer encoding. In a chunked body, the chunk-size lines, their extensions, and the trailers have a limit of their own, of the same size. A larger body gets HTTP 400. With `Expect: 100-continue`, the 400 replaces `100 Continue`, so the client does not send the body. Without it, the worker reads and drops the body after the answer, for at most 10 seconds and 24 MB. Without that read, the connection would close with unread bytes, and the client would get a TCP reset instead of the 400. A body that is larger, or that is still arriving after 10 seconds, still gets the reset. Other lifecycle hooks get HTTP 200. Other paths get HTTP 404.

`worker.idle(timeout)` waits until every job started so far has ended, and a running `SuspendMicrovm` call has returned. It returns `False` when the timeout ends first. `worker.close()` stops the server and the idle time. Running jobs continue, so call `idle()` first to let them report their outcome.

## Suspending when idle

A session's run hook payload can set `autoSuspendIdleSeconds`. The session sets it only with `auto_suspend_on_idle=True`, because suspending is off by default. The worker then suspends its own MicroVM when it has run no job for that many seconds:

1. The idle time starts when the `run` hook arrives without a job, when a job ends, and when the `resume` lifecycle hook arrives, if the image enables it.
2. A job that starts cancels the idle time. So the worker never suspends the MicroVM during a job.
3. When the idle time ends, the worker calls `SuspendMicrovm` with the MicroVM ID from the `run` hook, never one from a job request. The call returned in under 100 milliseconds in testing, before the MicroVM froze. No attempt of the call starts after 10 seconds, because a `before-send` handler on the client stops botocore's retries at that deadline. botocore cannot cancel an attempt in flight, and it opens the connection after `before-send`. So an attempt that started before the deadline can still reach the service after it. The worker waits for that attempt to end, and refuses jobs until then. The client's connect and read timeouts bound the attempt: 5 and 10 seconds for the default client. A client from `create_microvms_client` should use similar timeouts. botocore's own defaults are 60 seconds each, so with them the refusal can last minutes. The durable function resumes the MicroVM before the next job, and the process continues with its memory and files.
4. From the moment the idle time ends, the worker answers every job with HTTP 503 and does not start it. A job accepted then would freeze with the MicroVM, and nothing would resume it. After a 503, the durable function checks the state, resumes the MicroVM, and delivers the job again.
5. The refusal ends when the `resume` lifecycle hook arrives, or 30 seconds after the call returned, if no hook arrives. The idle time then starts again. At most one call runs at a time. A `resume` hook that arrives during the call ends the refusal at once, and the next idle time starts when the call ends. So enable the `resume` hook in the image. Without it, the first job after a resume can wait a little over 30 seconds: the 30 seconds, plus up to 4 seconds until the durable function's next delivery attempt. A test measured 32 seconds.
6. A call that the service rejected on its first attempt, such as `AccessDeniedException` for a missing permission, is logged as a warning, and the MicroVM keeps running. Missing credentials count as a rejection. A later call in the same MicroVM most likely finds no credentials either. The worker accepts jobs again at once and does not retry the call. The next job that ends starts the idle time again. The exception is a `resume` hook that arrived during the call. The idle time then starts when the call ends. So the worker retries the call one idle time after the call ends, if no job runs and no `suspend` hook refusal is in effect when the call ends. A second exception is a `suspend` hook that arrived during the call and whose refusal is still in effect when the call ends. The MicroVM is then suspending anyway. So the rejection does not end the hook's refusal. That refusal ends on the `resume` hook, or 30 seconds after the last `suspend` hook. The idle time then starts again.
7. A call that the service refused for now on its first attempt is logged as a warning too, and the worker accepts jobs again at once. This covers expired credentials and a signature error, with or without a clock offset, whatever the HTTP status. The service answers these before it acts. botocore retries none of these errors, so they reach the worker on the first attempt. The idle time then starts again, so the worker tries the call again. That idle time lasts at least 30 seconds, also when `autoSuspendIdleSeconds` is shorter. The same error can repeat on every call, for example with a wrong secret key or an expired session token from environment variables. So while no job ends and no lifecycle hook arrives, the worker makes at most one call per 30 seconds, and logs one warning per call. A job that ends after the refusal, or a `resume` hook after it, starts the normal idle time instead. A `resume` hook or a job that ends during the call does not shorten the idle time after the refusal, because the refused outcome arrives after it. A `suspend` hook whose refusal is still in effect when the call ends is the exception, as in item 6. Its refusal stays, and the idle time starts when it ends. The default client retries a throttle, so a throttle from it usually ends as an unknown outcome, as in item 8. A throttle counts as refused for now only from a client that does not retry it, such as a client from `create_microvms_client` with one attempt. The JavaScript worker classifies these errors the same way. Its default client retries a clock-skew error. So there, a clock-skew error usually ends as an unknown outcome.
8. A call whose outcome is unknown is handled like a successful call, because the service may have suspended the MicroVM anyway. This covers a timeout, a conflict, a server error, and a dropped connection. An error after botocore retried an earlier attempt also counts as unknown, because the earlier attempt may have suspended the MicroVM. The default client uses botocore's standard retry mode with 3 attempts, and that mode retries a throttle. So a throttle usually ends here. The refusal continues until the `resume` hook or the 30-second limit. The idle time then starts again, so the worker tries the call again. A `resume` hook during the call, or the end of a `suspend` hook's 30 seconds during the call, has already ended the refusal. The worker then accepts jobs at once, and tries the call again one idle time after the call and every job end.
9. The worker logs one warning for each failed call. The message names the outcome. The fields are, in order, `microvmId`, `error`, `outcome`, `suspendHookArrived`, `terminating`, `closed`, and `refusingJobs`. `microvmId` is the MicroVM ID of the call. `error` holds the name and the message of the error. `outcome` is `rejected`, `refusedForNow`, or `unknown`. `suspendHookArrived` states whether a `suspend` hook arrived since the idle time ended. `terminating` states whether a `terminate` hook came before the worker acted on the outcome. `closed` states whether `worker.close()` came before the worker acted on the outcome. `refusingJobs` states whether the worker answers jobs with HTTP 503 after it has acted on the outcome.

`worker.close()` stops the idle time. A closed worker never suspends its MicroVM again. A refusal that is already running still ends on the `resume` hook or after 30 seconds.

The worker counts only jobs. It cannot see background threads or processes, or inbound traffic that is not a job. A session with such work leaves `auto_suspend_on_idle` off, and the payload then has no `autoSuspendIdleSeconds`.

The worker creates a `lambda-microvms` client for each suspend, and closes it after the call. Pass `create_microvms_client` to provide the client. The worker never closes a client that it returns. A client that retries must report its retries in `ResponseMetadata.RetryAttempts` of the error that it raises, as botocore does. The worker otherwise counts a retried call as a first attempt. An earlier attempt of that call may have suspended the MicroVM. So a retried throttle would count as refused for now, and the worker would accept a job that then freezes with the MicroVM.

## Suspend and terminate hooks

Lambda waits for the answer to a `suspend` or `terminate` hook, up to the hook's timeout. The network and the execution role's credentials work while the hook runs. These facts come from probes in us-east-1, which the JavaScript SDK ran.

On the `suspend` hook the worker:

1. Answers HTTP 200 at once. A suspend hook that ran past its timeout terminated the MicroVM instead of suspending it.
2. Refuses every job with HTTP 503, as after its own suspend. A job accepted now would freeze with the MicroVM. The refusal ends when the `resume` hook arrives, or after 30 seconds.
3. Leaves running jobs alone. They freeze with the MicroVM, and continue after a resume.
4. Defers a job from a `run` hook whose body was still arriving. The durable function cannot deliver a `run` hook job again, so the worker does not refuse it. It starts the job when the refusal ends.

Lambda called the hook after `SuspendMicrovm` from outside, after the worker's own `SuspendMicrovm`, and when the idle policy suspended the MicroVM.

On the `terminate` hook the worker:

1. Sets each running job's `context.cancelled`, and stops its heartbeats.
2. Fails each running job's callback with `MicrovmTerminatedError`. The worker reports `MicrovmTerminatedError` as the callback's error type. The `microvm` operation then fails with a `MicrovmJobFailedError` whose message contains `(MicrovmTerminatedError)`. The worker does not report the handler's outcome when it returns later.
3. Skips a job whose callback a heartbeat already found gone. A report would fail the same way.
4. Answers HTTP 200 when the reports end, or after 5 seconds. A report took 20 to 1,700 milliseconds in testing. The answer also waits for a job that is reporting its own outcome, for a `run` hook request whose body is still arriving, and for every callback failure report in flight, such as the report for an invalid request. Work that begins during the wait is included. A second `terminate` hook that arrives during the wait waits for the same work. Lambda can end the MicroVM as soon as it gets the answer, so a report sent after it can be lost.
5. Refuses every later job with HTTP 503, and never suspends the MicroVM again. A job from a `run` hook whose body was still arriving is not started. Its callback fails with `MicrovmTerminatedError`, as does a job deferred by an earlier `suspend` hook.

Lambda called the hook after `TerminateMicrovm` on a running MicroVM, and at the end of `maximumDurationInSeconds`. It did not call the hook when it terminated a suspended MicroVM, after `TerminateMicrovm` or at the end of the idle policy's `suspendedDurationSeconds`. The process is frozen then. A job of such a MicroVM fails at its heartbeat timeout or its timeout.

The MicroVM endpoint forwards a request to a hook path from any caller that holds an auth token for the MicroVM. Lambda's own hook calls carried `Host: localhost:8080` and no `x-amzn-requestid` header. A forwarded request carried the endpoint's host name and an `x-amzn-requestid` header. So the worker acts on `suspend`, `terminate`, and `resume` only when the request has a `localhost`, `127.0.0.1`, or `[::1]` host, with any port, and no `x-amzn-requestid`. It answers any other request to these hooks with 200, and logs a warning. The Lambda documentation does not state these headers. If Lambda changed them, the worker would ignore `suspend` and `terminate`. A refusal after a suspend would then end after 30 seconds instead of at the `resume` hook. Without the check on `resume`, a forwarded `resume` between a `suspend` hook and the freeze could end the refusal early, and a job accepted then would freeze with the MicroVM. The `run` hook does not use the check, because a false rejection there would lose every job.

## Lower-level building blocks

- `MicrovmWorkerListener.create(...)` takes the same arguments as `start_microvm_worker`, except `port`. Call its `close()` when you stop serving it, so that it no longer suspends the MicroVM. Its `request_handler_class` serves the hooks and routes from your own `http.server.ThreadingHTTPServer`. Set the server's backlog to 511, as `start_microvm_worker` does. A session can send many jobs at once, and the default backlog of 5 resets the connections beyond it. The server's constructor already listens, so set `request_queue_size = 511` as a class attribute of a `ThreadingHTTPServer` subclass, not on the server object.
- `RunHookRequest.from_dict` and `MicrovmJobRequest.from_dict` validate the two documents that deliver a job: the `run` lifecycle hook body, and the body of an HTTP job request.
- `CallbackReporter` sends heartbeats, and completes the callback with a result or an error.

## The job documents

The `run` hook body that Lambda sends:

```json
{ "microvmId": "<id>", "runHookPayload": "<JSON string>" }
```

The `runHookPayload` string holds:

```json
{
  "version": 1,
  "region": "us-east-1",
  "job": { "callbackId": "<id>", "heartbeatTimeoutSeconds": 60, "input": {} }
}
```

`job` is absent when the job arrives over HTTP. An HTTP job request body has the job fields at the top level, with `version`, `region`, and an optional `microvmId`. `MicrovmJobRequest` holds those job fields in its `job` attribute.

A document that does not match raises `InvalidRunHookPayloadError`. When the document named a non-empty callback ID, the error carries `callback_id` and `region`, so the worker can fail that callback at once.

## Reporting

`CallbackReporter.succeed` sends a JSON result of at most 256 KB. A larger result raises `ResultTooLargeError`, and a result that is not JSON-serializable raises `ResultSerializationError`. In both cases no call is made, so the caller can report a failure instead. A `float` that is `nan` or infinite is not JSON, so it raises `ResultSerializationError`.

`CallbackReporter.fail` sends the error's class name and message, cut to 256 and 8,192 characters. The traceback is not sent, because it would expose the image's file paths in the durable execution history.

Each completion makes up to 5 attempts, and each attempt ends after 30 seconds. A permanent error, such as `AccessDeniedException` or a closed callback, is not retried. The service answers `CallbackTimeoutException` for a callback that is already complete, timed out, or whose execution stopped. That answer after an attempt with an unknown outcome means that an earlier attempt most likely delivered the outcome. The reporter then logs a warning and returns.

## Heartbeats

When the job sets `heartbeatTimeoutSeconds`, `Heartbeats` sends a heartbeat at once, and then about every third of the heartbeat timeout, at most every 15 minutes. Each wait is 1 to 2 seconds shorter than the interval, so MicroVMs that start together send at different moments. The jitter comes from a hash of the callback ID, not from the `random` module. Lambda restores every MicroVM from one snapshot of the worker process, so `random` would return the same values in each of them.

Each heartbeat call ends after a third of the interval. After a failed heartbeat, the next one comes after an eighth to a quarter of the interval, for at most two failures in a row. With these limits, two failed or stalled calls in a row still stay within the heartbeat timeout:

1. The service times the heartbeat timeout from when it receives a heartbeat. It can receive a call at the call's start or at its end.
2. So the worst gap runs from a good call received at its start, through two failed calls and their retry waits, to a good call received at its end.
3. With interval I, that gap is at most about 2.83 I. The heartbeat timeout is at least 3 I.

A third failure in a row waits a full interval, and the job can then reach its heartbeat timeout. The first heartbeat of a job also resolves credentials and opens a connection within its call timeout. With the default interval, that is a ninth of the heartbeat timeout. So a heartbeat timeout under about 15 seconds can lose the first heartbeat. The quick retry usually covers it.

An explicit `heartbeat_interval_seconds` must be above 0 and at most 900, and it is cut to a third of the job's heartbeat timeout. Other values raise `ValueError`.

## Logging

The worker logs to the standard library logger `aws_durable_execution_sdk_python_microvm_worker`, and passes its structured fields in `extra`. Every line about a job carries its `callbackId`. `CallbackReporter.create` and the heartbeats take the same `logger` argument. Configure `logging` in the image to see its INFO lines, for example with `logging.basicConfig(level=logging.INFO)`. Any object with the `logging.Logger` signatures for `info`, `warning`, and `error` can replace it. A logger that raises does not stop the worker: the line is dropped.

## Permissions

The MicroVM's execution role needs `lambda:SendDurableExecutionCallbackSuccess`, `lambda:SendDurableExecutionCallbackFailure`, and `lambda:SendDurableExecutionCallbackHeartbeat` on the durable function's ARN (`arn:aws:lambda:<region>:<account>:function:<name>:*`).

A session that suspends when idle also needs `lambda:SuspendMicrovm` on the MicroVM image (`arn:aws:lambda:<region>:<account>:microvm-image:<name>`). The action authorizes on the image, not on the MicroVM. So a MicroVM with this permission can suspend any MicroVM from the same image, even during a job. That job then waits until its heartbeat timeout or its timeout. Use one image per trust boundary.

The worker needs boto3 1.43.35 or later, the first release with the `lambda-microvms` client.

## Best practices for job handlers

These rules apply to the `handler` and the route handlers that you pass to `start_microvm_worker`. Each rule states what happens if a handler breaks it.

1. Write each handler as a plain function. To run async code, call `asyncio.run` inside the handler. The worker runs each job in a thread of its own, and that thread has no running event loop. So `asyncio.run` can start one there. An `async def` handler makes `start_microvm_worker` raise `TypeError`, and a handler that returns a coroutine fails each of its jobs with `ResultSerializationError`. See [Usage](#usage).
2. Return a JSON-serializable value of at most 256 KB. The worker reports any other value as a failure of the job. So the durable function raises `MicrovmJobFailedError`. Store a larger result elsewhere, for example in S3, and return its key. See [Reporting](#reporting).
3. Pass a `logger` that does not raise. The worker drops each line whose logger raises, and it continues. So that line is lost, and nothing reports the loss. See [Logging](#logging).
