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

The worker needs `handler`, `routes`, or both. A handler is a plain function. An `async def` function, or an object whose `__call__` is `async def`, raises `TypeError`. A handler that returns an awaitable fails its job with a `TypeError`, because the worker cannot run it. The handler's return value becomes the durable function's result, and must be JSON-serializable. An exception fails the durable operation with the exception's class name and message.

`start_microvm_worker` serves in a daemon thread and returns. So the entry point ends with `worker.wait()`, which blocks until `worker.close()`.

## Image configuration

Start the worker when the image's process starts. Lambda snapshots the process after the `ready` hook, so the worker must already be listening.

The image needs these hook settings in `CreateMicrovmImage`:

```json
{
  "hooks": {
    "port": 8080,
    "microvmHooks": { "run": "ENABLED", "runTimeoutInSeconds": 10 },
    "microvmImageHooks": { "ready": "ENABLED" }
  }
}
```

- The `run` hook delivers the job, or tells the worker that jobs arrive over HTTP. So it must be enabled.
- The service requires the `ready` hook whenever a lifecycle hook is enabled. The worker answers it with HTTP 200.
- `port` must match the worker's port. The worker listens on 8080 by default, which is also the port that the MicroVM endpoint forwards to. Pass `port=` for another port.

The worker listens on IPv6 and IPv4 at once when the host supports it. Lambda calls the hooks on `localhost`, and that name can resolve to either family.

The worker answers the `resume`, `suspend`, and `terminate` hooks with HTTP 200, and does nothing else for them yet. A later change acts on them.

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

A second delivery of a job is answered like the first and ignored, while the job runs and after it ended. The durable function can deliver the same job again: it retries a delivery whose answer it did not receive, and its request step runs again on replay when its result was not checkpointed. The worker remembers the callback IDs of the last 1,000 finished jobs. So a job runs once, unless more than 1,000 other jobs ended in between, or the worker process restarted.

The worker creates a Lambda client for each job, after the job arrives, not at image build. A client created before the snapshot would carry build-time state into every MicroVM. A client kept across jobs would keep connections that a suspend of the MicroVM can leave dead. The worker closes each job's default client when the job ends. It never closes a client from `create_client`.

A document that does not match the contract gets HTTP 400 with the validation message. If the document named a callback, the worker also fails that callback, so the durable function fails at once instead of at its timeout. A request whose body cannot be read gets HTTP 400 with a generic message, and the error is logged. The worker accepts a `run` hook body of at most 64 KB and a job request body of at most 6 MB, with a `Content-Length` or with chunked transfer encoding. In a chunked body, the chunk-size lines, their extensions, and the trailers have a limit of their own, of the same size. A larger body gets HTTP 400. With `Expect: 100-continue`, the 400 replaces `100 Continue`, so the client does not send the body. Without it, the worker reads and drops the body after the answer, for at most 10 seconds and 24 MB. Without that read, the connection would close with unread bytes, and the client would get a TCP reset instead of the 400. A body that is larger, or that is still arriving after 10 seconds, still gets the reset. Other lifecycle hooks get HTTP 200. Other paths get HTTP 404.

`worker.idle(timeout)` waits until every job started so far has ended, and returns `False` when the timeout ends first. `worker.close()` stops the server. Running jobs continue, so call `idle()` first to let them report their outcome.

## Lower-level building blocks

- `MicrovmWorkerListener.create(...)` takes the same arguments as `start_microvm_worker`, except `port`. Its `request_handler_class` serves the hooks and routes from your own `http.server.ThreadingHTTPServer`. Set the server's backlog to 511, as `start_microvm_worker` does. A session can send many jobs at once, and the default backlog of 5 resets the connections beyond it. The server's constructor already listens, so set `request_queue_size = 511` as a class attribute of a `ThreadingHTTPServer` subclass, not on the server object.
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
