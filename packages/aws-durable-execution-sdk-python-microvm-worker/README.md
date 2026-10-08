# AWS Durable Execution SDK for Python: MicroVM worker

> This package is experimental. Its API can change in any release.

This package runs inside an AWS Lambda MicroVM. It receives jobs from a durable function, sends heartbeats while each job runs, and reports each job's result or error to the durable function's callback.

The package does not depend on the durable execution SDK, because it runs in the MicroVM and not in the durable function. The job document is the same in every SDK language. So a durable function in any language can send jobs to this worker.

The package currently contains these parts:

- `parse_run_hook_request` and `parse_job_request` validate the two documents that deliver a job: the `run` lifecycle hook body, and the body of an HTTP job request.
- `CallbackReporter` sends heartbeats, and completes the callback with a result or an error.
- `Heartbeats` sends a job's heartbeats on a schedule.

The HTTP listener for the lifecycle hooks and the job routes comes in a later change.

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

`job` is absent when the job arrives over HTTP. An HTTP job request body has the job fields at the top level, with `version`, `region`, and an optional `microvmId`.

A document that does not match raises `InvalidRunHookPayloadError`. When the document named a callback, the error carries `callback_id` and `region`, so the worker can fail that callback at once.

## Reporting

`CallbackReporter.succeed` sends a JSON result of at most 256 KB. A larger result raises `ResultTooLargeError`, and a result that is not JSON-serializable raises `ResultSerializationError`. In both cases no call is made, so the caller can report a failure instead. A `float` that is `nan` or infinite is not JSON, so it raises `ResultSerializationError`.

`CallbackReporter.fail` sends the error's class name and message, cut to 256 and 8,192 characters. The traceback is not sent, because it would expose the image's file paths in the durable execution history.

Each completion makes up to 5 attempts, and each attempt ends after 30 seconds. A permanent error, such as `AccessDeniedException` or a callback that is already complete, is not retried. An "already complete" answer after an attempt with an unknown outcome means that an earlier attempt most likely delivered the outcome. The reporter then logs a warning and returns.

## Heartbeats

When the job sets `heartbeatTimeoutSeconds`, `Heartbeats` sends a heartbeat at once, and then about every third of the heartbeat timeout, at most every 15 minutes. Each wait is 1 to 2 seconds shorter than the interval, so MicroVMs that start together send at different moments. The jitter comes from a hash of the callback ID, not from the `random` module. Lambda restores every MicroVM from one snapshot of the worker process, so `random` would return the same values in each of them.

Each heartbeat call ends after half the interval. After a failed heartbeat, the next one comes after an eighth to a quarter of the interval, for at most two failures in a row. So two failed or stalled calls in a row still stay within the heartbeat timeout.

## Permissions

The MicroVM's execution role needs `lambda:SendDurableExecutionCallbackSuccess`, `lambda:SendDurableExecutionCallbackFailure`, and `lambda:SendDurableExecutionCallbackHeartbeat` on the durable function's ARN (`arn:aws:lambda:<region>:<account>:function:<name>:*`).
