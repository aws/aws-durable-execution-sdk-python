# AWS Durable Execution SDK for Python: compute worker

> This package is experimental. Its API can change in any release.

The compute worker runs jobs from durable functions on compute that is defined outside the durable function. Each compute type has its own subpackage: `aws_durable_execution_sdk_python_compute_worker.microvm`, the worker for AWS Lambda MicroVMs, and `aws_durable_execution_sdk_python_compute_worker.agentcore`, the worker for Amazon Bedrock AgentCore Runtime. The rest of this README describes the MicroVM worker, except the section [Run a job in AgentCore Runtime](#run-a-job-in-agentcore-runtime).

Run jobs from a durable function inside an AWS Lambda MicroVM, and report each job's result to the durable function.

The worker runs in the MicroVM image, not in the durable function. It does not depend on the durable execution SDK. The job document is the same in every SDK language. So a durable function in any language can send jobs to this worker.

## Install

```console
pip install aws-durable-execution-sdk-python-compute-worker
```

## Quick start

```python
import logging

from aws_durable_execution_sdk_python_compute_worker.microvm import (
    MicrovmJobContext,
    start_microvm_worker,
)


def run_build(job_input: dict, context: MicrovmJobContext) -> dict:
    return {"repo": job_input["repo"], "passed": True}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    worker = start_microvm_worker(handler=run_build)
    worker.wait()
```

Start the worker when the image's process starts. `start_microvm_worker` serves in a daemon thread and returns. So the entry point ends with `worker.wait()`.

The handler's return value becomes the durable function's result. An exception fails the job with the exception's class name and message.

## Common uses

### Serve named routes

```python
from aws_durable_execution_sdk_python_compute_worker.microvm import (
    MicrovmJobContext,
    start_microvm_worker,
)


def run_build(job_input: dict, context: MicrovmJobContext) -> dict:
    return {"passed": True}


def clone_build(job_input: dict, context: MicrovmJobContext) -> dict:
    return {"cloned": job_input["repo"]}


if __name__ == "__main__":
    worker = start_microvm_worker(
        handler=run_build,  # Jobs that name no route.
        routes={"/clone-build": clone_build},  # Jobs sent to this path.
    )
    worker.wait()
```

The durable function sends a job to a route with `MicrovmRequestConfig(path=...)` or `MicrovmInvokeOptions(path=...)`. Use the same string in both places.

### Stop a cancelled job

`context.cancelled` is a `threading.Event`. The worker sets it when the durable function stops waiting, for example after a heartbeat timeout, and when Lambda calls the `terminate` hook. Python cannot stop a running thread. So check the event in long work.

```python
import subprocess

from aws_durable_execution_sdk_python_compute_worker.microvm import MicrovmJobContext


def run_build(job_input: dict, context: MicrovmJobContext) -> dict:
    for command in job_input["commands"]:
        if context.cancelled.is_set():
            return {"cancelled": True}
        subprocess.run(command, shell=False, check=True)
    return {"passed": True}
```

### Run async code

A handler must be a plain function. An `async def` handler raises `TypeError`. Call `asyncio.run` inside the handler instead:

```python
import asyncio

from aws_durable_execution_sdk_python_compute_worker.microvm import MicrovmJobContext


async def build(job_input: dict) -> dict:
    await asyncio.sleep(1)
    return {"passed": True}


def run_build(job_input: dict, context: MicrovmJobContext) -> dict:
    return asyncio.run(build(job_input))
```

## Advanced

### Image configuration

Set these hooks in `CreateMicrovmImage`. `port` must match the worker's port, 8080 by default.

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

- The `run` hook is required. It delivers small jobs, or tells the worker that jobs arrive over HTTP.
- The `suspend` and `resume` hooks are needed for sessions that suspend. Without `resume`, the first job after a resume can wait about 30 seconds.
- The `terminate` hook lets the worker fail running jobs at once when the MicroVM ends. Give it at least 10 seconds.
- The service requires the `ready` hook when any lifecycle hook is enabled.

### Suspending

The worker suspends its own MicroVM when the session sets `auto_suspend_on_idle=True`. It never suspends during a job. The SDK never calls `ResumeMicrovm`. So the session's MicroVM must set `idlePolicy.autoResumeEnabled`, and the service resumes it on the next request. Setting that is the job of the durable function's developer. Set `maxIdleDurationSeconds` above the MicroVM's `maximumDurationInSeconds`.

The worker counts only jobs. It does not see background threads, processes, or other inbound traffic. Leave `auto_suspend_on_idle` off for a session with such work.

With `suspend_after`, the durable function suspends the MicroVM itself. The worker does nothing for it.

### Results and heartbeats

- Return a JSON-serializable value of at most 256 KB. The worker reports any other value as a failure of the job. Store a larger result elsewhere, for example in S3, and return its key.
- The worker sends heartbeats while a job runs, when the job sets a heartbeat timeout.
- The worker ignores a second delivery of a job that it already ran. It remembers the last 1,000 finished jobs. So make side effects safe to repeat across process restarts.

### Logging

The worker logs to the standard library logger `aws_durable_execution_sdk_python_compute_worker.microvm`. Pass `logger=` to replace it. Configure `logging` in the image to see its INFO lines.

### Permissions

The MicroVM's execution role needs `lambda:SendDurableExecutionCallbackSuccess`, `lambda:SendDurableExecutionCallbackFailure`, and `lambda:SendDurableExecutionCallbackHeartbeat` on the durable function's ARN (`arn:aws:lambda:<region>:<account>:function:<name>:*`).

With `auto_suspend_on_idle`, the role also needs `lambda:SuspendMicrovm` on the MicroVM image. That permission lets a MicroVM suspend any MicroVM of the same image. So use one image per trust boundary.

The worker needs boto3 1.43.35 or later.

## Run a job in AgentCore Runtime

> The `agentcore` subpackage is experimental. Its API can change in any release.

`start_agentcore_worker` serves the [AgentCore Runtime HTTP contract](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-http-protocol-contract.html) in the agent's container. It needs no dependency beyond this package.

```python
from aws_durable_execution_sdk_python_compute_worker.agentcore import (
    AgentcoreJobContext,
    start_agentcore_worker,
)


def review(job_input: dict, context: AgentcoreJobContext) -> dict:
    context.logger.info("reviewing", extra={"pr": job_input["pr"]})
    return {"approved": True, "comments": []}


if __name__ == "__main__":
    start_agentcore_worker(handler=review).wait()
```

The durable function sends the job with the `agentcore` operation of the [extras](https://github.com/aws/aws-durable-execution-sdk-python/tree/main/packages/aws-durable-execution-sdk-python-extras#run-a-job-in-agentcore-runtime) package:

```python
review = agentcore(context, "review", job_input={"pr": 7}, config=AGENT)
```

The worker listens on port 8080, because AgentCore Runtime sends its requests to that port. The host depends on where the worker runs:

- Inside AgentCore Runtime, the `AGENTCORE_RUNTIME_URL` environment variable is set. The worker then listens on `0.0.0.0`, as the AgentCore contract requires. The runtime publishes no port, and AgentCore authorizes each `InvokeAgentRuntime` caller before it forwards the request.
- Anywhere else, such as a laptop, the worker listens on `127.0.0.1`. The worker has no authentication, so it must not be reachable from the network.
- A `host` argument overrides both.

- `POST /invocations` reads the job document, starts the job's thread, and answers `{"accepted": true}` at once. The thread runs the handler only after the answer is written. A callback ID that the worker already accepted gets the same answer, and the job does not run again. A document that does not match the contract gets HTTP 400.
- `GET /ping` answers `{"status": "HealthyBusy"}` while a job runs, and `{"status": "Healthy"}` otherwise. `time_of_last_update` holds the Unix time, in seconds, of the worker's start or of the last change of the status. It does not change between pings with the same status.
- The job thread sends heartbeats when the job sets a heartbeat timeout, and then the success or failure callback.

The worker remembers the callback IDs of accepted jobs only in its process memory. So it runs a job once per process. An answer can be lost after the job finished, and the retry can arrive after the idle timeout replaced the session's process. The new process then runs the same job again. So delivery is at-least-once across process replacement. Make the handler's side effects safe to repeat.

While a job runs, the worker answers `/ping` with `HealthyBusy`, so the agent runtime's idle timeout (`idleRuntimeSessionTimeout`) does not end the job. After the idle timeout, a session with no running job ends. A later request with the same session ID then gets a new microVM without an error, and the memory and files of the old one are lost. The `agentcore` operation sends one job per session, so this loss cannot affect a job.

### Rules for AgentCore jobs

1. Set `heartbeat_timeout` to at least 1 second. It ends the job when the worker crashes or stops reporting.
2. `timeout` must fit the agent runtime's configured `maxLifetime`.
3. A job can run more than once when AgentCore replaces the worker process. Make handler side effects safe to repeat.
4. Your logger must not throw.
5. The runtime's idle timeout applies only while no job is running. One `agentcore()` call runs one job, so it does not lose state between jobs.

The agent runtime's execution role needs `lambda:SendDurableExecutionCallbackSuccess`, `lambda:SendDurableExecutionCallbackFailure`, and `lambda:SendDurableExecutionCallbackHeartbeat` on the durable function's ARN. The function's execution role needs `bedrock-agentcore:InvokeAgentRuntime` and `bedrock-agentcore:StopRuntimeSession` on the agent runtime ARN and on `<runtime ARN>/runtime-endpoint/*`.

## Reference

- [docs/worker.md](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python-compute-worker/docs/worker.md): the request flow, the lifecycle hooks, idle suspend outcomes, the job documents, reporting, heartbeats, logging fields, and best practices.
- [extras](https://github.com/aws/aws-durable-execution-sdk-python/tree/main/packages/aws-durable-execution-sdk-python-extras): the `microvm` and `microvm_session` operations that send jobs.
- [Source](https://github.com/aws/aws-durable-execution-sdk-python/tree/main/packages/aws-durable-execution-sdk-python-compute-worker/src/aws_durable_execution_sdk_python_compute_worker/microvm): lower-level building blocks such as `MicrovmWorkerListener` and `CallbackReporter`.

## License

This project is licensed under the [Apache-2.0 License](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/LICENSE).
