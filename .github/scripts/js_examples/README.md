# JS examples against the local runner

The JS SDK repository ([aws/aws-durable-execution-sdk-js]) has about 125
example tests. Each test runs a durable function and checks its result and
history. The JS repository runs them against real Lambda. This directory runs
the same tests against the Python local runner in
`packages/aws-durable-execution-sdk-python-testing`. Everything runs on your
machine. No AWS account or credentials are needed.

Run it after you change the testing package. The same script runs in CI
(`.github/workflows/js-examples.yml`) on pull requests that touch the testing
package.

[aws/aws-durable-execution-sdk-js]: https://github.com/aws/aws-durable-execution-sdk-js

## Quick start

You need `git`, [hatch](https://hatch.pypa.io/), and Node.js 22 or newer with
`npm`. Run from the repository root:

```sh
hatch run dev-testing:js-examples           # every example, about 2.5 minutes
hatch run dev-testing:js-examples invoke    # only tests whose path matches "invoke"
```

`dev-testing` is the hatch environment for the testing package. It has the SDK
and the testing package from your checkout, installed in editable mode, so a
run always tests your current source.

To use a JS SDK checkout you already have, pass it with `--js-dir`, or set
`JS_SDK_DIR` once in your shell:

```sh
export JS_SDK_DIR=~/github/aws-durable-execution-sdk-js
hatch run dev-testing:js-examples invoke
```

The harness uses that checkout as it is. It does not fetch, switch branches or
clone. It runs `npm ci` and builds the example bundles when the checkout's
source differs from the last build: a new commit, or an uncommitted change
to a tracked or untracked file.

Without `--js-dir`, the harness clones the JS SDK's `main` branch into
`~/.cache/dex-js-examples/js-sdk` on the first run, and builds it. Each later
run fetches `main` again, and rebuilds only when `main` has moved.

The last lines of the output give the result:

```text
[js-examples] selected 125 test files: 121 passed (0 on retry), 0 failed, 4 have no cloud tests, 0 not run
```

The script exits 0 when every selected test passes and 1 otherwise. It exits
2 when setup fails, for example when the interpreter running it does not have
the testing package from this checkout.

## Common tasks

Each command below follows `hatch run dev-testing:js-examples`.

| Task | Options |
| --- | --- |
| Run tests whose path matches a regex | `step/ 'wait-for-callback/.*heartbeat'` |
| Test against another JS SDK branch, tag or commit | `--js-ref v1.2.0` |
| Use your own JS SDK checkout | `--js-dir ~/github/aws-durable-execution-sdk-js` |
| Skip the JS build for that checkout | `--js-dir ~/github/aws-durable-execution-sdk-js --no-build` |
| Force a JS rebuild | `--rebuild` |
| See runner debug logs | `--log-level DEBUG hello-world` |
| Pass options to jest | `invoke --jest-args='--verbose'` |
| All options | `--help` |

`--js-ref` works only with the cloned JS SDK, so it cannot be combined with
`--js-dir` or `JS_SDK_DIR`. With `--js-dir`, the harness writes only build
output into your checkout. Its lock and build stamp live in
`~/.cache/dex-js-examples`.

A run holds its JS checkout's lock until its tests finish. So a second run on
the same checkout waits for the first. Runs on different checkouts, for
example the cloned SDK and your own, run at the same time.

## When a test fails

Each run writes its files to a new directory under
`~/.cache/dex-js-examples/runs/`, so runs from two worktrees never overwrite
each other. `runs/latest` points at the newest run, and only the last 10 runs
are kept. With `--out`, a run writes to that directory instead. It empties the
directory first, so it refuses one that has files in it and was not created by
an earlier run.

| File | Contents |
| --- | --- |
| `report.json` | Passed, failed, flaky and skipped test files |
| `runner.log` | The Python local runner, which is the code under test |
| `shim.log` | One line per handler invocation: request ID, function, duration, outcome |
| `jest.json`, `retry1.json` | Raw jest results, for the first run and the retry |
| `maps/` | The function maps generated from the examples' `template.yml` |

To investigate one example, run it alone with debug logs:

```sh
hatch run dev-testing:js-examples --log-level DEBUG --retries 0 --jest-args='--verbose' step/interrupted-no-retry
```

Add `--dump-dir /tmp/dump` to also save every history, state and checkpoint
response the tests read.

A test file that fails is run once more on fresh servers. If it then passes,
the run succeeds and the file is reported as `FLAKY`. A few examples race real
timers, for example parallel branches that must checkpoint within
milliseconds of each other, and they can fail on a loaded machine. Use
`--retries 0` to see every failure.

## How it works

The Python local runner is the backend, in place of the Lambda service. Three
helpers run around it, on the loopback, on free ports chosen for each run:

```text
jest ----> invoke_proxy.py ----> local runner ----> lambda-shim.cjs
(test      (starts executions)   (the code under   (runs the JS handlers,
 driver)                          test)              like the Lambda runtime)
```

1. **jest** runs the JS example tests unchanged, in the mode the JS repository
   uses against real Lambda (`NODE_ENV=integration`). Each test is a client:
   it starts an execution, then reads history and state, and some tests send
   callbacks. `LAMBDA_ENDPOINT` points the tests' Lambda client at the proxy.
2. **invoke_proxy.py** exists because the tests start an execution with Lambda
   `Invoke`, and the local runner has no `Invoke` route. The proxy turns that
   call into the runner's `POST /start-durable-execution`, and returns the
   execution ARN in the header the JS SDK reads. It forwards every other
   request to the runner unchanged.
3. **The local runner** runs the executions. To run a handler, it calls
   Lambda `Invoke` on its Lambda endpoint, which is the shim. It resolves
   chained-invoke targets from the function configs `run.py` gives it.
4. **lambda-shim.cjs** hosts the built example bundles, as the Lambda runtime
   does in AWS. It runs each function on worker threads. A worker handles one
   invocation at a time and is reused afterwards, like a warm Lambda sandbox.
   When an invocation runs past the function's `Timeout`, the shim returns
   Lambda's `Sandbox.Timedout` error and terminates the worker.
   `step/interrupted-no-retry` depends on this. Inside the handler, the durable
   SDK checkpoints straight to the runner, because the shim sets
   `AWS_ENDPOINT_URL_LAMBDA` to the runner's address.

`run.py` starts the runner with `WebRunner` from the testing package, in a
child process, so each server session gets a runner with no state left from
an earlier one. A run has one session, plus one per retry. The proxy runs on a
thread in `run.py`, and the shim and jest run as separate Node processes.
Before the run, `run.py` reads the examples' `template.yml` and writes three maps: test file to
function name (for jest), function name to bundle, timeout and environment
(for the shim), and function name to `DurableConfig` (for the runner).

The runner emulates region `us-west-2` and account `123456789012`. The proxy
and the shim use the same values. If they differ, the runner rejects chained
invokes whose target ARN names another region or account.

## Which tests run

Every `*.test.ts` under the examples' `src/examples` is selected, except for
three groups:

- `otel/` examples. They export spans to an OpenTelemetry collector, which the
  harness does not run.
- Examples marked `localOnly` in the JS catalog. `template.yml` omits them,
  and the JS repository's integration run skips them too. The output reports
  how many were skipped.
- Tests without "cloud" in their name. The integration jest config runs only
  those. A file with no such test is reported as "no cloud tests".

## Known failures

`known-failures.txt` lists test files that are expected to fail, one per line,
with the reason after `#`. It is empty today. A listed file that fails does
not fail the run. A listed file that passes does fail the run, so that the
list stays accurate.

## A failure caused by a JS change

Every run tests against the JS SDK's `main` branch. So a run can fail because
of a new JS commit, with no Python change. To check, run the failing test
against an older JS commit:

```sh
hatch run dev-testing:js-examples --js-ref <older JS commit> <failing test>
```

If it passes there, the new JS commit exposed something the runner does not
handle yet.

## Harness tests

`.github/scripts/tests/test_js_examples.py` tests the template parsing, test
selection and result handling in `run.py`. It needs no JS SDK:

```sh
hatch run dev-testing:python -m pytest .github/scripts/tests/test_js_examples.py
```
