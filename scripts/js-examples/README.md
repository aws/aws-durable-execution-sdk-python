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

You need `git`, Python 3.11 or newer, and Node.js 22 or newer with `npm`.

```sh
scripts/js-examples/run.sh            # every example, about 2.5 minutes
scripts/js-examples/run.sh invoke     # only tests whose path matches "invoke"
```

The first run takes a few more minutes. It does two setup steps:

1. It creates `scripts/js-examples/.venv` and installs the SDK and the testing
   package from your checkout. The install is editable, so later runs use
   your current source without reinstalling.
2. It clones the JS SDK into `~/.cache/dex-js-examples/js-sdk` at the commit
   in `js-sdk.ref`, then runs `npm ci` and builds the example bundles. Later
   runs reuse the build until the commit changes.

The last lines of the output give the result:

```text
[js-examples] selected 125 test files: 121 passed (0 on retry), 0 failed, 4 have no cloud tests, 0 not run
```

The script exits 0 when every selected test passes and 1 otherwise. It exits
2 when setup fails.

## Common tasks

| Task | Command |
| --- | --- |
| Run tests whose path matches a regex | `run.sh step/ 'wait-for-callback/.*heartbeat'` |
| Test against another JS SDK branch, tag or commit | `run.sh --js-ref main` |
| Use your own JS SDK checkout | `run.sh --js-dir ~/src/aws-durable-execution-sdk-js` |
| Skip the JS build for that checkout | `run.sh --js-dir ~/src/aws-durable-execution-sdk-js --no-build` |
| Force a JS rebuild | `run.sh --rebuild` |
| See runner debug logs | `run.sh --log-level DEBUG hello-world` |
| Pass options to jest | `run.sh invoke -- --verbose` |
| All options | `run.sh --help` |

With `--js-dir`, the harness builds your checkout in place and changes nothing
else in it. Its lock and build stamp live in `~/.cache/dex-js-examples`.

## When a test fails

Each run writes its files to `scripts/js-examples/.out/`, or to the directory
given with `--out`:

| File | Contents |
| --- | --- |
| `report.json` | Passed, failed, flaky and skipped test files |
| `runner.log` | The Python local runner, which is the code under test |
| `shim.log` | One line per handler invocation: request ID, function, duration, outcome |
| `proxy.log` | The invoke proxy |
| `jest-*.json` | Raw jest results |
| `maps/` | The function maps generated from the examples' `template.yml` |

To investigate one example, run it alone with debug logs:

```sh
scripts/js-examples/run.sh --log-level DEBUG --retries 0 step/interrupted-no-retry -- --verbose
```

Set `PROXY_DUMP_DIR=/tmp/dump` to also save every history, state and
checkpoint response the tests read.

A test file that fails is run once more on fresh servers. If it then passes,
the run succeeds and the file is reported as `FLAKY`. A few examples race real
timers, for example parallel branches that must checkpoint within
milliseconds of each other, and they can fail on a loaded machine. Use
`--retries 0` to see every failure.

## How it works

Four processes run on the loopback, on free ports chosen for each run:

```text
jest (CloudDurableTestRunner)
  -> invoke_proxy.py   turns Lambda Invoke into POST /start-durable-execution;
                       forwards every other request unchanged
  -> local runner      from this checkout: the code under test
  -> lambda-shim.cjs   answers the runner's Invoke calls by running the
                       built example bundles
```

1. jest runs the examples in integration mode (`NODE_ENV=integration`), the
   mode the JS repository uses against real Lambda. The JS test runner starts
   each execution with Lambda `Invoke` and reads the execution ARN from the
   response. `invoke_proxy.py` gives the local runner that endpoint.
2. The local runner invokes handlers at its `--lambda-endpoint`, which is the
   shim. It resolves chained-invoke targets with `--function-configs`.
3. `lambda-shim.cjs` implements Lambda `Invoke` for the example bundles. It
   runs each function on worker threads. A worker handles one invocation at a
   time and is reused afterwards, like a warm Lambda sandbox. When an
   invocation runs past the function's `Timeout`, the shim returns Lambda's
   `Sandbox.Timedout` error and terminates the worker. Some examples depend
   on this, for example `step/interrupted-no-retry`.
4. `run.py` reads the examples' `template.yml` and writes the three maps the
   other processes need: test file to function name (for jest), function
   name to bundle, timeout and environment (for the shim), and function name
   to `DurableConfig` (for the runner).

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

## Updating the pinned JS SDK

Pull requests test against the commit in `js-sdk.ref`. So a failure on a pull
request points at the Python change, not at a new JS commit. The weekly CI
run tests against JS `main` instead. When it is green, move the pin:

```sh
git ls-remote https://github.com/aws/aws-durable-execution-sdk-js.git refs/heads/main
# write that commit into scripts/js-examples/js-sdk.ref, then:
scripts/js-examples/run.sh
```

## Harness tests

`test_run.py` tests the template parsing, test selection and result handling
in `run.py`. It needs no JS SDK:

```sh
scripts/js-examples/.venv/bin/python -m pytest scripts/js-examples
```
