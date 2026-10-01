#!/usr/bin/env python3
"""Run the JS SDK's example test suite against the Python local runner.

The JS SDK repository has about 125 example test files. Each one deploys a
durable function and asserts on its result and history. In the JS repo's CI
they run against real Lambda. This script runs them against the Python
testing package's web runner instead, with every process on the loopback:

    jest (CloudDurableTestRunner)
      -> invoke_proxy.py   Lambda Invoke becomes POST /start-durable-execution;
                           every other request is forwarded unchanged
      -> web runner        the code under test, from this checkout
      -> lambda-shim.cjs   answers the runner's Invoke calls by running the
                           built example bundles, one worker thread each

Run it with an interpreter that has this checkout's testing package
installed, for example through hatch:

    hatch run dev-testing:js-examples [options] [patterns]

See README.md in this directory for usage.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import importlib.util
import json
import logging
import multiprocessing
import os
import re
import resource
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
import zlib
from dataclasses import dataclass
from multiprocessing.synchronize import Event as EventType
from pathlib import Path
from typing import Any

import yaml

import invoke_proxy

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[2]
TESTING_PACKAGE = "aws_durable_execution_sdk_python_testing"
TESTING_SOURCE = REPO_ROOT / "packages/aws-durable-execution-sdk-python-testing/src"
JS_SDK_URL = "https://github.com/aws/aws-durable-execution-sdk-js.git"
EXAMPLES_REL = Path("packages/aws-durable-execution-sdk-js-examples")
# The examples depend on these workspaces by "*". The root "npm run build"
# also builds the insight tools and the VS Code extension, which the harness
# does not need, so the script builds only these, in dependency order.
JS_BUILD_WORKSPACES = (
    "packages/aws-durable-execution-sdk-js",
    "packages/aws-durable-execution-sdk-js-testing",
    "packages/aws-durable-execution-sdk-js-otel",
    "packages/aws-durable-execution-sdk-js-examples",
)
# otel examples export spans to an OpenTelemetry collector. The harness does
# not run one, so these examples cannot pass here. They are not selected.
EXCLUDED_DIRS = ("/otel/",)
REGION = "us-west-2"
MIN_NODE_MAJOR = 22


def log(msg: str) -> None:
    print(f"[js-examples] {msg}", flush=True)


class HarnessError(Exception):
    """A setup problem. main() prints it and exits with status 2."""


# ---------------------------------------------------------------------------
# JS SDK checkout and build
# ---------------------------------------------------------------------------


def cache_dir() -> Path:
    base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
    return Path(base) / "dex-js-examples"


def git(cwd: Path, *args: str, capture: bool = False) -> str:
    result = subprocess.run(
        ["git", "-c", "advice.detachedHead=false", *args],
        cwd=cwd,
        check=True,
        text=True,
        stdout=subprocess.PIPE if capture else None,
    )
    return (result.stdout or "").strip()


def checkout_js_sdk(js_dir: Path, ref: str) -> None:
    """Make js_dir a checkout of ref, cloning on first use.

    The fetch is shallow and works for a branch, a tag, or a full commit SHA.
    """
    if not (js_dir / ".git").exists():
        log(f"cloning {JS_SDK_URL} into {js_dir}")
        js_dir.mkdir(parents=True, exist_ok=True)
        git(js_dir, "init", "--quiet")
        git(js_dir, "remote", "add", "origin", JS_SDK_URL)
    if re.fullmatch(r"[0-9a-f]{40}", ref) and _head(js_dir) == ref:
        return
    log(f"fetching JS SDK ref {ref}")
    git(js_dir, "fetch", "--quiet", "--depth", "1", "origin", ref)
    git(js_dir, "checkout", "--quiet", "--force", "--detach", "FETCH_HEAD")


def _head(js_dir: Path) -> str | None:
    try:
        return git(js_dir, "rev-parse", "HEAD", capture=True)
    except subprocess.CalledProcessError:
        return None


def state_file(js_dir: Path, kind: str) -> Path:
    """A per-checkout file in the cache directory, for locks and build stamps."""
    cache_dir().mkdir(parents=True, exist_ok=True)
    return cache_dir() / f"{kind}-{zlib.crc32(str(js_dir).encode()):08x}"


def source_fingerprint(js_dir: Path) -> str:
    """Identify the JS source a build would use: the commit plus local edits.

    The commit alone is not enough. A developer edits a --js-dir checkout
    without committing, and the build must then run again. So the
    fingerprint also hashes the uncommitted changes to tracked files and the
    content of untracked files. Ignored files, such as node_modules and dist,
    are not part of it.
    """
    head = _head(js_dir) or "unknown"
    digest = hashlib.sha256()
    digest.update(
        subprocess.run(
            ["git", "diff", "HEAD", "--binary"],
            cwd=js_dir,
            check=True,
            capture_output=True,
        ).stdout
    )
    untracked = git(
        js_dir, "ls-files", "--others", "--exclude-standard", "-z", capture=True
    )
    for name in sorted(filter(None, untracked.split("\0"))):
        digest.update(name.encode() + b"\0")
        with contextlib.suppress(OSError):
            digest.update((js_dir / name).read_bytes())
    return f"{head} {digest.hexdigest()}"


def build_js_sdk(js_dir: Path, *, force: bool) -> None:
    """Install and build the JS workspaces the examples need.

    A stamp file records the source fingerprint of the last build. The build
    is skipped when the stamp matches, so repeat runs start in seconds.
    """
    head = _head(js_dir) or "unknown"
    stamp = state_file(js_dir, "built")
    dist = js_dir / EXAMPLES_REL / "dist"
    if (
        not force
        and stamp.exists()
        and stamp.read_text().strip() == source_fingerprint(js_dir)
        and dist.is_dir()
    ):
        log(f"JS SDK already built at {head[:12]}")
        return
    log(
        f"building JS SDK at {head[:12]} (first run for this commit takes a few minutes)"
    )
    # Some workspaces the examples do not use have install scripts that
    # download or compile native code. Electron downloads a ~100 MB binary.
    # node-llama-cpp (an insight dependency) builds llama.cpp from source when
    # its prebuilt binary does not fit the host, and that build fails on a
    # machine without a C++ toolchain. The harness needs neither, so both
    # install steps are skipped.
    env = {
        **os.environ,
        "ELECTRON_SKIP_BINARY_DOWNLOAD": "1",
        "NODE_LLAMA_CPP_POSTINSTALL": "skip",
        "HUSKY": "0",
    }
    npm = shutil.which("npm") or "npm"
    subprocess.run(
        [npm, "ci", "--no-audit", "--no-fund"], cwd=js_dir, env=env, check=True
    )
    for workspace in JS_BUILD_WORKSPACES:
        log(f"npm run build -w {workspace}")
        subprocess.run(
            [npm, "run", "build", "-w", workspace], cwd=js_dir, env=env, check=True
        )
    # The build regenerates tracked files such as template.yml. So the
    # fingerprint is taken after the build, from the state it leaves.
    stamp.write_text(source_fingerprint(js_dir) + "\n")


def check_python() -> None:
    """Fail unless this interpreter has the testing package from this checkout.

    The runner is started with this interpreter. So a testing package
    installed from PyPI, or from another checkout, would be tested instead of
    this one, and the run would still report success.
    """
    spec = importlib.util.find_spec(TESTING_PACKAGE)
    origin = Path(spec.origin).resolve() if spec and spec.origin else None
    # The source directory itself, not just the repository: a wheel from PyPI
    # installed into a virtual environment inside the checkout is under the
    # repository too.
    if origin is None or not origin.is_relative_to(TESTING_SOURCE):
        found = f" It imports {origin.parent}." if origin else ""
        raise HarnessError(
            f"{sys.executable} does not have this checkout's testing package.{found}"
            " Run: hatch run dev-testing:js-examples"
        )


def check_node() -> None:
    node = shutil.which("node")
    if node is None:
        raise HarnessError(
            f"node is not on PATH. Install Node.js {MIN_NODE_MAJOR} or newer."
        )
    version = subprocess.run(
        [node, "--version"], capture_output=True, text=True, check=True
    ).stdout
    major = int(version.strip().lstrip("v").split(".")[0])
    if major < MIN_NODE_MAJOR:
        raise HarnessError(
            f"Node.js {version.strip()} found; {MIN_NODE_MAJOR} or newer is required."
        )


# ---------------------------------------------------------------------------
# Function maps from template.yml
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FunctionMaps:
    # Handler file basename -> FunctionName. jest reads it from FUNCTION_NAME_MAP.
    name_map: dict[str, str]
    # FunctionName -> bundle, export, timeout, environment. The shim reads it.
    shim_map: dict[str, dict[str, Any]]
    # FunctionName -> {} or {"DurableConfig": {...}}. The runner reads it from
    # --function-configs to resolve chained-invoke targets.
    function_configs: dict[str, dict[str, Any]]


def build_function_maps(template_path: Path) -> FunctionMaps:
    """Derive the three maps from the examples' SAM template.

    The template uses only long-form intrinsics (Fn::GetAtt), so safe_load
    parses it. Globals are not used by the generated template, so each
    function's Properties are complete.
    """
    with template_path.open(encoding="utf-8") as fh:
        template = yaml.safe_load(fh)
    name_map: dict[str, str] = {}
    shim_map: dict[str, dict[str, Any]] = {}
    function_configs: dict[str, dict[str, Any]] = {}
    for logical_id, resource_def in template["Resources"].items():
        if resource_def.get("Type") != "AWS::Serverless::Function":
            continue
        props = resource_def["Properties"]
        function_name = props["FunctionName"]
        file_base, _, export = props["Handler"].partition(".")
        variables = dict((props.get("Environment") or {}).get("Variables") or {})
        # The template points the SDK at a runner reached from a SAM Docker
        # container. The shim sets the real runner address instead.
        variables.pop("AWS_ENDPOINT_URL_LAMBDA", None)
        if file_base in name_map:
            raise HarnessError(f"{logical_id}: handler file {file_base} is used twice")
        name_map[file_base] = function_name
        shim_map[function_name] = {
            "file": file_base,
            "export": export or "handler",
            "timeoutSeconds": int(props.get("Timeout", 3)),
            "environment": {k: str(v) for k, v in variables.items()},
        }
        durable = props.get("DurableConfig")
        function_configs[function_name] = (
            {"DurableConfig": dict(durable)} if durable else {}
        )
    if not name_map:
        raise HarnessError(f"no AWS::Serverless::Function resources in {template_path}")
    return FunctionMaps(name_map, shim_map, function_configs)


# ---------------------------------------------------------------------------
# Test selection
# ---------------------------------------------------------------------------


def select_tests(
    examples: Path,
    patterns: list[str],
    shard: tuple[int, int] | None,
    maps: FunctionMaps,
) -> tuple[list[str], list[str]]:
    """Return (tests to run, tests skipped as not deployed).

    Paths are relative to the examples package. A pattern is a regular
    expression matched against the path, as jest matches its positional
    patterns. With no patterns, every test outside EXCLUDED_DIRS is selected.

    template.yml omits examples marked localOnly in the JS catalog. The JS
    test helper skips the cloud tests of such an example, and the JS repo's
    integration run does not deploy them. So a test whose handler file has no
    function in the template is skipped here too, not run.

    A shard i/N takes every Nth runnable file of the sorted list, starting at
    file i.
    """
    all_tests = sorted(
        str(p.relative_to(examples))
        for p in (examples / "src/examples").rglob("*.test.ts")
    )
    tests = [t for t in all_tests if not any(d in f"/{t}" for d in EXCLUDED_DIRS)]
    if patterns:
        regexes = [re.compile(p) for p in patterns]
        tests = [t for t in tests if any(r.search(t) for r in regexes)]
    deployed = [
        t for t in tests if Path(t).name.removesuffix(".test.ts") in maps.name_map
    ]
    not_deployed = [t for t in tests if t not in deployed]
    if shard is not None:
        index, count = shard
        deployed = deployed[index - 1 :: count]
    return deployed, not_deployed


def check_bundles_exist(tests: list[str], maps: FunctionMaps, dist: Path) -> None:
    """Fail early when a selected test's bundle was not built."""
    missing = [
        test
        for test in tests
        if not (dist / f"{Path(test).name.removesuffix('.test.ts')}.js").exists()
    ]
    if missing:
        raise HarnessError(
            "bundles missing from dist/ (run with --rebuild):\n  "
            + "\n  ".join(missing)
        )


# ---------------------------------------------------------------------------
# Servers
# ---------------------------------------------------------------------------


def free_port() -> int:
    """Ask the OS for an unused loopback port.

    Every run picks new ports. So a server left behind by an earlier run can
    never answer this run's requests, and two runs can share a machine.
    """
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
        return port


def serve_runner(
    port: int,
    shim_url: str,
    function_configs: Path,
    skip_time: bool,
    log_level: str,
    log_file: Path,
    env: dict[str, str],
    ready: EventType,
) -> None:
    """Run the local runner until the process is terminated.

    This is the target of the runner's child process. It takes plain values
    because the child is started with the "spawn" method, which pickles the
    arguments. It sets ready once the runner is listening.
    """
    # Imported here, not at module level: the harness unit tests import this
    # module without the testing package installed.
    from aws_durable_execution_sdk_python_testing.child_dispatcher import (  # noqa: PLC0415
        FunctionConfigs,
    )
    from aws_durable_execution_sdk_python_testing.runner import (  # noqa: PLC0415
        WebRunner,
        WebRunnerConfig,
    )
    from aws_durable_execution_sdk_python_testing.web.server import (  # noqa: PLC0415
        WebServiceConfig,
    )

    os.environ.clear()
    os.environ.update(env)
    level = logging.getLevelNamesMapping()[log_level.upper()]
    logging.basicConfig(
        filename=log_file,
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logging.getLogger("botocore").setLevel(logging.WARNING)
    config = WebRunnerConfig(
        web_service=WebServiceConfig(host="127.0.0.1", port=port, log_level=level),
        lambda_endpoint=shim_url,
        local_runner_endpoint=f"http://127.0.0.1:{port}",
        local_runner_region=REGION,
        skip_time=skip_time,
        function_configs=FunctionConfigs.from_value(f"file://{function_configs}"),
    )
    try:
        with WebRunner(config) as runner:
            ready.set()
            runner.serve_forever()
    except Exception:
        logging.getLogger(__name__).exception("runner failed")
        raise


class Servers:
    """Starts the runner, the proxy and the shim, and stops all three.

    1. The runner runs in a child process, started with multiprocessing. So
       every server session gets a runner with no state left from an earlier
       session, for example when failed tests are retried.
    2. The proxy runs on a thread in this process. It keeps no state.
    3. The shim is Node, so it runs as a separate program.
    """

    def __init__(
        self,
        out: Path,
        maps_dir: Path,
        examples: Path,
        skip_time: bool,
        log_level: str,
        dump_dir: Path | None,
    ):
        self.out = out
        self.maps_dir = maps_dir
        self.examples = examples
        self.skip_time = skip_time
        self.log_level = log_level
        self.dump_dir = dump_dir
        self.runner: multiprocessing.process.BaseProcess | None = None
        self.proxy: invoke_proxy.ProxyServer | None = None
        self.shim: subprocess.Popen[bytes] | None = None
        self.proxy_url = ""

    def start(self) -> None:
        runner_port, shim_port = free_port(), free_port()
        runner_url = f"http://127.0.0.1:{runner_port}"
        shim_url = f"http://127.0.0.1:{shim_port}"

        # "spawn" starts a fresh interpreter. The default on Linux before
        # Python 3.14 is "fork", which is unsafe here because this process
        # already runs threads (the proxy, from an earlier session).
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        self.runner = context.Process(
            target=serve_runner,
            name="js-examples-runner",
            args=(
                runner_port,
                shim_url,
                self.maps_dir / "function-configs.json",
                self.skip_time,
                self.log_level,
                self.out / "runner.log",
                harness_env(),
                ready,
            ),
        )
        self.runner.start()

        function_configs = json.loads(
            (self.maps_dir / "function-configs.json").read_text()
        )
        self.proxy = invoke_proxy.ProxyServer(
            0, runner_url, self.dump_dir, function_configs
        )
        self.proxy_url = f"http://127.0.0.1:{self.proxy.server_port}"
        threading.Thread(target=self.proxy.serve_forever, daemon=True).start()

        with (self.out / "shim.log").open("ab") as logfile:
            # A new session makes the shim a process group leader, so
            # stop_group() also ends anything it started.
            self.shim = subprocess.Popen(
                [
                    "node",
                    str(HERE / "lambda-shim.cjs"),
                    "--port",
                    str(shim_port),
                    "--map",
                    str(self.maps_dir / "shim-map.json"),
                    "--dist",
                    str(self.examples / "dist"),
                    "--runner",
                    runner_url,
                ],
                stdout=logfile,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
                env=harness_env(),
            )

        self._wait_runner(ready)
        self._wait_shim(f"{shim_url}/health")

    def _wait_runner(self, ready: EventType, timeout: float = 60) -> None:
        assert self.runner is not None
        deadline = time.monotonic() + timeout
        while not ready.wait(0.25):
            if not self.runner.is_alive():
                raise HarnessError(
                    f"runner exited with status {self.runner.exitcode};"
                    f" see {self.out / 'runner.log'}"
                )
            if time.monotonic() > deadline:
                raise HarnessError(f"runner did not start within {timeout:.0f}s")

    def _wait_shim(self, url: str, timeout: float = 60) -> None:
        assert self.shim is not None
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.shim.poll() is not None:
                raise HarnessError(
                    f"shim exited with status {self.shim.returncode};"
                    f" see {self.out / 'shim.log'}"
                )
            with contextlib.suppress(OSError):
                with urllib.request.urlopen(url, timeout=2):  # noqa: S310
                    return
            time.sleep(0.25)
        raise HarnessError(f"shim did not answer {url} within {timeout:.0f}s")

    def stop(self) -> None:
        if self.shim is not None:
            stop_group(self.shim)
            self.shim = None
        if self.proxy is not None:
            self.proxy.shutdown()
            self.proxy.server_close()
            self.proxy = None
        if self.runner is not None:
            # The runner starts no processes of its own: it reaches handlers
            # over HTTP, at the shim. So ending this one process is enough.
            self.runner.terminate()
            self.runner.join(5)
            if self.runner.is_alive():
                self.runner.kill()
                self.runner.join()
            self.runner = None


def stop_group(proc: subprocess.Popen[bytes]) -> None:
    """Stop a process started with start_new_session, and its children.

    Sends SIGTERM to the process group, waits up to 5 seconds, then sends
    SIGKILL. The group is signalled even after the leader exits, because a
    child such as a jest worker can outlive it.
    """
    with contextlib.suppress(ProcessLookupError):
        os.killpg(proc.pid, signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    with contextlib.suppress(ProcessLookupError):
        os.killpg(proc.pid, signal.SIGKILL)
    proc.wait()


def harness_env() -> dict[str, str]:
    """Environment for every child process.

    Nothing leaves the machine. The runner signs its local Invoke calls with
    boto, so credentials must exist, and any value works. A developer's real
    AWS profile or endpoint overrides would redirect those calls, so the
    harness removes them.
    """
    env = {
        k: v
        for k, v in os.environ.items()
        if not (k.startswith("AWS_") or k == "LOG_LEVEL")
    }
    env.update(
        AWS_ACCESS_KEY_ID="test",
        AWS_SECRET_ACCESS_KEY="test",
        AWS_REGION=REGION,
        AWS_DEFAULT_REGION=REGION,
    )
    return env


# ---------------------------------------------------------------------------
# jest
# ---------------------------------------------------------------------------


def run_jest(
    examples: Path,
    tests: list[str],
    servers: Servers,
    maps_dir: Path,
    results_file: Path,
    workers: int,
    extra: list[str],
) -> dict[str, Any]:
    env = harness_env()
    env.update(
        NODE_ENV="integration",
        LAMBDA_ENDPOINT=servers.proxy_url,
        FUNCTION_NAME_MAP=(maps_dir / "function-name-map.json").read_text(),
    )
    # The AWS SDK client in CloudDurableTestRunner loads modules with a
    # dynamic import(). jest's CommonJS runtime needs this flag for that.
    node_options = env.get("NODE_OPTIONS", "")
    if "--experimental-vm-modules" not in node_options:
        env["NODE_OPTIONS"] = f"{node_options} --experimental-vm-modules".strip()
    cmd = [
        "npx",
        "--no-install",
        "jest",
        "--config",
        "jest.config.integration.js",
        # The config sets bail: true. The harness wants every result.
        "--bail=0",
        f"--maxWorkers={workers}",
        "--json",
        f"--outputFile={results_file}",
        *extra,
        "--runTestsByPath",
        *tests,
    ]
    # jest runs in its own process group, like the servers. A Ctrl-C reaches
    # this script, and the finally block stops jest and all its workers.
    proc = subprocess.Popen(cmd, cwd=examples, env=env, start_new_session=True)
    try:
        status = proc.wait()
    finally:
        stop_group(proc)
    if not results_file.exists():
        raise HarnessError(
            f"jest wrote no results to {results_file}; see its output above"
        )
    results: dict[str, Any] = json.loads(results_file.read_text())
    # summarize() reads the verdict from the JSON, but jest can also exit
    # non-zero for an error no suite reports, for example in global teardown.
    results[EXIT_STATUS_KEY] = status
    return results


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


EXIT_STATUS_KEY = "jsExamplesExitStatus"


def unexplained_exits(results: list[dict[str, Any]]) -> list[int]:
    """Non-zero jest exit statuses that no failed suite in that run explains."""
    statuses = []
    for result in results:
        status = result.get(EXIT_STATUS_KEY, 0)
        suites_failed = any(
            suite["status"] == "failed"
            or any(a["status"] == "failed" for a in suite.get("assertionResults", []))
            for suite in result.get("testResults", [])
        )
        if status != 0 and not suites_failed:
            statuses.append(status)
    return statuses


def load_known_failures() -> dict[str, str]:
    """Read known-failures.txt: one test path per line, then '#' and the reason."""
    path = HERE / "known-failures.txt"
    known: dict[str, str] = {}
    if not path.exists():
        return known
    for line in path.read_text().splitlines():
        entry, _, reason = line.partition("#")
        if entry.strip():
            known[entry.strip()] = reason.strip()
    return known


def outcomes(results: list[dict[str, Any]], examples: Path) -> dict[str, str]:
    """Map each test file to "passed", "failed" or "skipped".

    results are in run order, and a retry comes after the first attempt. A
    retry's pass or failure replaces the earlier outcome. A retry in which
    every test was skipped proves nothing, so it never replaces one.
    """
    outcome: dict[str, str] = {}
    for result in results:
        for suite in result.get("testResults", []):
            rel = str(Path(suite["name"]).resolve().relative_to(examples.resolve()))
            statuses = [a["status"] for a in suite.get("assertionResults", [])]
            if suite["status"] == "failed" or "failed" in statuses:
                outcome[rel] = "failed"
            elif "passed" not in statuses:
                # The file has no cloud tests: the integration jest config
                # runs only tests whose name contains "cloud". The JS repo's
                # integration run skips these files the same way.
                outcome.setdefault(rel, "skipped")
            else:
                outcome[rel] = "passed"
    return outcome


def failed_tests(results: list[dict[str, Any]], examples: Path) -> list[str]:
    known = load_known_failures()
    return sorted(
        t
        for t, s in outcomes(results, examples).items()
        if s == "failed" and t not in known
    )


def summarize(
    results: list[dict[str, Any]],
    examples: Path,
    selected: list[str],
    out: Path,
    retried: list[str],
) -> int:
    """Print the outcome, write report.json, and return the exit status.

    The run fails when:
      1. a test file fails, after retries, and is not in known-failures.txt,
      2. a listed file passes, so the list is out of date,
      3. jest reported fewer files than were selected,
      4. no test passed at all, or
      5. jest exited non-zero although every suite in its results passed.
    Checks 3 and 4 catch a run that silently executed nothing.
    A file that failed and then passed on retry does not fail the run. It is
    reported as flaky.
    """
    outcome = outcomes(results, examples)
    known = load_known_failures()
    passed = sorted(t for t, s in outcome.items() if s == "passed")
    skipped = sorted(t for t, s in outcome.items() if s == "skipped")
    failed = sorted(t for t, s in outcome.items() if s == "failed")
    flaky = [t for t in retried if outcome.get(t) == "passed"]
    unexpected = [t for t in failed if t not in known]
    stale = [t for t in passed if t in known]
    missing = sorted(set(selected) - set(outcome))
    jest_errors = unexplained_exits(results)

    (out / "report.json").write_text(
        json.dumps(
            {
                "passed": passed,
                "failed": failed,
                "flaky": flaky,
                "skipped_no_cloud_tests": skipped,
                "unexpected_failures": unexpected,
                "known_failures_now_passing": stale,
                "not_run": missing,
                "jest_exit_errors": jest_errors,
            },
            indent=2,
        )
        + "\n"
    )

    print()
    log(
        f"selected {len(selected)} test files: {len(passed)} passed "
        f"({len(flaky)} on retry), {len(failed)} failed, "
        f"{len(skipped)} have no cloud tests, {len(missing)} not run"
    )
    for t in flaky:
        log(f"FLAKY {t} failed, then passed on retry")
    for t in failed:
        note = f"  (known: {known[t]})" if t in known else ""
        log(f"FAIL {t}{note}")
    for t in stale:
        log(f"PASS {t} is listed in known-failures.txt; remove it from the list")
    for t in missing:
        log(f"NOT RUN {t}")
    for status in jest_errors:
        log(
            f"jest exited with status {status} although every suite passed; see its output above"
        )
    log(f"logs and report.json: {out}")
    if not passed:
        log("no test passed; treating the run as failed")
        return 1
    return 1 if (unexpected or stale or missing or jest_errors) else 0


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def parse_shard(value: str) -> tuple[int, int]:
    match = re.fullmatch(r"(\d+)/(\d+)", value)
    if not match or not 1 <= int(match[1]) <= int(match[2]):
        raise argparse.ArgumentTypeError("shard must look like 2/4")
    return int(match[1]), int(match[2])


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="run.py",
        description="Run the JS SDK example tests against this checkout's local runner.",
    )
    parser.add_argument(
        "patterns",
        nargs="*",
        metavar="PATTERN",
        help="regex matched against test paths, e.g. 'invoke' or 'step/'. Default: all tests.",
    )
    parser.add_argument(
        "--js-ref",
        help="JS SDK branch, tag or commit to test with (default: main).",
    )
    parser.add_argument(
        "--js-dir",
        type=Path,
        default=os.environ.get("JS_SDK_DIR") or None,
        help="use this JS SDK checkout as it is, instead of cloning. It is built"
        " unless --no-build. Default: $JS_SDK_DIR if set.",
    )
    parser.add_argument(
        "--no-build", action="store_true", help="skip npm ci and the JS builds"
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="build the JS SDK even if it looks up to date",
    )
    parser.add_argument(
        "--shard", type=parse_shard, metavar="I/N", help="run only shard I of N"
    )
    parser.add_argument(
        "--workers", type=int, default=4, help="jest --maxWorkers (default 4)"
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=1,
        help="run failed test files again this many times (default 1). A pass on retry is reported as flaky.",
    )
    parser.add_argument(
        "--skip-time",
        action="store_true",
        help="start the runner with --skip-time. Faster, but some examples assert real timing and fail.",
    )
    parser.add_argument(
        "--log-level", default="WARNING", help="runner log level (default WARNING)"
    )
    parser.add_argument(
        "--out",
        type=Path,
        help="logs and report directory. Default: a new directory per run under"
        f" {runs_dir()}, with {runs_dir() / 'latest'} pointing at the newest",
    )
    parser.add_argument(
        "--dump-dir",
        type=Path,
        help="save every history, state and checkpoint response the tests read here",
    )
    parser.add_argument(
        "--jest-args",
        type=shlex.split,
        default=[],
        metavar="ARGS",
        help="extra jest arguments, as one quoted string, e.g. --jest-args='--verbose'",
    )
    args = parser.parse_args(argv)
    if args.js_dir and args.js_ref:
        parser.error(
            "--js-ref needs the cloned JS SDK; it cannot be used with --js-dir or $JS_SDK_DIR"
        )
    return args


def raise_fd_limit() -> None:
    # Hundreds of short-lived sockets are open at once during a run.
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target = 8192 if hard == resource.RLIM_INFINITY else min(8192, hard)
    if soft < target:
        resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    check_python()
    check_node()
    raise_fd_limit()

    js_dir = (
        args.js_dir.expanduser().resolve() if args.js_dir else cache_dir() / "js-sdk"
    )
    # The lock and the build stamp live in the cache directory. So the harness
    # writes nothing into a checkout passed with --js-dir except build output.
    lock_path = state_file(js_dir, "lock")

    # Two runs may use the same JS checkout, and one of them may check out
    # another ref or rebuild. So a run holds the checkout's lock, exclusively,
    # from checkout through the end of its tests. A second run on the same
    # checkout waits. Changing the lock to shared after the build would not be
    # safe: flock releases the lock before taking it again in the new mode, so
    # another run could check out a different ref in between.
    with lock_path.open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            log(f"waiting for another run that is using {js_dir}")
            fcntl.flock(lock, fcntl.LOCK_EX)
        if not args.js_dir:
            ref = args.js_ref or "main"
            checkout_js_sdk(js_dir, ref)
        elif not (js_dir / EXAMPLES_REL).is_dir():
            raise HarnessError(f"{js_dir} is not a JS SDK checkout")
        if not args.no_build:
            build_js_sdk(js_dir, force=args.rebuild)
        return run(args, js_dir / EXAMPLES_REL)


OUT_MARKER = ".js-examples-out"


def prepare_out(out: Path) -> Path:
    """Empty the output directory, or create it, and return it.

    --out takes any path, and emptying it deletes files. So an existing
    directory is emptied only when it is empty already or an earlier run made
    it, which the marker file shows. Anything else, such as the checkout or a
    home directory passed by mistake, is refused.
    """
    if out.exists():
        if not out.is_dir():
            raise HarnessError(f"--out {out} exists and is not a directory")
        if any(out.iterdir()) and not (out / OUT_MARKER).is_file():
            raise HarnessError(
                f"--out {out} is not empty and was not created by this harness;"
                " choose another directory"
            )
        shutil.rmtree(out)
    out.mkdir(parents=True)
    (out / OUT_MARKER).write_text("Created by .github/scripts/js_examples/run.py\n")
    return out


KEEP_RUNS = 10


def runs_dir() -> Path:
    return cache_dir() / "runs"


def new_run_dir() -> Path:
    """Create this run's output directory under runs_dir(), and return it.

    Every run gets its own directory, named by its start time. So
    runs that overlap, for example from two worktrees, never write to or
    delete each other's files. The directories of all but the newest
    KEEP_RUNS runs are deleted, and runs_dir()/latest points at this one.
    """
    base = runs_dir()
    base.mkdir(parents=True, exist_ok=True)
    # mkdtemp picks a name no other run has. The time prefix makes the names
    # sort by start time.
    out = Path(tempfile.mkdtemp(prefix=time.strftime("%Y%m%d-%H%M%S-"), dir=base))
    (out / OUT_MARKER).write_text("Created by .github/scripts/js_examples/run.py\n")
    # Names sort by start time. Only directories with the marker are deleted.
    old = sorted(
        d
        for d in base.iterdir()
        if d.is_dir() and not d.is_symlink() and (d / OUT_MARKER).is_file()
    )[:-KEEP_RUNS]
    for directory in old:
        shutil.rmtree(directory, ignore_errors=True)
    latest = base / "latest"
    temporary = base / f".latest-{os.getpid()}"
    temporary.unlink(missing_ok=True)
    temporary.symlink_to(out.name)
    # rename() replaces the old link in one step, so latest always exists.
    temporary.replace(latest)
    return out


def run(args: argparse.Namespace, examples: Path) -> int:
    log(
        f"JS SDK {_head(examples.parent.parent) or '(not a git checkout)'}; runner from {REPO_ROOT}"
    )
    out = prepare_out(args.out.resolve()) if args.out else new_run_dir()
    maps_dir = out / "maps"
    maps_dir.mkdir()

    maps = build_function_maps(examples / "template.yml")
    (maps_dir / "function-name-map.json").write_text(
        json.dumps(maps.name_map, indent=2)
    )
    (maps_dir / "shim-map.json").write_text(json.dumps(maps.shim_map, indent=2))
    (maps_dir / "function-configs.json").write_text(
        json.dumps(maps.function_configs, indent=2)
    )

    tests, not_deployed = select_tests(examples, args.patterns, args.shard, maps)
    if not_deployed:
        log(
            f"skipping {len(not_deployed)} test files with no function in template.yml (localOnly examples)"
        )
    if not tests:
        raise HarnessError("no test files match the selection")
    check_bundles_exist(tests, maps, examples / "dist")
    log(
        f"{len(tests)} test files, {args.workers} jest workers"
        + (", --skip-time" if args.skip_time else "")
    )

    results = [run_session(args, examples, maps_dir, out, tests, "jest")]
    failed = failed_tests(results, examples)
    retried: list[str] = []
    if failed and args.retries > 0:
        # Some examples race real timers, for example a parallel branch that
        # must checkpoint within a few milliseconds of another. Under load
        # they fail now and then. So failed files run once more, on fresh
        # servers. summarize() reports a file that passes on retry as flaky.
        for attempt in range(1, args.retries + 1):
            log(f"retrying {len(failed)} failed test file(s), attempt {attempt}")
            retried = sorted(set(retried) | set(failed))
            results.append(
                run_session(args, examples, maps_dir, out, failed, f"retry{attempt}")
            )
            failed = failed_tests(results, examples)
            if not failed:
                break
    return summarize(results, examples, tests, out, retried)


def run_session(
    args: argparse.Namespace,
    examples: Path,
    maps_dir: Path,
    out: Path,
    tests: list[str],
    label: str,
) -> dict[str, Any]:
    """Start fresh servers, run tests with jest, and stop the servers."""
    servers = Servers(
        out, maps_dir, examples, args.skip_time, args.log_level, args.dump_dir
    )
    try:
        servers.start()
        return run_jest(
            examples,
            tests,
            servers,
            maps_dir,
            out / f"{label}.json",
            args.workers,
            args.jest_args,
        )
    finally:
        servers.stop()


if __name__ == "__main__":
    # SIGTERM (for example a cancelled CI job) raises SystemExit, so the
    # finally blocks stop the servers and jest.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    # A shell starts background jobs with SIGINT ignored. Restore the default
    # so Ctrl-C and kill -INT always stop the run.
    signal.signal(signal.SIGINT, signal.default_int_handler)
    try:
        sys.exit(main(sys.argv[1:]))
    except HarnessError as exc:
        print(f"[js-examples] error: {exc}", file=sys.stderr)
        sys.exit(2)
    except subprocess.CalledProcessError as exc:
        cmd = " ".join(str(part) for part in exc.cmd)
        print(
            f"[js-examples] error: `{cmd}` exited with status {exc.returncode}; "
            "see its output above",
            file=sys.stderr,
        )
        sys.exit(2)
    except KeyboardInterrupt:
        sys.exit(130)
