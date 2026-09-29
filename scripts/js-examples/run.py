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

run.sh creates the virtual environment and then calls this script. See
README.md in this directory for usage.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import os
import re
import resource
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.request
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent.parent
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


def build_js_sdk(js_dir: Path, *, force: bool) -> None:
    """Install and build the JS workspaces the examples need.

    A stamp file records the commit that was last built. The build is
    skipped when the stamp matches HEAD, so repeat runs start in seconds.
    """
    head = _head(js_dir) or "unknown"
    stamp = state_file(js_dir, "built")
    dist = js_dir / EXAMPLES_REL / "dist"
    if (
        not force
        and stamp.exists()
        and stamp.read_text().strip() == head
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
    stamp.write_text(head + "\n")


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
        tests = [t for t in all_tests if any(r.search(t) for r in regexes)]
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


class Servers:
    """Starts the runner, the proxy and the shim, and stops all three."""

    def __init__(
        self, out: Path, maps_dir: Path, examples: Path, skip_time: bool, log_level: str
    ):
        self.out = out
        self.maps_dir = maps_dir
        self.examples = examples
        self.skip_time = skip_time
        self.log_level = log_level
        self.procs: list[tuple[str, subprocess.Popen[bytes]]] = []
        self.proxy_url = ""

    def start(self) -> None:
        runner_port, proxy_port, shim_port = free_port(), free_port(), free_port()
        runner_url = f"http://127.0.0.1:{runner_port}"
        shim_url = f"http://127.0.0.1:{shim_port}"
        self.proxy_url = f"http://127.0.0.1:{proxy_port}"

        self._spawn(
            "runner",
            [
                sys.executable,
                "-m",
                "aws_durable_execution_sdk_python_testing.cli",
                "start-server",
                "--host",
                "127.0.0.1",
                "--port",
                str(runner_port),
                "--lambda-endpoint",
                shim_url,
                "--local-runner-endpoint",
                runner_url,
                "--local-runner-region",
                REGION,
                "--function-configs",
                f"file://{self.maps_dir / 'function-configs.json'}",
                "--skip-time" if self.skip_time else "--no-skip-time",
                "--log-level",
                self.log_level,
            ],
            cwd=REPO_ROOT,
        )
        self._spawn(
            "proxy",
            [
                sys.executable,
                str(HERE / "invoke_proxy.py"),
                "--port",
                str(proxy_port),
                "--upstream",
                runner_url,
            ],
        )
        self._spawn(
            "shim",
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
        )
        self._wait_healthy("runner", f"{runner_url}/health")
        self._wait_healthy("proxy", f"{self.proxy_url}/health")
        self._wait_healthy("shim", f"{shim_url}/health")

    def _spawn(self, name: str, cmd: list[str], cwd: Path | None = None) -> None:
        logfile = (self.out / f"{name}.log").open("ab")
        # A new session makes the process a group leader. stop() signals the
        # whole group, so child processes cannot outlive the run.
        proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=logfile,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            env=harness_env(),
        )
        self.procs.append((name, proc))

    def _wait_healthy(self, name: str, url: str, timeout: float = 60) -> None:
        proc = dict(self.procs)[name]
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise HarnessError(
                    f"{name} exited with status {proc.returncode}; see {self.out / (name + '.log')}"
                )
            with contextlib.suppress(OSError):
                with urllib.request.urlopen(url, timeout=2):  # noqa: S310
                    return
            time.sleep(0.25)
        raise HarnessError(f"{name} did not answer {url} within {timeout:.0f}s")

    def stop(self) -> None:
        for _, proc in self.procs:
            stop_group(proc)
        self.procs.clear()


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
        proc.wait()
    finally:
        stop_group(proc)
    if not results_file.exists():
        raise HarnessError(
            f"jest wrote no results to {results_file}; see its output above"
        )
    results: dict[str, Any] = json.loads(results_file.read_text())
    return results


def batches(tests: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        return [tests]
    return [tests[i : i + size] for i in range(0, len(tests), size)]


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


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

    results are in run order. A retry comes after the first attempt, so its
    outcome replaces the first one.
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
                outcome[rel] = "skipped"
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
      3. jest reported fewer files than were selected, or
      4. no test passed at all.
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
    log(f"logs and report.json: {out}")
    if not passed:
        log("no test passed; treating the run as failed")
        return 1
    return 1 if (unexpected or stale or missing) else 0


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
        prog="run.sh",
        description="Run the JS SDK example tests against this checkout's local runner.",
        epilog="Arguments after -- go to jest unchanged.",
    )
    parser.add_argument(
        "patterns",
        nargs="*",
        metavar="PATTERN",
        help="regex matched against test paths, e.g. 'invoke' or 'step/'. Default: all tests.",
    )
    parser.add_argument(
        "--js-ref",
        help="JS SDK branch, tag or commit to test with. Default: the commit in js-sdk.ref.",
    )
    parser.add_argument(
        "--js-dir",
        type=Path,
        help="use this JS SDK checkout as is, instead of the cached clone. It is built unless --no-build.",
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
        "--batch-size",
        type=int,
        default=0,
        metavar="N",
        help="restart the servers every N test files. Default 0: one set of servers for the run.",
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
        default=HERE / ".out",
        help="logs and report directory (default .out/)",
    )
    if "--" in argv:
        split = argv.index("--")
        args = parser.parse_args(argv[:split])
        args.jest_args = argv[split + 1 :]
    else:
        args = parser.parse_args(argv)
        args.jest_args = []
    return args


def raise_fd_limit() -> None:
    # Hundreds of short-lived sockets are open at once during a run.
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target = 8192 if hard == resource.RLIM_INFINITY else min(8192, hard)
    if soft < target:
        resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    check_node()
    raise_fd_limit()

    js_dir = (
        args.js_dir.expanduser().resolve() if args.js_dir else cache_dir() / "js-sdk"
    )
    # The lock and the build stamp live in the cache directory. So the harness
    # writes nothing into a checkout passed with --js-dir except build output.
    lock_path = state_file(js_dir, "lock")

    # Two runs may share the cached clone. The lock is exclusive while one run
    # checks out and builds, and shared while runs read the build. So a run
    # never checks out another commit under a run that is still testing.
    with lock_path.open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not args.js_dir:
            ref = args.js_ref or (HERE / "js-sdk.ref").read_text().split()[0]
            checkout_js_sdk(js_dir, ref)
        elif not (js_dir / EXAMPLES_REL).is_dir():
            raise HarnessError(f"{js_dir} is not a JS SDK checkout")
        if not args.no_build:
            build_js_sdk(js_dir, force=args.rebuild)
        fcntl.flock(lock, fcntl.LOCK_SH)
        return run(args, js_dir / EXAMPLES_REL)


def run(args: argparse.Namespace, examples: Path) -> int:
    log(
        f"JS SDK {_head(examples.parent.parent) or '(not a git checkout)'}; runner from {REPO_ROOT}"
    )
    out = args.out.resolve()
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)
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
    groups = batches(tests, args.batch_size)
    log(
        f"{len(tests)} test files, {len(groups)} server session(s), {args.workers} jest workers"
        + (", --skip-time" if args.skip_time else "")
    )

    results = run_groups(args, examples, maps_dir, out, groups, "jest")
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
            results += run_groups(
                args, examples, maps_dir, out, [failed], f"retry{attempt}"
            )
            failed = failed_tests(results, examples)
            if not failed:
                break
    return summarize(results, examples, tests, out, retried)


def run_groups(
    args: argparse.Namespace,
    examples: Path,
    maps_dir: Path,
    out: Path,
    groups: list[list[str]],
    label: str,
) -> list[dict[str, Any]]:
    results = []
    for number, group in enumerate(groups, start=1):
        servers = Servers(out, maps_dir, examples, args.skip_time, args.log_level)
        try:
            servers.start()
            results.append(
                run_jest(
                    examples,
                    group,
                    servers,
                    maps_dir,
                    out / f"{label}-{number}.json",
                    args.workers,
                    args.jest_args,
                )
            )
        finally:
            servers.stop()
    return results


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
