"""Process exit while a handler invocation is blocked.

Python cannot interrupt a thread blocked in a socket read, and the
interpreter joins non-daemon threads at exit. So the proof that a
blocked Invoke cannot hold the runner's process is a process: a real
WebRunner whose Lambda endpoint accepts the connection and never
answers, stopped while that invocation is in flight. The child process
must exit long before the read timeout (the invocation timeout plus
sixty seconds).
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time

_SCRIPT = textwrap.dedent(
    """
    import socket
    import threading
    import time

    from aws_durable_execution_sdk_python_testing.model import (
        StartDurableExecutionInput,
    )
    from aws_durable_execution_sdk_python_testing.runner import (
        WebRunner,
        WebRunnerConfig,
    )
    from aws_durable_execution_sdk_python_testing.web.server import WebServiceConfig

    # A Lambda endpoint that accepts every connection and never answers.
    accepted = threading.Event()
    endpoint = socket.socket()
    endpoint.bind(("127.0.0.1", 0))
    endpoint.listen()
    held: list[socket.socket] = []

    def accept_forever() -> None:
        while True:
            conn, _ = endpoint.accept()
            held.append(conn)
            accepted.set()

    threading.Thread(target=accept_forever, daemon=True).start()

    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()

    runner = WebRunner(
        WebRunnerConfig(
            web_service=WebServiceConfig(host="127.0.0.1", port=port),
            lambda_endpoint=f"http://127.0.0.1:{endpoint.getsockname()[1]}",
            local_runner_endpoint=f"http://127.0.0.1:{port}",
        )
    )
    runner.start()
    runner._executor.start_execution(  # noqa: SLF001
        StartDurableExecutionInput(
            account_id="123456789012",
            function_name="blocked",
            function_qualifier="$LATEST",
            execution_name="blocked-run",
            execution_timeout_seconds=300,
            execution_retention_period_days=1,
        )
    )
    if not accepted.wait(15):
        raise SystemExit("the handler invocation never reached the endpoint")
    time.sleep(0.3)  # let the invoke settle into its blocked read
    print("invoke in flight", flush=True)
    runner.stop()
    print("runner stopped", flush=True)
    """
)


def test_stopping_the_runner_during_a_blocked_invoke_lets_the_process_exit():
    env = dict(os.environ)
    # The runner signs its Invokes; any credentials will do locally.
    env.setdefault("AWS_ACCESS_KEY_ID", "test")
    env.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    env.setdefault("AWS_DEFAULT_REGION", "us-west-2")

    started = time.monotonic()
    completed = subprocess.run(  # noqa: S603
        [sys.executable, "-c", _SCRIPT],
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )
    elapsed = time.monotonic() - started

    assert completed.returncode == 0, completed.stderr
    assert "invoke in flight" in completed.stdout
    assert "runner stopped" in completed.stdout
    # Import, start, invoke and stop take about a second. The blocked
    # read would hold a non-daemon thread for 960 seconds.
    assert elapsed < 15, f"process took {elapsed:.1f}s to exit"
