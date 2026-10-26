"""Tests for the catalog entries of the MicroVM examples."""

import json
from pathlib import Path


CATALOG_PATH = Path(__file__).resolve().parents[2] / "examples-catalog.json"

# The request step of an HTTP delivery retries for up to retry_window, 60
# seconds by default. It needs at least 11 seconds left in the invocation
# when it starts. So the extras README asks for a function timeout of at
# least retry_window plus 11 seconds.
HTTP_DELIVERY_MIN_TIMEOUT_SECONDS = 60 + 11


def catalog_entry(name):
    examples = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))["examples"]
    return next(example for example in examples if example["name"] == name)


def test_the_session_function_timeout_covers_the_http_retry_window():
    # Every session job goes over HTTP.
    entry = catalog_entry("MicroVM Session")

    assert entry["usesMicrovm"] is True
    assert entry["timeout"] >= HTTP_DELIVERY_MIN_TIMEOUT_SECONDS


def test_the_suspend_session_function_timeout_covers_the_http_retry_window():
    # Every session job goes over HTTP. The wait for a resume counts toward
    # the retry window. So the same minimum applies.
    entry = catalog_entry("MicroVM Session Suspend")

    assert entry["usesMicrovm"] is True
    assert entry["timeout"] >= HTTP_DELIVERY_MIN_TIMEOUT_SECONDS


def test_the_run_job_function_uses_the_global_timeout():
    # The input is small, so the job goes in the run hook. The run hook
    # delivery adds no Lambda time for the boot.
    entry = catalog_entry("MicroVM Run Job")

    assert entry["usesMicrovm"] is True
    assert "timeout" not in entry
