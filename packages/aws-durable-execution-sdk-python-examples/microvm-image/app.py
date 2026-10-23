"""The application in the MicroVM image that the MicroVM examples run.

Lambda starts this process when it builds the image, and snapshots it. A
MicroVM launched from the image resumes the snapshot. The worker then receives
each job, runs it, and reports the result to the durable function's callback.

scripts/ensure_microvm_image.py builds the image from this file, the
Dockerfile, and the worker wheel. The cloud tests pass the image ARN to the
example functions.
"""

import hashlib
import logging
from pathlib import Path
from typing import Any

from aws_durable_execution_sdk_python_microvm_worker import (
    MicrovmJobContext,
    start_microvm_worker,
)


# Only the MicroVM's own jobs write here. A session's jobs run in one MicroVM.
# So a later job of the session reads what an earlier job wrote.
SESSION_DIR = Path("/tmp/session")


def count_words(job_input: dict[str, Any], context: MicrovmJobContext) -> dict:
    """Serve the run-job example. A job that names no route runs this handler."""
    text: str = job_input["text"]
    return {
        "wordCount": len(text.split()),
        "sha256": hashlib.sha256(text.encode()).hexdigest(),
        "microvmId": context.microvm_id,
    }


def _session_file(key: str) -> Path:
    # A key with a directory part could name a file outside SESSION_DIR. So
    # the handlers accept only a plain file name.
    if not key or Path(key).name != key or key in {".", ".."}:
        msg = f"invalid key: {key!r}"
        raise ValueError(msg)
    return SESSION_DIR / key


def write_value(job_input: dict[str, Any], context: MicrovmJobContext) -> dict:
    """Serve the session example's first job. It writes a file."""
    path = _session_file(job_input["key"])
    SESSION_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(job_input["value"], encoding="utf-8")
    return {"microvmId": context.microvm_id}


def read_value(job_input: dict[str, Any], context: MicrovmJobContext) -> dict:
    """Serve the session example's second job. It reads the file."""
    path = _session_file(job_input["key"])
    return {
        "microvmId": context.microvm_id,
        "value": path.read_text(encoding="utf-8"),
    }


def main() -> None:
    # The worker logs to the standard library logger. INFO makes its lines
    # reach stdout.
    logging.basicConfig(level=logging.INFO)
    worker = start_microvm_worker(
        handler=count_words,
        routes={"/write": write_value, "/read": read_value},
    )
    # start_microvm_worker serves in a daemon thread and returns. So the
    # process waits here.
    worker.wait()


if __name__ == "__main__":
    main()
