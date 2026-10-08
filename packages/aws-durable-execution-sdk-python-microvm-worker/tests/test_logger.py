# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the worker's loggers."""

from __future__ import annotations

import json

from aws_durable_execution_sdk_python_microvm_worker.logger import (
    JsonLogger,
    MicrovmWorkerLogger,
    SafeLogger,
    describe,
)


def test_json_logger_writes_one_object_per_line(capsys):
    logger = JsonLogger()
    logger.info("job started", {"microvmId": "mvm-1"})
    logger.warning("heartbeat failed", {"error": ValueError("x")})
    logger.error("could not report")
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {
        "level": "INFO",
        "message": "job started",
        "microvmId": "mvm-1",
    }
    err = [json.loads(line) for line in captured.err.splitlines()]
    assert err == [
        {"level": "WARN", "message": "heartbeat failed", "error": "x"},
        {"level": "ERROR", "message": "could not report"},
    ]
    assert isinstance(logger, MicrovmWorkerLogger)


class RaisingLogger:
    def info(self, message, data=None):
        raise RuntimeError(message)

    warning = info
    error = info


def test_safe_logger_drops_a_line_whose_logger_raises():
    logger = SafeLogger(RaisingLogger())
    logger.info("a")
    logger.warning("b", {})
    logger.error("c")


class Unprintable(Exception):
    def __str__(self) -> str:
        raise RuntimeError


def test_describe():
    assert describe(ValueError("bad")) == {"name": "ValueError", "message": "bad"}
    assert describe(Unprintable()) == {
        "name": "Unprintable",
        "message": "unknown error",
    }
    assert describe("text") == "text"
