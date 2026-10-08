# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the worker's loggers."""

from __future__ import annotations

import logging

from aws_durable_execution_sdk_python_microvm_worker.logger import (
    DEFAULT_LOGGER_NAME,
    default_logger,
    describe,
    safe_logger,
)


def test_default_logger_is_the_package_stdlib_logger_and_keeps_extra(caplog):
    logger = safe_logger()
    with caplog.at_level(logging.INFO, logger=DEFAULT_LOGGER_NAME):
        logger.info("job started", extra={"microvmId": "mvm-1"})
        logger.warning("heartbeat failed")
        logger.error("could not report")
    assert default_logger() is logging.getLogger(DEFAULT_LOGGER_NAME)
    assert [(r.levelname, r.getMessage()) for r in caplog.records] == [
        ("INFO", "job started"),
        ("WARNING", "heartbeat failed"),
        ("ERROR", "could not report"),
    ]
    # A logging.Logger keeps the structured fields on the record.
    assert caplog.records[0].microvmId == "mvm-1"


class RaisingLogger:
    def info(self, msg, *args, extra=None):
        raise RuntimeError(msg)

    warning = info
    error = info


def test_safe_logger_drops_a_line_whose_logger_raises():
    logger = safe_logger(RaisingLogger())
    logger.info("a")
    logger.warning("b", extra={})
    logger.error("c")


def test_safe_logger_drops_a_line_with_a_reserved_extra_key(caplog):
    """logging raises KeyError when extra overwrites a LogRecord attribute."""
    with caplog.at_level(logging.INFO, logger=DEFAULT_LOGGER_NAME):
        safe_logger().info("x", extra={"message": "clash"})
    assert caplog.records == []


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
