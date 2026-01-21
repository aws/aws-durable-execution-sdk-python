"""DEX004: Detect non-deterministic time calls."""

import ast
from typing import Iterator

from flake8_durable_execution.rules.base import BaseRule, Error

TIME_METHODS = {"time", "monotonic", "perf_counter", "process_time"}


class TimeTimeRule(BaseRule):
    code = "DAR004"
    message = "Avoid time.time()/monotonic() in durable functions - time differs on replay"

    def check(self, node: ast.AST) -> Iterator[Error]:
        if not isinstance(node, ast.Call):
            return

        if isinstance(node.func, ast.Attribute):
            if node.func.attr in TIME_METHODS:
                yield self._error(node)
