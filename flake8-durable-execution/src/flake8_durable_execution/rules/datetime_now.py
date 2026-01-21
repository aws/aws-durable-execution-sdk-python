"""DEX002: Detect non-deterministic datetime calls."""

import ast
from typing import Iterator

from flake8_durable_execution.rules.base import BaseRule, Error

DATETIME_METHODS = {"now", "utcnow", "today"}


class DatetimeNowRule(BaseRule):
    code = "DAR002"
    message = "Avoid datetime.now()/today() in durable functions - time differs on replay"

    def check(self, node: ast.AST) -> Iterator[Error]:
        if not isinstance(node, ast.Call):
            return

        if isinstance(node.func, ast.Attribute):
            if node.func.attr in DATETIME_METHODS:
                yield self._error(node)
