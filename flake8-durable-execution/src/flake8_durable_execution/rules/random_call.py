"""DEX001: Detect non-deterministic random calls."""

import ast
from typing import Iterator

from flake8_durable_execution.rules.base import BaseRule, Error

RANDOM_METHODS = {"random", "randint", "choice", "shuffle", "sample", "uniform", "randrange"}


class RandomCallRule(BaseRule):
    code = "DAR001"
    message = "Avoid random calls in durable functions - results differ on replay"

    def check(self, node: ast.AST) -> Iterator[Error]:
        if not isinstance(node, ast.Call):
            return

        if isinstance(node.func, ast.Attribute):
            if node.func.attr in RANDOM_METHODS:
                yield self._error(node)
