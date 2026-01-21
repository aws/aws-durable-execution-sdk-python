"""DAR007: Detect durable operations inside @durable_step functions."""

import ast
from typing import Iterator

from flake8_durable_execution.rules.base import BaseRule, Error

DURABLE_OPS = {"step", "wait", "invoke", "parallel", "map", "wait_for_callback", "wait_for_condition"}


class NestedDurableOpRule(BaseRule):
    code = "DAR007"
    message = "Durable operations cannot be called inside @durable_step"

    def __init__(self) -> None:
        self.in_durable_step = False

    def check_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> Iterator[Error]:
        """Check a @durable_step function for nested durable ops."""
        for child in ast.walk(node):
            if isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute):
                if child.func.attr in DURABLE_OPS:
                    yield (child.lineno, child.col_offset, f"{self.code} {self.message}", type(self))

    def check(self, node: ast.AST) -> Iterator[Error]:
        # This rule is handled specially by the checker
        return
        yield  # Make this a generator
