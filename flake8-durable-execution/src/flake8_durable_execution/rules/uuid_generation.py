"""DEX003: Detect non-deterministic uuid generation."""

import ast
from typing import Iterator

from flake8_durable_execution.rules.base import BaseRule, Error

UUID_FUNCTIONS = {"uuid1", "uuid4"}


class UuidGenerationRule(BaseRule):
    code = "DAR003"
    message = "Avoid uuid1()/uuid4() in durable functions - generates different values on replay"

    def check(self, node: ast.AST) -> Iterator[Error]:
        if not isinstance(node, ast.Call):
            return

        # uuid.uuid4()
        if isinstance(node.func, ast.Attribute):
            if node.func.attr in UUID_FUNCTIONS:
                yield self._error(node)

        # from uuid import uuid4; uuid4()
        elif isinstance(node.func, ast.Name):
            if node.func.id in UUID_FUNCTIONS:
                yield self._error(node)
