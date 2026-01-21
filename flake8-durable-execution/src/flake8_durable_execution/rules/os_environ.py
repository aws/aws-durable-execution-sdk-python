"""DAR005: Detect os.environ access that may vary between replays."""

import ast
from typing import Iterator

from flake8_durable_execution.rules.base import BaseRule, Error


class OsEnvironRule(BaseRule):
    code = "DAR005"
    message = "Avoid os.environ access in durable functions - environment may differ on replay"

    def check(self, node: ast.AST) -> Iterator[Error]:
        if not isinstance(node, ast.Subscript):
            return

        # os.environ["KEY"] or os.environ.get("KEY")
        if isinstance(node.value, ast.Attribute):
            if node.value.attr == "environ":
                yield self._error(node)
