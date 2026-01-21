"""DAR006: Detect network calls that may return different results on replay."""

import ast
from typing import Iterator

from flake8_durable_execution.rules.base import BaseRule, Error

HTTP_METHODS = {"get", "post", "put", "delete", "patch", "head", "options", "request"}
URLLIB_METHODS = {"urlopen", "urlretrieve"}
SOCKET_METHODS = {"connect", "send", "recv", "sendall"}


class NetworkCallRule(BaseRule):
    code = "DAR006"
    message = "Wrap network calls in ctx.step() - responses may differ on replay"

    def check(self, node: ast.AST) -> Iterator[Error]:
        if not isinstance(node, ast.Call):
            return

        if isinstance(node.func, ast.Attribute):
            method = node.func.attr
            if method in HTTP_METHODS | URLLIB_METHODS | SOCKET_METHODS:
                yield self._error(node)
