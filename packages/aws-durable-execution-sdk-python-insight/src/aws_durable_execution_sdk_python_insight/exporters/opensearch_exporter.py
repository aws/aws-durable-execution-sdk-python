# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""OpenSearch (Index API) Workflow Insight exporter."""

from __future__ import annotations

import base64
from enum import StrEnum
from typing import Any, Literal
from urllib.parse import quote

from aws_durable_execution_sdk_python_insight.exporters._common import (
    compact_dumps,
    http_send,
)


class OpenSearchAuth(StrEnum):
    """Authentication method for the index request."""

    # IAM credentials, SigV4-signed (Amazon OpenSearch Service)
    SIGV4 = "sigv4"
    # username / password (self-managed, or a domain with basic auth)
    BASIC = "basic"


# Accepted string inputs, kept in lockstep with the enum values above.
OpenSearchAuthInput = Literal["sigv4", "basic"]

# Same character set that browsers leave unescaped in URI components.
_DOC_ID_SAFE = "-_.!~*'()"


class OpenSearchExporter:
    """Indexes each record as one document, keyed by ``executionArn``.

    A later export for the same execution overwrites the document. Supports
    SigV4 (IAM) and basic authentication; no OpenSearch client library needed.
    """

    def __init__(
        self,
        endpoint: str,
        region: str,
        index_name: str = "workflow-insight",
        auth: OpenSearchAuth | OpenSearchAuthInput = OpenSearchAuth.SIGV4,
        username: str | None = None,
        password: str | None = None,
        max_record_size_bytes: int | None = None,
    ) -> None:
        self.endpoint = endpoint[:-1] if endpoint.endswith("/") else endpoint
        self.index_name = index_name
        self.region = region
        self.auth = OpenSearchAuth(auth)
        if self.auth == OpenSearchAuth.BASIC and (username is None or password is None):
            msg = "OpenSearchExporter: auth='basic' requires username and password."
            raise ValueError(msg)
        self.username = username
        self.password = password
        self.max_record_size_bytes: int | None = (
            10_000_000 if max_record_size_bytes is None else max_record_size_bytes
        )
        self._credentials: Any = None

    def render(self, record: dict[str, Any]) -> dict[str, Any]:
        return record

    def export(self, record: dict[str, Any]) -> None:
        doc_id = quote(record["executionArn"], safe=_DOC_ID_SAFE)
        url = f"{self.endpoint}/{self.index_name}/_doc/{doc_id}"
        body = compact_dumps(record).encode("utf-8")
        headers: dict[str, str] = {"Content-Type": "application/json"}

        if self.auth == OpenSearchAuth.BASIC:
            token = base64.b64encode(
                f"{self.username}:{self.password}".encode()
            ).decode("ascii")
            headers["Authorization"] = f"Basic {token}"
        else:
            # The signed header set is sent as-is: it already carries
            # content-type, host, date, token and authorization.
            headers = self._sign(url, body, headers)

        status, reason, detail = http_send("PUT", url, headers, body)
        if not 200 <= status < 300:
            msg = f"OpenSearch index failed: {status} {reason}"
            if detail:
                msg += f" — {detail[:500]}"
            raise RuntimeError(msg)

    def flush(self) -> None:
        return None

    def _sign(self, url: str, body: bytes, headers: dict[str, str]) -> dict[str, str]:
        # deferred: botocore is provided by the Lambda runtime
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest

        if self._credentials is None:
            import botocore.session

            self._credentials = botocore.session.get_session().get_credentials()
            if self._credentials is None:
                msg = "OpenSearchExporter: no AWS credentials found for SigV4 signing."
                raise RuntimeError(msg)
        request = AWSRequest(method="PUT", url=url, data=body, headers=headers)
        SigV4Auth(self._credentials, "es", self.region).add_auth(request)
        return dict(request.headers.items())
