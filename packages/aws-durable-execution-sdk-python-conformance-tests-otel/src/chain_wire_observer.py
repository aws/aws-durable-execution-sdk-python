"""Temporary case11 observation of prepared requests, never an SDK feature.

Only trace identifiers and runtime-model metadata are emitted. The observer
does not change the request, create a client, supply propagation metadata,
or fabricate a response.
"""

from dataclasses import dataclass
from functools import partial
import json
import re

import boto3
import botocore
from botocore.awsrequest import AWSPreparedRequest

from aws_durable_execution_sdk_python.lambda_service import LambdaClient


_EVENT = "before-send.lambda.CheckpointDurableExecution"
_ID = "case11-readonly-prepared-request-observer"


@dataclass(frozen=True)
class TraceCarrier:
    present: bool
    root: str | None = None
    parent: str | None = None
    sampled: str | None = None

    @classmethod
    def from_header(cls, value: object) -> "TraceCarrier":
        if value is None:
            return cls(False)
        if isinstance(value, bytes):
            value = value.decode("ascii", errors="replace")
        if not isinstance(value, str):
            return cls(True)
        fields: dict[str, str] = {}
        patterns = {
            "Root": r"1-[0-9a-fA-F]{8}-[0-9a-fA-F]{24}",
            "Parent": r"[0-9a-fA-F]{16}",
            "Sampled": r"[01]",
        }
        for part in value.split(";"):
            name, separator, field = part.strip().partition("=")
            if name not in patterns:
                continue
            if (
                name in fields
                or not separator
                or not re.fullmatch(patterns[name], field)
            ):
                return cls(True)
            fields[name] = field
        if set(fields) != set(patterns):
            return cls(True)
        return cls(True, fields["Root"], fields["Parent"], fields["Sampled"])

    def to_dict(self) -> dict[str, bool | str | None]:
        return {
            "present": self.present,
            "valid": self.root is not None,
            "Root": self.root,
            "Parent": self.parent,
            "Sampled": self.sampled,
        }


def _emit(record: dict[str, object]) -> None:
    try:
        print("DURABLE_CHAIN_WIRE " + json.dumps(record, sort_keys=True), flush=True)
    except Exception:
        pass


def _version(value: str) -> str:
    return value if re.fullmatch(r"[0-9][0-9A-Za-z.+-]{0,63}", value) else "unavailable"


def observe_request(
    request: AWSPreparedRequest, *, model_has_field: bool, **_kwargs: object
) -> None:
    """Observe the final prepared body, leaving body and signed headers intact."""
    try:
        if not isinstance(request.body, (str, bytes, bytearray)):
            _emit({"event": "observer_unavailable", "reason": "non_text_body"})
            return
        body = json.loads(request.body)
        http_header = next(
            (
                value
                for key, value in request.headers.items()
                if key.lower() == "x-amzn-trace-id"
            ),
            None,
        )
        for update in body.get("Updates", []):
            if not isinstance(update, dict) or not (
                update.get("Type") == "CHAINED_INVOKE"
                and update.get("Action") == "START"
                and update.get("Name") == "otel-invoke"
            ):
                continue
            options = update.get("ChainedInvokeOptions", {})
            header = options.get("XAmznTraceId") if isinstance(options, dict) else None
            operation_id = update.get("Id")
            record: dict[str, object] = {
                "event": "prepared_invoke_start",
                "model_has_field": model_has_field,
                "body_trace": TraceCarrier.from_header(header).to_dict(),
                "http_trace": TraceCarrier.from_header(http_header).to_dict(),
                "boto3_version": _version(boto3.__version__),
                "botocore_version": _version(botocore.__version__),
            }
            if isinstance(operation_id, str) and re.fullmatch(
                r"[0-9a-fA-F]{64}", operation_id
            ):
                record["operation_id"] = operation_id
            _emit(record)
    except Exception as error:
        _emit({"event": "observer_error", "error_type": type(error).__name__})


def install_checkpoint_observer() -> None:
    """Observe the existing SDK client through botocore's event interface.

    The private cache access is confined to this temporary diagnostic fixture.
    It must not be merged into a shipping SDK or normal conformance handler.
    """
    try:
        client = LambdaClient._cached_boto_client
        if client is None:
            _emit({"event": "observer_unavailable", "reason": "no_cached_sdk_client"})
            return
        model = client.meta.service_model.operation_model("CheckpointDurableExecution")
        has_field = "XAmznTraceId" in (
            model.input_shape.members["Updates"]
            .member.members["ChainedInvokeOptions"]
            .members
        )
        client.meta.events.register_last(
            _EVENT, partial(observe_request, model_has_field=has_field), unique_id=_ID
        )
    except Exception as error:
        _emit({"event": "observer_install_error", "error_type": type(error).__name__})
