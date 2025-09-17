# mypy: ignore-errors
"""Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.

The orignal actually lives here:
https://github.com/aws/aws-lambda-python-runtime-interface-client/blob/main/awslambdaric/lambda_context.py

On a quick look it's missing tenant_id and the Python 3.13 upgrades.

The 3.1.1 wheel is ~269.1 kB. Which honeslly, the entire dependency for the sake of this little class?

For what it's worth, PowerTools also doesn't re-use the actual Python RIC LambdaContext, it also defines its
own copied type here:
https://github.com/aws-powertools/powertools-lambda-python/blob/6e900c79fff44675fcef3a71a0e3310c54f01ecd/aws_lambda_powertools/utilities/typing/lambda_context.py

For the moment I'm going to use this copied class, since all it's really doing is providing a base class for DurableContext -
given duck-typing it doesn't actually have to inherit from the "same" class in the RIC.
Yes, this can get out of date with the Python RIC, but at worst it just means red squiggly lines on new properties -
given duck-typing it'll work at runtime.

"""

import logging
import os
import sys
import time


class LambdaContext:
    """Replicate the LambdaContext from the AWS Lambda ARIC.

    https://github.com/aws/aws-lambda-python-runtime-interface-client/blob/main/awslambdaric/lambda_context.py

    This is here solely for typings and to get DurableContext to inherit from LambdaContext without needing to
    add `aws-lambda-python-runtime-interface-client` as a direct dependency of the Durable Executions SDK.

    This has a subtle and important side-effect. This class is _not_ actually the LambdaContext that the AWS
    Lambda runtime passes to the Lambda handler. So do NOT added any custom methods or attributes here, you can
    only rely on duck-typing so whatever is in this class replicates what is in the actual class, it will work.
    """

    def __init__(
        self,
        invoke_id,
        client_context,
        cognito_identity,
        epoch_deadline_time_in_ms,
        invoked_function_arn=None,
        tenant_id=None,
    ):
        self.aws_request_id: str = invoke_id
        self.log_group_name: str | None = os.environ.get("AWS_LAMBDA_LOG_GROUP_NAME")
        self.log_stream_name: str | None = os.environ.get("AWS_LAMBDA_LOG_STREAM_NAME")
        self.function_name: str | None = os.environ.get("AWS_LAMBDA_FUNCTION_NAME")
        self.memory_limit_in_mb: str | None = os.environ.get(
            "AWS_LAMBDA_FUNCTION_MEMORY_SIZE"
        )
        self.function_version: str | None = os.environ.get(
            "AWS_LAMBDA_FUNCTION_VERSION"
        )
        self.invoked_function_arn: str | None = invoked_function_arn
        self.tenant_id: str | None = tenant_id

        self.client_context = make_obj_from_dict(ClientContext, client_context)
        if self.client_context is not None:
            self.client_context.client = make_obj_from_dict(
                Client, self.client_context.client
            )

        self.identity = make_obj_from_dict(CognitoIdentity, {})
        if cognito_identity is not None:
            self.identity.cognito_identity_id = cognito_identity.get(
                "cognitoIdentityId"
            )
            self.identity.cognito_identity_pool_id = cognito_identity.get(
                "cognitoIdentityPoolId"
            )

        self._epoch_deadline_time_in_ms = epoch_deadline_time_in_ms

    def get_remaining_time_in_millis(self) -> int:
        epoch_now_in_ms = int(time.time() * 1000)
        delta_ms = self._epoch_deadline_time_in_ms - epoch_now_in_ms
        return delta_ms if delta_ms > 0 else 0

    def log(self, msg):
        for handler in logging.getLogger().handlers:
            if hasattr(handler, "log_sink"):
                handler.log_sink.log(str(msg))
                return
        sys.stdout.write(str(msg))

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(["
            f"aws_request_id={self.aws_request_id},"
            f"log_group_name={self.log_group_name},"
            f"log_stream_name={self.log_stream_name},"
            f"function_name={self.function_name},"
            f"memory_limit_in_mb={self.memory_limit_in_mb},"
            f"function_version={self.function_version},"
            f"invoked_function_arn={self.invoked_function_arn},"
            f"client_context={self.client_context},"
            f"identity={self.identity},"
            f"tenant_id={self.tenant_id}"
            "])"
        )


class CognitoIdentity:
    __slots__ = ["cognito_identity_id", "cognito_identity_pool_id"]

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(["
            f"cognito_identity_id={self.cognito_identity_id},"
            f"cognito_identity_pool_id={self.cognito_identity_pool_id}"
            "])"
        )


class Client:
    __slots__ = [
        "installation_id",
        "app_title",
        "app_version_name",
        "app_version_code",
        "app_package_name",
    ]

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(["
            f"installation_id={self.installation_id},"
            f"app_title={self.app_title},"
            f"app_version_name={self.app_version_name},"
            f"app_version_code={self.app_version_code},"
            f"app_package_name={self.app_package_name}"
            "])"
        )


class ClientContext:
    __slots__ = ["custom", "env", "client"]

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(["
            f"custom={self.custom},"
            f"env={self.env},"
            f"client={self.client}"
            "])"
        )


def make_obj_from_dict(_class, _dict, fields=None):  # noqa: ARG001
    if _dict is None:
        return None
    obj = _class()
    set_obj_from_dict(obj, _dict)
    return obj


def set_obj_from_dict(obj, _dict, fields=None):
    if fields is None:
        fields = obj.__class__.__slots__
    for field in fields:
        setattr(obj, field, _dict.get(field, None))


def make_dict_from_obj(obj):
    """Convert an object with __slots__ back to a dictionary.

    Custom addition - not in the original AWS Lambda Runtime Interface Client (ARIC). This
    is to help when DurableContext needs to call LambdaContext's super() constructor and pass
    it the original dictionaries.
    This is the reverse of make_obj_from_dict to convert __slots__ objects back to dictionaries.
    """
    if obj is None:
        return None

    result = {}
    for field in obj.__class__.__slots__:
        value = getattr(obj, field, None)
        # Recursively convert nested objects
        if value is not None and hasattr(value, "__slots__"):
            value = make_dict_from_obj(value)
        result[field] = value
    return result
