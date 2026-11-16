# AI operations: reference

> This module is experimental. Every export can change or be removed in any release.

This file describes `decider` and `llm` in `aws_durable_execution_sdk_python_extras.ai`. The exports match `@aws/durable-execution-sdk-js-extras/ai`.

```python
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python_extras.ai import decider, llm, sagemaker_transport

decide = decider(transport=sagemaker_transport("strands-decider-2b-qwen3-5-v1-2610"))
model = llm()


@durable_execution
def handler(ticket: dict, context: DurableContext) -> dict:
    team = decide.classify(context, "Which team?", ["billing", "returns", "other"], ticket["body"])
    if decide.check(context, "Is the customer asking for a human?", ticket["body"]):
        return {"team": team, "escalate": True}
    summary = model.ask(context, "Summarize in one sentence.", ticket["body"])
    return {"team": team, "summary": summary}
```

The first argument of each call is the context that the call runs in. Inside a child context or a `map` item, pass that context. Create `decide` and `model` once, when the module loads.

## `decider()`

A decision model is not a large language model. It does not write text. It picks one of your options or rates on a scale. So its answer is always one of the options or a number in range.

`decider()` assumes that you already deployed a [Strands Decider](https://github.com/strands-labs/strands-decider) and that it is ready. It does not host or check the model.

| Operation | Returns |
|---|---|
| `check(context, question, input)` | `True` when the model's probability of yes is at least `threshold`, 0.5 by default |
| `classify(context, question, choices, input)` | Exactly one of `choices`, in your spelling |
| `label(context, question, choices, input)` | Every choice that applies, in the order of `choices`. It can be empty. |
| `score(context, question, scale, input)` | A number from 0 to 1. The first entry of `scale` is 0 and the last is 1. |

### Where the model runs

Every host takes the same request, so only the way the call is sent changes. Pass `endpoint` or one `transport`:

| Host | Option | AWS call | Permission |
|---|---|---|---|
| Any server, such as `strands-decider serve` | `endpoint="https://.../v1/systemone"` | none, plain HTTP | none |
| Amazon SageMaker real-time endpoint | `transport=sagemaker_transport(endpoint_name)` | `InvokeEndpoint` | `sagemaker:InvokeEndpoint` on the endpoint |
| AWS Lambda function | `transport=lambda_transport(function_name, qualifier=...)` | `Invoke`, with the request as the event | `lambda:InvokeFunction` on the function |
| Amazon Bedrock AgentCore Runtime | `transport=agentcore_transport(runtime_arn)` | `InvokeAgentRuntime` | `bedrock-agentcore:InvokeAgentRuntime` on the runtime |
| Anything else | `transport=your_callable` | yours | yours |

The [Strands Decider repository](https://github.com/strands-labs/strands-decider/tree/main/deploy) has CDK stacks that deploy the model to SageMaker and to Lambda.

```python
from aws_durable_execution_sdk_python_extras.ai import (
    agentcore_transport,
    decider,
    lambda_transport,
    sagemaker_transport,
)

on_sagemaker = decider(transport=sagemaker_transport("strands-decider-2b-qwen3-5-v1-2610"))
on_lambda = decider(transport=lambda_transport("strands-decider-2b-qwen3-5-v1-2610", qualifier="live"))
on_agentcore = decider(
    transport=agentcore_transport(
        "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/strands-decider-abc"
    )
)
on_a_server = decider(
    endpoint="https://decider.example.com/v1/systemone",
    headers={"authorization": f"Bearer {token}"},
    threshold=0.7,
)
```

- Pass only one of `endpoint` and `transport`. Without either, `decider()` uses the `STRANDS_DECIDER_ENDPOINT` environment variable. It raises `TypeError` when that is not set.
- The AWS transports use boto3, which this package already depends on. They sign the call with the function's credentials. Each one takes `client=` to replace the default boto3 client, for example to set a Region.
- A transport is any callable that takes a `DeciderRequest` dict and returns the JSON reply. An exception it raises goes to the step retry strategy.
- On SageMaker and AgentCore Runtime, the call reaches the server's `POST /invocations` route. The Strands Decider server answers that route like `/v1/systemone`. The 0.2.0 release on PyPI has no `/invocations` route, so deploy a later version.
- On SageMaker, an HTTP error from the server becomes a `DeciderError` with the server's status.
- On Lambda, the function fails the invocation when it refuses a request, for example an input that does not fit the window. That becomes a `DeciderError` with status 422, which is not retried. Any other function error is retried. `lambda_transport` does not use the function's Function URL. A new execution environment loads the model first, which can take 30 seconds without SnapStart.
- An endpoint with a user name or password is refused: pass credentials in `headers`. Redirects are not followed, so your headers never reach another server. Errors name the endpoint without its query, because errors are checkpointed.

## `llm()`

```python
from aws_durable_execution_sdk_python_extras.ai import BedrockCallOptions, BedrockConfig, llm

model = llm(BedrockConfig(model_id="global.anthropic.claude-sonnet-5-5", max_tokens=2048))
short = model.ask(context, "Summarize.", text, bedrock=BedrockCallOptions(max_tokens=100))
```

- The default model is Claude Haiku 5.5 (`global.anthropic.claude-haiku-5-5`, exported as `DEFAULT_LLM_MODEL_ID`). Bedrock has no alias that follows the newest model of a family. So the default is pinned, and a later release moves it to a newer model. Set `model_id` to keep one model.
- `llm()` takes one destination. This release supports `BedrockConfig` only.
- `ask` sends a temperature only when you set one, because some newer models, such as Claude Haiku 5.5, reject it.
- `system` adds your own instructions after the built-in system prompt. `guardrail=BedrockGuardrail(identifier=..., version=...)` applies a Bedrock guardrail. `client` replaces the default `bedrock-runtime` client.
- `bedrock=BedrockCallOptions(...)` changes `model_id`, `temperature`, or `max_tokens` for one call.
- `ask` sends the input inside `<input>` tags, and its system prompt says that the input is data, not instructions. An instruction in the input can still change the reply. So do not use a reply alone to authorize an action.

## Limits

| Operation | Limits |
|---|---|
| All | `question` is a non-empty string. `input` is a string, `None`, or a value that `json.dumps` can encode. |
| `decider()` operations | The model reads the input and each question, with its choices, in one context window: 4,096 tokens for Strands Decider 2B. A request that does not fit fails with `DeciderError` and status 422, which is not retried. Shorten the input before the call. |
| `classify` | 2 to 255 choices (`MAX_CLASSIFY_OPTIONS`). Choices are non-empty and differ after trimming and ignoring case. The model always picks one, so add a choice such as `"other"` when none may fit. |
| `label` | At least 2 choices, with the same rules. Each choice is one yes-or-no question, so each adds time. |
| `score` | 2 to 10 entries (`MAX_SCORE_LEVELS`), from lowest to highest. |
| `ask` | The reply is at most `max_tokens`, 1024 by default. The reply must fit the 256 KB checkpoint limit. |

A call that breaks an argument limit creates no durable operation. It raises `TypeError` or `ValueError`.

## What a call does

1. The call checks its arguments against the limits above.
2. One step sends one request. The step is named `decider.<operation>` or `llm.ask`. Pass `name=` to rename it. Renaming an existing call breaks executions in flight.
3. The step has a subtype from `AiOperationSubType`: `DeciderCheck`, `DeciderClassify`, `DeciderLabel`, `DeciderScore`, or `LlmAsk`. The values match the JavaScript SDK.
4. The step result is the answer, checkpointed as plain JSON. The input is not checkpointed. A replay returns the answer and does not call the model.
5. The request runs in a daemon thread. The step stops waiting for it 10 seconds before the invocation runs out of time and raises `AiTimeoutError`, so the step can record the failure. boto3 cannot cancel a call, so the thread runs until the call returns, and its result is discarded.

## Errors and retries

Every error class extends `AiError`, which extends the SDK's `DurableOperationError`. The call raises the class that the step's last attempt raised, on the first run and on replay. Its `status` or `stop_reason` is stored in the error's `data`, so it survives replay. A step that fails with any other error, such as an AWS access-denied error, raises the SDK's `StepError`.

| Error | When | Retried |
|---|---|---|
| `AiTimeoutError` | A request is still running 10 seconds before the invocation runs out of time | Yes, in a new invocation |
| `DeciderError` | The decision model cannot be reached, or answers with an error. `status` is the HTTP status, or `None` when it did not answer. | On no answer, 408, 429, and 5xx |
| `DeciderInvalidResponseError` | The decision model's reply is not JSON, has the wrong shape, or has a score outside the scale | No |
| `LlmMaxTokensError` | The `ask` reply stops at `max_tokens` | No |
| `LlmStoppedError` | The `ask` reply stops for another reason, such as a guardrail that intervened. `stop_reason` says why. | Only `malformed_model_output` and `malformed_tool_use` |

The step retry strategy is `default_ai_retry_strategy`. It also retries AWS throttling and server errors, such as `ThrottlingException`, `TooManyRequestsException`, `ServiceUnavailable`, and any status of 429 or 5xx, and connection failures. It allows up to 5 attempts, with a delay of 2, 4, 8, and 16 seconds. It does not retry access-denied or validation errors, because they repeat. Pass `retry_strategy=` to `decider()` or `llm()` to replace it.

## Permissions

| Call | The function's role needs |
|---|---|
| `ask` | `bedrock:InvokeModel` on the model, and on the inference profile when the model ID names one. A guardrail also needs `bedrock:ApplyGuardrail`. |
| `decider()` with an AWS transport | The permission in the [Where the model runs](#where-the-model-runs) table |
| `decider()` with `endpoint` or your own transport | No AWS permission |

## Testing

Pass a callable as `transport` to `decider()`, or a fake client as `client=` to an AWS transport or to `BedrockConfig`. The local test runner then runs a handler without AWS credentials or a model server.
