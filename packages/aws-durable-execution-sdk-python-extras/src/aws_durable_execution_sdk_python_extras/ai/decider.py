# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The ``decider`` operations: ``check``, ``classify``, ``label``, and
``score``.

This module is experimental. Its API can change in any release.
"""

from __future__ import annotations

import math
import os
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.retries import RetryDecision

from aws_durable_execution_sdk_python_extras.ai._shared import (
    MAX_CLASSIFY_OPTIONS,
    call_before_deadline,
    invalid,
    match_option,
    run_step,
    serialize_input,
    to_unit_score,
    validate_options,
    validate_question,
    validate_scale,
)
from aws_durable_execution_sdk_python_extras.ai.retry import default_ai_retry_strategy
from aws_durable_execution_sdk_python_extras.ai.subtypes import AiOperationSubType
from aws_durable_execution_sdk_python_extras.ai.transports import (
    DeciderRequest,
    DeciderTransport,
    _http_transport,
    _parse_endpoint,
)


DEFAULT_DECIDER_MODEL = "strands-decider-latest"
"""The ``model`` field of a request when the caller sets none."""

_ANSWER = "answer"

_SCORE_TOLERANCE = 0.01
"""Rounding room for a score at either end of the scale."""

_Answers = Mapping[str, Any]


def _answers_of(response: Any) -> _Answers:
    answers = response.get("answers") if isinstance(response, dict) else None
    if not isinstance(answers, dict):
        raise invalid("The decision model's reply has no answers.")
    return answers


def _number_field(answer: Any, kind: str, field: str) -> float:
    value = answer.get(field) if isinstance(answer, dict) else None
    if (
        not isinstance(answer, dict)
        or answer.get("type") != kind
        or isinstance(value, bool)
        or not isinstance(value, int | float)
        or not math.isfinite(value)
    ):
        raise invalid(f'The decision model\'s "{field}" is not a number.')
    return float(value)


def _probability(answer: Any) -> float:
    """Return the model's probability of yes, from 0 to 1.

    A probability within :data:`_SCORE_TOLERANCE` outside 0 to 1 is rounding,
    and is clamped. One farther out is an invalid reply. Otherwise a reply such
    as 7 or -3 would become a confident answer.
    """
    p = _number_field(answer, "noul", "noul")
    if p < -_SCORE_TOLERANCE or p > 1 + _SCORE_TOLERANCE:
        raise invalid(f"The decision model's probability {p} is outside 0 to 1.")
    return min(1.0, max(0.0, p))


def _threshold(value: float | None) -> float:
    if value is None:
        return 0.5
    if isinstance(value, bool) or not isinstance(value, int | float):
        msg = "threshold must be a number from 0 to 1."
        raise TypeError(msg)
    if not 0 <= value <= 1:
        msg = "threshold must be a number from 0 to 1."
        raise ValueError(msg)
    return float(value)


class Decider:
    """The four operations that :func:`decider` returns.

    Experimental. It can change or be removed in any release.

    Each operation is one durable step, and each one takes the context first.
    Every operation has these limits:

    - ``question`` must be a non-empty string.
    - ``input`` must be a string, ``None``, or a value that ``json.dumps`` can
      encode.
    - The decision model reads the input and each question, with its choices,
      in one context window: 4,096 tokens for Strands Decider 2B. A request
      that does not fit fails with :class:`DeciderError` and status 422, which
      is not retried. Shorten the input before the call.
    - An invalid argument creates no durable operation. The call raises
      ``TypeError`` or ``ValueError``.
    - A request still running 10 seconds before the invocation runs out of
      time stops with :class:`AiTimeoutError`, and the step retries in a new
      invocation.
    - The answer is the step result. So a replay returns it without calling
      the model.
    - When the step fails, the call raises the :class:`AiError` subclass that
      the last attempt raised. Any other failure raises ``StepError``.

    ``name`` renames the step. The default is ``decider.<operation>``. Replay
    compares the name with the checkpoint. So adding or changing a name on an
    existing call breaks executions in flight.
    """

    def __init__(
        self,
        transport: DeciderTransport,
        model: str,
        threshold: float,
        retry_strategy: Callable[[Exception, int], RetryDecision],
    ) -> None:
        self._transport = transport
        self._model = model
        self._threshold = threshold
        self._retry_strategy = retry_strategy

    def _ask(
        self, value: Any, questions: dict[str, Any], remaining: float | None
    ) -> _Answers:
        request: DeciderRequest = {
            "model": self._model,
            "state": serialize_input(value),
            "questions": questions,
        }
        return _answers_of(
            call_before_deadline(lambda: self._transport(request), remaining)
        )

    def _run(
        self,
        context: DurableContext,
        operation: str,
        sub_type: AiOperationSubType,
        name: str | None,
        body: Callable[[float | None], Any],
    ) -> Any:
        return run_step(
            context,
            name if name is not None else f"decider.{operation}",
            sub_type.value,
            self._retry_strategy,
            body,
        )

    def check(
        self,
        context: DurableContext,
        question: str,
        input: Any,  # noqa: A002 - the name matches the JavaScript SDK
        *,
        name: str | None = None,
    ) -> bool:
        """Answer a yes-or-no question.

        Phrase the question so that yes is ``True``. The answer is ``True``
        when the model's probability of yes is at least ``threshold``, 0.5 by
        default.

        Args:
            context: The context to create the step in.
            question: A yes-or-no question.
            input: The text or data that the question is about.
            name: The step name. The default is ``decider.check``.
        """
        validate_question(question)
        serialize_input(input)

        def body(remaining: float | None) -> bool:
            answers = self._ask(
                input,
                {_ANSWER: {"type": "noul", "instructions": question}},
                remaining,
            )
            return _probability(answers.get(_ANSWER)) >= self._threshold

        return bool(
            self._run(context, "check", AiOperationSubType.DECIDER_CHECK, name, body)
        )

    def classify(
        self,
        context: DurableContext,
        question: str,
        choices: Sequence[str],
        input: Any,  # noqa: A002 - the name matches the JavaScript SDK
        *,
        name: str | None = None,
    ) -> str:
        """Return exactly one of the choices, in the caller's spelling.

        Limits:

        - 2 to :data:`MAX_CLASSIFY_OPTIONS` choices.
        - Every choice is a non-empty string.
        - The choices must differ after trimming whitespace and ignoring case.
        - The model always picks a choice. Add a choice such as ``"other"``
          when none may fit.
        - Every choice counts toward the model's context window.

        Args:
            context: The context to create the step in.
            question: The question that the choices answer.
            choices: The possible answers.
            input: The text or data that the question is about.
            name: The step name. The default is ``decider.classify``.
        """
        validate_question(question)
        options = validate_options(choices, MAX_CLASSIFY_OPTIONS)
        serialize_input(input)

        def body(remaining: float | None) -> str:
            criteria = dict.fromkeys(options)
            answers = self._ask(
                input,
                {
                    _ANSWER: {
                        "type": "choice",
                        "instructions": question,
                        "criteria": criteria,
                    }
                },
                remaining,
            )
            answer = answers.get(_ANSWER)
            if not isinstance(answer, dict) or answer.get("type") != "choice":
                raise invalid("The decision model's answer is not a choice.")
            chosen = match_option(options, answer.get("choice"))
            if chosen is None:
                raise invalid(f'"{answer.get("choice")}" is not one of the options.')
            return chosen

        return str(
            self._run(
                context, "classify", AiOperationSubType.DECIDER_CLASSIFY, name, body
            )
        )

    def label(
        self,
        context: DurableContext,
        question: str,
        choices: Sequence[str],
        input: Any,  # noqa: A002 - the name matches the JavaScript SDK
        *,
        name: str | None = None,
    ) -> list[str]:
        """Return every choice that applies, in the order of ``choices``.

        The result can be empty.

        Limits:

        - At least 2 choices.
        - Every choice is a non-empty string.
        - The choices must differ after trimming whitespace and ignoring case.
        - Each choice is one yes-or-no question to the decision model. The
          model reads the input once, but each choice adds time. So keep the
          list short.
        - A choice applies when the model's probability of yes is at least
          ``threshold``, 0.5 by default.

        Args:
            context: The context to create the step in.
            question: The question that the choices answer.
            choices: The labels to test.
            input: The text or data that the question is about.
            name: The step name. The default is ``decider.label``.
        """
        validate_question(question)
        options = validate_options(choices)
        serialize_input(input)

        def body(remaining: float | None) -> list[str]:
            # One yes-or-no question per option. The model reads the input once.
            questions = {
                f"o{i}": {
                    "type": "noul",
                    "instructions": f"{question}\nDoes this label apply: {option}",
                }
                for i, option in enumerate(options)
            }
            answers = self._ask(input, questions, remaining)
            return [
                option
                for i, option in enumerate(options)
                if _probability(answers.get(f"o{i}")) >= self._threshold
            ]

        return list(
            self._run(context, "label", AiOperationSubType.DECIDER_LABEL, name, body)
        )

    def score(
        self,
        context: DurableContext,
        question: str,
        scale: Sequence[str],
        input: Any,  # noqa: A002 - the name matches the JavaScript SDK
        *,
        name: str | None = None,
    ) -> float:
        """Return a number from 0 to 1.

        The first entry of ``scale`` is 0, the last entry is 1, and the entries
        between them are evenly spaced. The number can fall between two
        entries.

        Limits:

        - 2 to :data:`MAX_SCORE_LEVELS` entries.
        - Every entry is a non-empty string.
        - Order the entries from lowest to highest.

        Args:
            context: The context to create the step in.
            question: The question that the scale answers.
            scale: The scale, from lowest to highest.
            input: The text or data that the question is about.
            name: The step name. The default is ``decider.score``.
        """
        validate_question(question)
        levels = validate_scale(scale)
        serialize_input(input)

        def body(remaining: float | None) -> float:
            answers = self._ask(
                input,
                {
                    _ANSWER: {
                        "type": "score",
                        "instructions": question,
                        "criteria": levels,
                    }
                },
                remaining,
            )
            # The model's score is the expected level index, from 0 to levels - 1.
            position = _number_field(answers.get(_ANSWER), "score", "score")
            last = len(levels) - 1
            if position < -_SCORE_TOLERANCE or position > last + _SCORE_TOLERANCE:
                raise invalid(
                    f"The decision model's score {position} is outside 0 to {last}."
                )
            return to_unit_score(position, len(levels))

        return float(
            self._run(context, "score", AiOperationSubType.DECIDER_SCORE, name, body)
        )


def decider(
    *,
    endpoint: str | None = None,
    headers: Mapping[str, str] | None = None,
    transport: DeciderTransport | None = None,
    model: str = DEFAULT_DECIDER_MODEL,
    threshold: float | None = None,
    retry_strategy: Callable[[Exception, int], RetryDecision] | None = None,
) -> Decider:
    """Create the four decision operations: ``check``, ``classify``,
    ``label``, and ``score``.

    Experimental. It can change or be removed in any release.

    Each call is one durable step that asks a decision model one question and
    returns a typed answer. :class:`Decider` lists each operation's limits.

    A decision model, such as Strands Decider, is not a large language model.
    It does not write text. It picks one of the caller's options or rates on a
    scale. So its answer is always one of the options, or a number in range.

    The model is a Strands Decider that you deployed. Every host takes the
    same request. Name the model one of two ways:

    1. ``endpoint``: the URL of its ``POST /v1/systemone`` route. The default
       is the ``STRANDS_DECIDER_ENDPOINT`` environment variable.
    2. ``transport``: the callable that sends the request. Use
       :func:`agentcore_transport`, :func:`sagemaker_transport`, or
       :func:`lambda_transport` for a model on AWS. They sign the call with
       the function's credentials. Write your own transport for any other
       host.

    This package does not host or check the model. It assumes that the model
    is deployed and ready.

    Example::

        decide = decider(transport=sagemaker_transport("strands-decider-2b"))


        @durable_execution
        def handler(ticket, context):
            if decide.check(context, "Is the customer asking for a human?", ticket):
                return escalate(ticket)
            return decide.classify(
                context, "Which team?", ["returns", "billing"], ticket
            )

    Args:
        endpoint: The full URL of the model's ``POST /v1/systemone`` route,
            such as ``http://127.0.0.1:8000/v1/systemone``. It must not contain
            a user name or password: pass credentials in ``headers``. Redirects
            are not followed.
        headers: Extra HTTP headers for ``endpoint``, such as an
            authorization header.
        transport: The callable that sends the request. Do not combine it with
            ``endpoint``.
        model: The value of the request's ``model`` field.
        threshold: The probability at or above which ``check`` and ``label``
            answer yes. From 0 to 1. The default is 0.5.
        retry_strategy: The step retry strategy. The default is
            :func:`default_ai_retry_strategy`.

    Raises:
        TypeError: When both ``endpoint`` and ``transport`` are set, when
            ``headers`` is set with ``transport``, or when neither
            is set and ``STRANDS_DECIDER_ENDPOINT`` is not set, or when the
            endpoint is not an absolute ``http:`` or ``https:`` URL or contains
            a user name or password.
        ValueError: When ``threshold`` is not between 0 and 1.
    """
    if transport is not None and endpoint is not None:
        msg = "decider() takes only one of transport and endpoint."
        raise TypeError(msg)
    if transport is not None and headers is not None:
        # The headers are sent only on the HTTP path. Dropping them silently
        # could send a request without its authorization header.
        msg = "decider() takes headers only with endpoint, not with transport."
        raise TypeError(msg)
    url = (
        endpoint if endpoint is not None else os.environ.get("STRANDS_DECIDER_ENDPOINT")
    )
    if transport is None:
        if not url:
            msg = (
                "decider() needs a transport, an endpoint, or the "
                "STRANDS_DECIDER_ENDPOINT environment variable."
            )
            raise TypeError(msg)
        transport = _http_transport(_parse_endpoint(url), headers)
    return Decider(
        transport=transport,
        model=model,
        threshold=_threshold(threshold),
        retry_strategy=retry_strategy or default_ai_retry_strategy,
    )
