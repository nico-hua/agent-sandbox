"""Bounded JSON object requests shared by sandbox and workspace mutations."""

import json
from typing import NoReturn

from fastapi import Request


MAX_JSON_BODY_BYTES = 4096


class RequestBodyTooLarge(Exception):
    """The streamed body exceeded the API's JSON byte limit."""


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Reject duplicate keys before a later value can hide the first."""
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def _reject_json_constant(_value: str) -> NoReturn:
    """Reject nonstandard numeric tokens accepted by Python's decoder."""
    raise ValueError("nonstandard JSON constant")


async def read_json_object(request: Request, *, allow_empty: bool = False) -> dict[str, object]:
    """Stop at the first over-limit chunk and decode one strict JSON object."""
    body = bytearray()
    async for chunk in request.stream():
        if len(chunk) > MAX_JSON_BODY_BYTES - len(body):
            raise RequestBodyTooLarge()
        body.extend(chunk)
    if not body and allow_empty:
        return {}
    try:
        payload = json.loads(body, parse_constant=_reject_json_constant, object_pairs_hook=_unique_object)
    except RecursionError:
        raise ValueError("JSON nesting is too deep") from None
    if not isinstance(payload, dict):
        raise ValueError("request body must be a JSON object")
    return payload
