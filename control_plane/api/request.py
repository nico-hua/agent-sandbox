"""Bounded JSON object requests shared by sandbox and workspace mutations."""

import json
import re
from typing import NoReturn

from fastapi import Request

from control_plane.domain.sandbox import (
    SandboxCreateOptions,
    SandboxNetwork,
    SandboxResources,
    _validate_environment,
)

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
        raise TypeError("request body must be a JSON object")
    return payload


def parse_sandbox_creation(payload: dict[str, object]) -> SandboxCreateOptions:
    """Check permitted creation fields, delegating field validation to core models."""
    if set(payload) - {"environment", "ttl_seconds", "resources", "network", "workspace_id"}:
        raise ValueError("invalid creation object")
    if "workspace_id" in payload and payload["workspace_id"] is None:
        raise ValueError("workspace_id must be omitted or a valid workspace ID")
    resources = payload.get("resources", {})
    if not isinstance(resources, dict) or set(resources) - {"cpu_millis", "memory_mib"}:
        raise ValueError("invalid resources object")
    network = payload.get("network", {})
    if not isinstance(network, dict) or set(network) - {"allow_domains"}:
        raise ValueError("invalid network object")
    return SandboxCreateOptions(
        **{key: value for key, value in payload.items() if key not in ("resources", "network")},
        resources=SandboxResources(**resources),
        network=SandboxNetwork(**network),
    )


def parse_workspace_environment(payload: dict[str, object]) -> str:
    """Read the only accepted workspace creation field using the domain name rules."""
    if set(payload) - {"environment"}:
        raise ValueError("invalid workspace creation object")
    environment = payload.get("environment", "basic")
    if type(environment) is not str:
        raise ValueError("invalid workspace environment")
    _validate_environment(environment)
    return environment


def parse_sandbox_page(request: Request) -> tuple[int, str | None]:
    """Read one unambiguous list page before service-level limit and cursor checks."""
    pairs = list(request.query_params.multi_items())
    if any(key not in ("limit", "cursor") for key, _ in pairs) or len({key for key, _ in pairs}) != len(pairs):
        raise ValueError()
    parameters = dict(pairs)
    raw_limit = parameters.get("limit", "20")
    if len(raw_limit) > 20 or re.fullmatch(r"[0-9]+", raw_limit) is None:
        raise ValueError()
    limit = int(raw_limit)
    return limit, parameters.get("cursor")
