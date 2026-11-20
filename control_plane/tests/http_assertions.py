"""Assertions for the public control-plane JSON response contract."""

from httpx import Response


def success_data(response: Response) -> object:
    """Verify the complete success envelope before inspecting or using business data."""
    payload = response.json()
    assert response.status_code == 200
    assert type(payload["code"]) is int
    assert payload == {"code": 200, "message": "ok", "data": payload["data"]}
    return payload["data"]


def failure_body(response: Response) -> dict:
    """Verify HTTP and JSON status, field types and the complete failure envelope."""
    payload = response.json()
    assert response.status_code in (400, 401, 403, 404, 405, 409, 413, 429, 500, 503, 504)
    assert type(payload["code"]) is int
    assert payload["code"] == response.status_code
    assert isinstance(payload["message"], str) and payload["message"]
    assert payload["data"] is None or isinstance(payload["data"], dict)
    assert set(payload) == {"code", "message", "data"}
    return payload


def assert_failure(response: Response, message: str, data: dict[str, str] | None = None) -> None:
    """Compare the exact public message and diagnostic data after checking the envelope."""
    payload = failure_body(response)
    assert payload["message"] == message
    assert payload["data"] == data
