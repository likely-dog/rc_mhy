"""Unit tests for strict vendor request rendering and config validation."""

from pathlib import Path

import pytest

from app.dispatcher.renderer import RenderError, render_request
from app.dispatcher.vendors import VendorConfig, load_vendor_registry


def test_render_request_when_payload_complete_renders_json_url_and_headers():
    """Given complete payload, all templated request components render."""
    vendor = VendorConfig(
        code="v",
        endpoint="https://x.test/{{ payload.id }}",
        method="POST",
        timeout_seconds=1,
        headers={"X-ID": "{{ payload.id }}"},
        body_template='{"id":"{{ payload.id }}"}',
    )
    result = render_request(vendor, {"id": "42"})
    assert result == {
        "method": "POST",
        "url": "https://x.test/42",
        "headers": {"X-ID": "42"},
        "json": {"id": "42"},
    }


def test_render_request_when_field_missing_raises_render_error():
    """Given a missing field, StrictUndefined prevents a malformed outbound request."""
    vendor = VendorConfig(
        code="v",
        endpoint="https://x.test",
        method="POST",
        timeout_seconds=1,
        body_template='{"id":"{{ payload.id }}"}',
    )
    with pytest.raises(RenderError):
        render_request(vendor, {})


def test_load_vendor_registry_when_env_present_expands_secret(tmp_path: Path, monkeypatch):
    """Given an environment placeholder, config loading expands it once."""
    monkeypatch.setenv("VENDOR_SECRET", "secret")
    path = tmp_path / "vendors.yaml"
    path.write_text(
        "vendors:\n  - code: v\n    endpoint: https://x.test\n    method: POST\n"
        "    timeout_seconds: 1\n    headers:\n      X-Key: ${VENDOR_SECRET}\n",
        encoding="utf-8",
    )
    assert load_vendor_registry(path)["v"].headers["X-Key"] == "secret"


def test_load_vendor_registry_when_duplicate_rejects_configuration(tmp_path: Path):
    """Given duplicate codes, startup validation fails clearly."""
    path = tmp_path / "vendors.yaml"
    path.write_text(
        "vendors:\n  - {code: v, endpoint: https://x.test, method: POST, timeout_seconds: 1}\n"
        "  - {code: v, endpoint: https://y.test, method: POST, timeout_seconds: 1}\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="duplicate vendor"):
        load_vendor_registry(path)
