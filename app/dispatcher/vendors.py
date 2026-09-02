"""Load and validate declarative vendor request templates."""

import os
import re
from pathlib import Path
from urllib.parse import urlsplit

import yaml
from pydantic import BaseModel, Field, field_validator

_JINJA_EXPR = re.compile(r"{{.*?}}")


class VendorConfig(BaseModel):
    code: str = Field(min_length=1, max_length=64)
    endpoint: str
    method: str
    timeout_seconds: float = Field(gt=0, le=60)
    headers: dict[str, str] = Field(default_factory=dict)
    body_template: str | None = None
    event_types: list[str] | None = None

    @field_validator("method")
    @classmethod
    def validate_method(cls, value: str) -> str:
        normalized = value.upper()
        if normalized not in {"GET", "POST", "PUT", "PATCH", "DELETE"}:
            raise ValueError("method must be GET, POST, PUT, PATCH, or DELETE")
        return normalized

    @field_validator("endpoint")
    @classmethod
    def validate_endpoint(cls, value: str) -> str:
        probe = _JINJA_EXPR.sub("placeholder", value)
        parsed = urlsplit(probe)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("endpoint must be an HTTP(S) URL")
        return value


def load_vendor_registry(path: str | Path) -> dict[str, VendorConfig]:
    config_path = Path(path)
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    entries = raw.get("vendors")
    if not isinstance(entries, list):
        raise ValueError("vendors.yaml must contain a 'vendors' list")

    registry: dict[str, VendorConfig] = {}
    for entry in entries:
        expanded = _expand_environment(entry)
        vendor = VendorConfig.model_validate(expanded)
        if vendor.code in registry:
            raise ValueError(f"duplicate vendor code: {vendor.code}")
        registry[vendor.code] = vendor
    return registry


def _expand_environment(value):
    if isinstance(value, str):
        return os.path.expandvars(value)
    if isinstance(value, dict):
        return {key: _expand_environment(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand_environment(item) for item in value]
    return value
