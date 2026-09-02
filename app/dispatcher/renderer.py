"""Strictly render a vendor's URL, headers, and JSON body from a payload."""

import json
from typing import Any

from jinja2 import Environment, StrictUndefined

from app.dispatcher.vendors import VendorConfig

_environment = Environment(undefined=StrictUndefined, autoescape=False)


class RenderError(ValueError):
    pass


def render_request(vendor: VendorConfig, payload: dict[str, Any]) -> dict[str, Any]:
    context = {"payload": payload}
    try:
        url = _environment.from_string(vendor.endpoint).render(context)
        headers = {
            key: _environment.from_string(value).render(context)
            for key, value in vendor.headers.items()
        }
        request: dict[str, Any] = {"method": vendor.method, "url": url, "headers": headers}
        if vendor.body_template is not None:
            rendered_body = _environment.from_string(vendor.body_template).render(context)
            request["json"] = json.loads(rendered_body)
        return request
    except Exception as exc:
        raise RenderError(f"vendor template rendering failed: {exc}") from exc
