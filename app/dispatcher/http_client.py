"""Factory for the shared outbound HTTP client."""

import httpx


def create_http_client() -> httpx.AsyncClient:
    limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
    return httpx.AsyncClient(limits=limits, follow_redirects=False)
