from __future__ import annotations

import asyncio
import json
import ssl
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


def create_evpoint_ssl_context(
    *,
    ca_bundle: str | None = None,
    verify: bool = True,
) -> ssl.SSLContext:
    if not verify:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context

    context = ssl.create_default_context()
    if ca_bundle:
        path = Path(ca_bundle)
        if path.is_file():
            context.load_verify_locations(cafile=str(path))
        else:
            logger.warning("evpoint.ca_bundle_missing", path=ca_bundle)
    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = True
    return context


class EvpointPushError(Exception):
    pass


def post_live_update_sync(
    url: str,
    payload: dict[str, Any],
    *,
    ssl_context: ssl.SSLContext | None,
    timeout_seconds: float,
) -> int:
    body = json.dumps(payload).encode("utf-8")
    current_url = url
    # Follow HTTPS redirects (EvPoint UseHttpsRedirection returns 307).
    for _ in range(5):
        request = urllib.request.Request(
            current_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(
                request, context=ssl_context, timeout=timeout_seconds
            ) as response:
                return int(response.status)
        except urllib.error.HTTPError as exc:
            if exc.code not in (301, 302, 307, 308):
                raise
            location = exc.headers.get("Location")
            if not location:
                raise
            current_url = urllib.parse.urljoin(current_url, location)
    raise EvpointPushError(f"Too many redirects for {url}")


async def post_live_update(
    url: str,
    payload: dict[str, Any],
    *,
    ssl_context: ssl.SSLContext | None,
    timeout_seconds: float,
) -> int:
    return await asyncio.to_thread(
        post_live_update_sync,
        url,
        payload,
        ssl_context=ssl_context,
        timeout_seconds=timeout_seconds,
    )
