from __future__ import annotations

import asyncio
import json
import ssl
import urllib.request
from typing import Any


def create_evpoint_ssl_context(*, ca_bundle: str | None = None) -> ssl.SSLContext:
    context = ssl.create_default_context()
    if ca_bundle:
        context.load_verify_locations(cafile=ca_bundle)
    if context.verify_mode == ssl.CERT_NONE:
        context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = True
    return context


def post_live_update_sync(
    url: str,
    payload: dict[str, Any],
    *,
    ssl_context: ssl.SSLContext,
    timeout_seconds: float,
) -> int:
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, context=ssl_context, timeout=timeout_seconds) as response:
        return int(response.status)


async def post_live_update(
    url: str,
    payload: dict[str, Any],
    *,
    ssl_context: ssl.SSLContext,
    timeout_seconds: float,
) -> int:
    return await asyncio.to_thread(
        post_live_update_sync,
        url,
        payload,
        ssl_context=ssl_context,
        timeout_seconds=timeout_seconds,
    )


class EvpointPushError(Exception):
    pass
