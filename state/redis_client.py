from __future__ import annotations

import redis.asyncio as redis

from config import get_settings

_redis: redis.Redis | None = None


def set_redis(client: redis.Redis | None) -> None:
    global _redis
    _redis = client


async def get_redis() -> redis.Redis:
    global _redis
    if _redis is None:
        settings = get_settings()
        _redis = redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_connect_timeout=5.0,
            socket_timeout=None,
        )
    return _redis


async def close_redis() -> None:
    global _redis
    if _redis is not None:
        await _redis.aclose()
        _redis = None
