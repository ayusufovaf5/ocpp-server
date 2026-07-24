from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import redis.exceptions
import structlog

from events.consumer import EventConsumer
from events.types import EventType
from state.redis_client import get_redis

logger = structlog.get_logger(__name__)

_LIVE_EVENT_TYPES = frozenset(
    {
        EventType.SESSION_STARTED,
        EventType.SESSION_STOPPED,
        EventType.METER_VALUES_RECEIVED,
        EventType.CHARGER_STATUS_CHANGED,
        EventType.CHARGER_CONNECTED,
        EventType.CHARGER_DISCONNECTED,
    }
)


class LiveStatusStreamConsumer(EventConsumer):
    def __init__(self, notify: asyncio.Queue[None]) -> None:
        self._group_id = f"live-status-sse-{uuid4().hex}"
        super().__init__(
            group=self._group_id,
            consumer_name="sse",
            block_ms=200,
            count=10,
        )
        self._notify = notify

    @property
    def group_id(self) -> str:
        return self._group_id

    async def ensure_group(self) -> None:
        client = await get_redis()
        try:
            await client.xgroup_create(self._stream, self._group, id="$", mkstream=True)
        except redis.exceptions.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def handle(
        self,
        event_type: EventType,
        payload: dict[str, Any],
        message_id: str,
    ) -> None:
        if event_type in _LIVE_EVENT_TYPES:
            try:
                self._notify.put_nowait(None)
            except asyncio.QueueFull:
                pass

    async def destroy_group(self) -> None:
        try:
            client = await get_redis()
            await client.xgroup_destroy(self._stream, self._group)
        except Exception as exc:
            logger.warning(
                "events.live_status_group_destroy_failed",
                group=self._group,
                error=str(exc),
            )


async def assert_redis_ready() -> None:
    client = await get_redis()
    pong = await client.ping()
    if not pong:
        raise ConnectionError("Redis ping failed")
