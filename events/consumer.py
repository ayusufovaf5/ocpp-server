from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from typing import Any

import redis.exceptions
import structlog

from events.types import EventType
from state.redis_client import get_redis

logger = structlog.get_logger(__name__)

STREAM_NAME = "csms.events"
DEFAULT_GROUP = "csms-consumers"


class EventConsumer(ABC):
    """Base Redis Streams consumer with a consumer group (at-least-once)."""

    def __init__(
        self,
        *,
        group: str = DEFAULT_GROUP,
        consumer_name: str,
        stream: str = STREAM_NAME,
        block_ms: int = 2000,
        count: int = 10,
    ) -> None:
        self._group = group
        self._consumer_name = consumer_name
        self._stream = stream
        self._block_ms = block_ms
        self._count = count
        self._running = False
        self._poll_seconds = max(self._block_ms / 1000.0, 0.05)

    async def ensure_group(self) -> None:
        client = await get_redis()
        try:
            await client.xgroup_create(self._stream, self._group, id="0", mkstream=True)
        except redis.exceptions.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    @abstractmethod
    async def handle(
        self,
        event_type: EventType,
        payload: dict[str, Any],
        message_id: str,
    ) -> None: ...

    async def run(self) -> None:
        self._running = True
        await self._wait_until_group_ready()
        logger.info(
            "events.consumer_started",
            group=self._group,
            consumer=self._consumer_name,
            stream=self._stream,
        )
        client = await get_redis()
        while self._running:
            try:
                # block=None = non-blocking. Redis BLOCK 0 means wait forever
                # (that collided with the client socket timeout and spammed errors).
                rows = await client.xreadgroup(
                    groupname=self._group,
                    consumername=self._consumer_name,
                    streams={self._stream: ">"},
                    count=self._count,
                    block=None,
                )
            except asyncio.CancelledError:
                raise
            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
                logger.warning(
                    "events.consumer_redis_unavailable",
                    consumer=self._consumer_name,
                    error=str(exc),
                )
                await asyncio.sleep(1)
                continue
            except Exception as exc:
                logger.warning(
                    "events.consumer_read_failed",
                    consumer=self._consumer_name,
                    error=f"{type(exc).__name__}: {exc}",
                )
                await asyncio.sleep(1)
                continue

            if not rows:
                try:
                    await asyncio.sleep(self._poll_seconds)
                except asyncio.CancelledError:
                    raise
                continue

            for _stream_name, messages in rows:
                for message_id, fields in messages:
                    await self._process_one(client, str(message_id), fields)

    async def _wait_until_group_ready(self) -> None:
        while self._running:
            try:
                await self.ensure_group()
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "events.consumer_waiting_for_redis",
                    consumer=self._consumer_name,
                    error=str(exc),
                )
                await asyncio.sleep(1)

    async def _process_one(self, client, message_id: str, fields: dict[str, str]) -> None:
        try:
            event_type = EventType(fields["type"])
            payload = json.loads(fields.get("payload") or "{}")
            await self.handle(event_type, payload, message_id)
            await client.xack(self._stream, self._group, message_id)
        except Exception as exc:
            logger.warning(
                "events.consumer_handle_failed",
                consumer=self._consumer_name,
                message_id=message_id,
                error=f"{type(exc).__name__}: {exc}",
            )

    def stop(self) -> None:
        self._running = False
