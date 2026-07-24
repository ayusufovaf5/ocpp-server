from __future__ import annotations

import json
from typing import Any

import structlog

from events.types import EventType
from state.redis_client import get_redis

logger = structlog.get_logger(__name__)

STREAM_NAME = "csms.events"


class EventPublisher:
    def __init__(self, *, stream: str = STREAM_NAME) -> None:
        self._stream = stream

    async def publish(self, event_type: EventType, payload: dict[str, Any]) -> str | None:
        try:
            client = await get_redis()
            message_id = await client.xadd(
                self._stream,
                {
                    "type": event_type.value,
                    "payload": json.dumps(payload, default=str),
                },
            )
            return str(message_id)
        except Exception:
            logger.exception(
                "events.publish_failed",
                event_type=event_type.value,
            )
            return None


_publisher: EventPublisher | None = None


def get_publisher() -> EventPublisher:
    global _publisher
    if _publisher is None:
        _publisher = EventPublisher()
    return _publisher


def set_publisher(publisher: EventPublisher | None) -> None:
    global _publisher
    _publisher = publisher
