from __future__ import annotations

from typing import Any

import structlog

from events.consumer import EventConsumer
from events.types import EventType

logger = structlog.get_logger(__name__)

_PAYLOAD_KEYS = (
    "charge_point_id",
    "connector_id",
    "session_id",
    "ocpp_transaction_id",
    "id_tag",
    "status",
    "meter_start",
    "meter_stop",
    "end_reason",
    "count",
    "resumed",
)


class LoggingConsumer(EventConsumer):
    def __init__(self, *, consumer_name: str = "logging-consumer") -> None:
        super().__init__(consumer_name=consumer_name, block_ms=500)

    async def handle(
        self,
        event_type: EventType,
        payload: dict[str, Any],
        message_id: str,
    ) -> None:
        fields = {
            key: payload[key]
            for key in _PAYLOAD_KEYS
            if key in payload and payload[key] is not None
        }
        logger.info("events.bus_event", type=event_type.value, **fields)
