from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from config import get_settings
from events.consumer import EventConsumer
from events.evpoint_http import (
    EvpointPushError,
    create_evpoint_ssl_context,
    post_live_update,
)
from events.evpoint_payload import build_payload, charger_details_from_event
from events.types import EventType

logger = structlog.get_logger(__name__)

EVPOINT_GROUP = "evpoint-push"

_PUSH_EVENT_TYPES = frozenset(
    {
        EventType.SESSION_STARTED,
        EventType.SESSION_STOPPED,
        EventType.METER_VALUES_RECEIVED,
        EventType.CHARGER_STATUS_CHANGED,
        EventType.CHARGER_CONNECTED,
        EventType.CHARGER_DISCONNECTED,
    }
)

HttpPostFn = Callable[..., Awaitable[int]]


class EvpointPushConsumer(EventConsumer):
    def __init__(
        self,
        *,
        consumer_name: str = "evpoint-push",
        http_post: HttpPostFn | None = None,
        ssl_context=None,
    ) -> None:
        super().__init__(
            group=EVPOINT_GROUP,
            consumer_name=consumer_name,
            block_ms=500,
        )
        settings = get_settings()
        self._url = settings.evpoint_live_update_url
        self._max_attempts = max(1, settings.evpoint_push_max_attempts)
        self._backoff_seconds = max(0.0, settings.evpoint_push_backoff_seconds)
        self._timeout_seconds = settings.evpoint_push_timeout_seconds
        self._ssl_context = ssl_context or create_evpoint_ssl_context(
            ca_bundle=settings.evpoint_ca_bundle
        )
        self._http_post = http_post or self._default_http_post

    async def _default_http_post(self, url: str, payload: dict[str, Any], **_: Any) -> int:
        try:
            return await post_live_update(
                url,
                payload,
                ssl_context=self._ssl_context,
                timeout_seconds=self._timeout_seconds,
            )
        except Exception as exc:
            raise EvpointPushError(str(exc)) from exc

    async def handle(
        self,
        event_type: EventType,
        payload: dict[str, Any],
        message_id: str,
    ) -> None:
        if event_type not in _PUSH_EVENT_TYPES:
            return

        charge_point_id = payload.get("charge_point_id")
        if not charge_point_id:
            logger.warning(
                "events.evpoint_push_missing_charge_point_id",
                type=event_type.value,
                message_id=message_id,
            )
            return

        details = charger_details_from_event(event_type, payload)
        body = build_payload(str(charge_point_id), details)

        last_error: BaseException | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                status = await self._http_post(
                    self._url,
                    body,
                    ssl_context=self._ssl_context,
                    timeout_seconds=self._timeout_seconds,
                )
                logger.info(
                    "events.evpoint_push_ok",
                    charge_point_id=charge_point_id,
                    type=event_type.value,
                    http_status=status,
                    attempt=attempt,
                )
                return
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "events.evpoint_push_attempt_failed",
                    charge_point_id=charge_point_id,
                    type=event_type.value,
                    message_id=message_id,
                    attempt=attempt,
                    max_attempts=self._max_attempts,
                    error=f"{type(exc).__name__}: {exc}",
                )
                if attempt < self._max_attempts and self._backoff_seconds > 0:
                    await asyncio.sleep(self._backoff_seconds * attempt)

        logger.warning(
            "events.evpoint_push_gave_up",
            charge_point_id=charge_point_id,
            type=event_type.value,
            message_id=message_id,
            attempts=self._max_attempts,
            error=f"{type(last_error).__name__}: {last_error}" if last_error else None,
        )
