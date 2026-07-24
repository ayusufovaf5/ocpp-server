from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

import structlog
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import StreamingResponse

import db as db_module
from events.live_status_consumer import LiveStatusStreamConsumer, assert_redis_ready
from services.live_status_service import LiveStatusService

logger = structlog.get_logger(__name__)

router = APIRouter()


def _sse_line(payload: dict) -> str:
    return json.dumps(payload, default=str) + "\n"


@router.get("/timed-live-details")
async def timed_live_details() -> StreamingResponse:
    try:
        await assert_redis_ready()
    except Exception as exc:
        logger.warning("live_status.redis_unavailable", error=str(exc))
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"status": "error", "response": "Event bus unavailable"},
        ) from None

    async def event_stream() -> AsyncIterator[str]:
        notify: asyncio.Queue[None] = asyncio.Queue(maxsize=1)
        consumer = LiveStatusStreamConsumer(notify)
        consumer_task = asyncio.create_task(consumer.run())
        try:
            async with db_module.async_session_factory() as db:
                snapshot = await LiveStatusService(db).build_timed_live_payload()
            yield _sse_line(snapshot)

            while True:
                await notify.get()
                while True:
                    try:
                        notify.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                async with db_module.async_session_factory() as db:
                    snapshot = await LiveStatusService(db).build_timed_live_payload()
                yield _sse_line(snapshot)
        except asyncio.CancelledError:
            raise
        finally:
            consumer.stop()
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass
            await consumer.destroy_group()

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
