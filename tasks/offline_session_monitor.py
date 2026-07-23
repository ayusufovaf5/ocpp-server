import asyncio

import structlog

import db as db_module
from config import get_settings
from services.session_service import SessionService

logger = structlog.get_logger(__name__)


async def run_offline_session_monitor() -> None:
    settings = get_settings()
    interval = max(5, settings.heartbeat_check_interval_seconds)
    grace = settings.offline_session_grace_period_seconds
    logger.info(
        "offline_session_monitor.started",
        interval=interval,
        grace_period_seconds=grace,
    )
    while True:
        try:
            async with db_module.async_session_factory() as session:
                closed = await SessionService(session).close_offline_timed_out_sessions(grace)
                if closed:
                    logger.info("offline_session_monitor.closed_sessions", count=closed)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("offline_session_monitor.error")
        await asyncio.sleep(interval)
