import asyncio

import structlog

import db as db_module
from config import get_settings
from services.charging_session_timeout_watcher import ChargingSessionTimeoutWatcher

logger = structlog.get_logger(__name__)


async def run_charging_session_timeout_monitor() -> None:
    settings = get_settings()
    interval = max(5, settings.heartbeat_check_interval_seconds)
    timeout_seconds = settings.charging_session_timeout_seconds
    logger.info(
        "charging_session_timeout_monitor.started",
        interval=interval,
        timeout_seconds=timeout_seconds,
    )
    while True:
        try:
            async with db_module.async_session_factory() as session:
                closed = await ChargingSessionTimeoutWatcher(session).close_expired(
                    timeout_seconds
                )
                if closed:
                    logger.info(
                        "charging_session_timeout_monitor.closed_sessions",
                        count=closed,
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("charging_session_timeout_monitor.error")
        await asyncio.sleep(interval)
