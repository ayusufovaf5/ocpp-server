import asyncio

import structlog

import db as db_module
from config import get_settings
from services.charger_service import ChargerService

logger = structlog.get_logger(__name__)


async def run_heartbeat_monitor() -> None:
    settings = get_settings()
    interval = max(5, settings.heartbeat_check_interval_seconds)
    timeout = settings.heartbeat_timeout_seconds
    logger.info("heartbeat_monitor.started", interval=interval, timeout=timeout)
    while True:
        try:
            async with db_module.async_session_factory() as session:
                service = ChargerService(session)
                count = await service.mark_stale_unavailable(timeout)
                if count:
                    logger.warning("heartbeat_monitor.marked_unavailable", count=count)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("heartbeat_monitor.error")
        await asyncio.sleep(interval)
