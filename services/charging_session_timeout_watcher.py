from sqlalchemy.ext.asyncio import AsyncSession

import structlog

from db.models import ChargingSession
from db.time import seconds_ago, utc_now, utc_now_iso
from repositories.session_repository import SessionRepository

logger = structlog.get_logger(__name__)


class ChargingSessionTimeoutWatcher:
    def __init__(self, db: AsyncSession) -> None:
        self._db = db
        self._sessions = SessionRepository(db)

    def arm(self, session: ChargingSession) -> None:
        now = utc_now()
        session.last_meter_at = now
        logger.info(
            "charging_session_timeout.armed",
            session_id=session.id,
            ocpp_transaction_id=session.ocpp_transaction_id,
            charger_id=session.charger_id,
            connector_id=session.connector_id,
            last_meter_at=now.isoformat(),
        )

    def extend(self, session: ChargingSession) -> None:
        now = utc_now()
        session.last_meter_at = now
        logger.info(
            "charging_session_timeout.extended",
            session_id=session.id,
            ocpp_transaction_id=session.ocpp_transaction_id,
            charger_id=session.charger_id,
            connector_id=session.connector_id,
            last_meter_at=now.isoformat(),
        )

    async def close_expired(self, timeout_seconds: int) -> int:
        from services.session_service import SessionService

        cutoff = seconds_ago(timeout_seconds)
        stale = await self._sessions.list_active_stale_by_meter(cutoff)
        if not stale:
            return 0

        closed = 0
        sessions = SessionService(self._db)
        now_iso = utc_now_iso()
        for session in stale:
            charger = session.charger
            if charger is None or session.ocpp_transaction_id is None:
                continue
            latest = await self._sessions.latest_meter_value(session.id)
            if latest is not None:
                meter_stop = int(round(latest.value))
            else:
                meter_stop = int(session.meter_start)
            stopped = await sessions.stop_transaction(
                charge_point_id=charger.charge_point_id,
                transaction_id=session.ocpp_transaction_id,
                meter_stop=meter_stop,
                timestamp=now_iso,
                connector_id=session.connector_id,
            )
            if stopped is None:
                continue
            closed += 1
            logger.info(
                "charging_session_timeout.closed",
                session_id=session.id,
                ocpp_transaction_id=session.ocpp_transaction_id,
                charge_point_id=charger.charge_point_id,
                connector_id=session.connector_id,
                timeout_seconds=timeout_seconds,
                meter_stop=meter_stop,
                end_reason=stopped.effective_end_reason,
            )
        return closed
