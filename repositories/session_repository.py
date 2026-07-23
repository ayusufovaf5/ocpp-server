from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import ChargingSession, MeterValue


class SessionRepository:
    def __init__(self, db: AsyncSession) -> None:
        self._db = db

    async def create(
        self,
        *,
        charger_id: int,
        connector_id: int,
        id_tag: str,
        started_at: datetime,
        meter_start: int,
    ) -> ChargingSession:
        row = ChargingSession(
            charger_id=charger_id,
            connector_id=connector_id,
            id_tag=id_tag,
            ocpp_transaction_id=None,
            started_at=started_at,
            meter_start=meter_start,
            status="Active",
        )
        self._db.add(row)
        await self._db.flush()
        row.ocpp_transaction_id = row.id
        await self._db.flush()
        return row

    async def get_active_by_charger_connector(
        self,
        charger_id: int,
        connector_id: int,
    ) -> ChargingSession | None:
        result = await self._db.execute(
            select(ChargingSession).where(
                ChargingSession.charger_id == charger_id,
                ChargingSession.connector_id == connector_id,
                ChargingSession.status == "Active",
            )
        )
        return result.scalar_one_or_none()

    async def get_any_active_by_charger(self, charger_id: int) -> ChargingSession | None:
        result = await self._db.execute(
            select(ChargingSession).where(
                ChargingSession.charger_id == charger_id,
                ChargingSession.status == "Active",
            )
        )
        return result.scalar_one_or_none()

    async def get_by_ocpp_transaction_id(
        self,
        ocpp_transaction_id: int,
    ) -> ChargingSession | None:
        result = await self._db.execute(
            select(ChargingSession).where(
                ChargingSession.ocpp_transaction_id == ocpp_transaction_id
            )
        )
        return result.scalar_one_or_none()

    async def stop(
        self,
        charging_session: ChargingSession,
        *,
        stopped_at: datetime,
        meter_stop: int | None,
    ) -> ChargingSession:
        charging_session.stopped_at = stopped_at
        charging_session.meter_stop = meter_stop
        charging_session.status = "Completed"
        await self._db.flush()
        return charging_session

    async def add_meter_value(
        self,
        *,
        session_id: int,
        timestamp: datetime,
        value: float,
        unit: str | None,
        measurand: str | None,
    ) -> MeterValue:
        row = MeterValue(
            session_id=session_id,
            timestamp=timestamp,
            value=value,
            unit=unit,
            measurand=measurand,
        )
        self._db.add(row)
        await self._db.flush()
        return row
