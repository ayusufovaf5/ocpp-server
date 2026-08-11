from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

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
        ocpp_transaction_id: int | None = None,
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
        row.ocpp_transaction_id = (
            int(ocpp_transaction_id) if ocpp_transaction_id is not None else row.id
        )
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

    async def list_active_by_charger(self, charger_id: int) -> list[ChargingSession]:
        result = await self._db.execute(
            select(ChargingSession).where(
                ChargingSession.charger_id == charger_id,
                ChargingSession.status == "Active",
            )
        )
        return list(result.scalars().all())

    async def latest_with_ocpp_transaction_id(
        self,
        charger_id: int,
        *,
        connector_id: int | None = None,
    ) -> ChargingSession | None:
        stmt = select(ChargingSession).where(
            ChargingSession.charger_id == charger_id,
            ChargingSession.ocpp_transaction_id.is_not(None),
        )
        if connector_id is not None:
            stmt = stmt.where(ChargingSession.connector_id == connector_id)
        stmt = stmt.order_by(ChargingSession.started_at.desc()).limit(1)
        result = await self._db.execute(stmt)
        return result.scalar_one_or_none()

    async def latest_completed_by_charger_connector(
        self,
        charger_id: int,
        connector_id: int,
    ) -> ChargingSession | None:
        result = await self._db.execute(
            select(ChargingSession)
            .where(
                ChargingSession.charger_id == charger_id,
                ChargingSession.connector_id == connector_id,
                ChargingSession.status == "Completed",
                ChargingSession.stopped_at.is_not(None),
            )
            .order_by(ChargingSession.stopped_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def list_all_active(self) -> list[ChargingSession]:
        result = await self._db.execute(
            select(ChargingSession).where(ChargingSession.status == "Active")
        )
        return list(result.scalars().all())

    async def list_active_stale_by_meter(
        self,
        before: datetime,
    ) -> list[ChargingSession]:
        activity_at = func.coalesce(
            ChargingSession.last_meter_at,
            ChargingSession.started_at,
        )
        result = await self._db.execute(
            select(ChargingSession)
            .where(
                ChargingSession.status == "Active",
                activity_at < before,
            )
            .options(selectinload(ChargingSession.charger))
        )
        return list(result.scalars().all())

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

    async def latest_meter_value(self, session_id: int) -> MeterValue | None:
        result = await self._db.execute(
            select(MeterValue)
            .where(MeterValue.session_id == session_id)
            .order_by(MeterValue.timestamp.desc(), MeterValue.id.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def list_meter_values_desc(self, session_id: int) -> list[MeterValue]:
        result = await self._db.execute(
            select(MeterValue)
            .where(MeterValue.session_id == session_id)
            .order_by(MeterValue.timestamp.desc(), MeterValue.id.desc())
        )
        return list(result.scalars().all())

    async def stop(
        self,
        charging_session: ChargingSession,
        *,
        stopped_at: datetime,
        meter_stop: int | None,
        end_reason: str | None = "station_stop",
        meter_stop_estimated: bool = False,
    ) -> ChargingSession:
        charging_session.stopped_at = stopped_at
        charging_session.meter_stop = meter_stop
        charging_session.meter_stop_estimated = meter_stop_estimated
        charging_session.end_reason = end_reason
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
