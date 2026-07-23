from datetime import datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Charger


class ChargerRepository:
    def __init__(self, db: AsyncSession) -> None:
        self._db = db

    async def get_by_charge_point_id(self, charge_point_id: str) -> Charger | None:
        result = await self._db.execute(
            select(Charger).where(Charger.charge_point_id == charge_point_id)
        )
        return result.scalar_one_or_none()

    async def upsert_on_boot(
        self,
        *,
        charge_point_id: str,
        vendor: str,
        model: str,
        firmware_version: str | None,
        connector_count: int,
        now: datetime,
    ) -> Charger:
        charger = await self.get_by_charge_point_id(charge_point_id)
        if charger is None:
            charger = Charger(
                charge_point_id=charge_point_id,
                vendor=vendor,
                model=model,
                firmware_version=firmware_version,
                connector_count=connector_count,
                status="Available",
                last_heartbeat=now,
            )
            self._db.add(charger)
        else:
            charger.vendor = vendor
            charger.model = model
            charger.firmware_version = firmware_version
            charger.connector_count = max(charger.connector_count, connector_count)
            charger.last_heartbeat = now
            if charger.status in {"Unknown", "Unavailable"}:
                charger.status = "Available"
        await self._db.flush()
        return charger

    async def touch_heartbeat(self, charge_point_id: str, now: datetime) -> Charger | None:
        charger = await self.get_by_charge_point_id(charge_point_id)
        if charger is None:
            return None
        charger.last_heartbeat = now
        if charger.status == "Unavailable":
            charger.status = "Available"
        await self._db.flush()
        return charger

    async def set_status(
        self,
        charge_point_id: str,
        status: str,
        *,
        now: datetime | None = None,
    ) -> Charger | None:
        charger = await self.get_by_charge_point_id(charge_point_id)
        if charger is None:
            return None
        charger.status = status
        if now is not None:
            charger.last_heartbeat = now
        await self._db.flush()
        return charger

    async def mark_stale_unavailable(self, *, older_than: datetime) -> int:
        result = await self._db.execute(
            update(Charger)
            .where(
                Charger.last_heartbeat.is_not(None),
                Charger.last_heartbeat < older_than,
                Charger.status != "Unavailable",
            )
            .values(status="Unavailable")
            .returning(Charger.id)
        )
        rows = result.fetchall()
        await self._db.flush()
        return len(rows)
