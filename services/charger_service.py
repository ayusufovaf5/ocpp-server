from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Charger
from db.time import seconds_ago, utc_now
from repositories.charger_repository import ChargerRepository
from repositories.connector_status_repository import ConnectorStatusRepository
from services.status import aggregate_station_status, normalize_connector_status

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class ConnectorStatusView:
    connector_id: int
    status: str
    updated_at: datetime


@dataclass(frozen=True)
class ChargerStatusView:
    charge_point_id: str
    status: str
    legacy_status: str
    connectors: list[ConnectorStatusView]
    charge_point_status: str | None


class ChargerService:
    def __init__(self, db: AsyncSession) -> None:
        self._repo = ChargerRepository(db)
        self._connector_statuses = ConnectorStatusRepository(db)
        self._db = db

    async def register_boot(
        self,
        *,
        charge_point_id: str,
        vendor: str,
        model: str,
        firmware_version: str | None = None,
        connector_count: int = 1,
    ) -> Charger:
        try:
            charger = await self._repo.upsert_on_boot(
                charge_point_id=charge_point_id,
                vendor=vendor,
                model=model,
                firmware_version=firmware_version,
                connector_count=connector_count,
                now=utc_now(),
            )
            await self._db.commit()
            return charger
        except Exception:
            await self._db.rollback()
            raise

    async def heartbeat(
        self,
        charge_point_id: str,
        *,
        connector_id: int | None = None,
        status: str | None = None,
    ) -> Charger | None:
        try:
            now = utc_now()
            charger = await self._repo.touch_heartbeat(charge_point_id, now)
            if charger is None:
                logger.warning(
                    "ocpp.heartbeat_unknown_charge_point",
                    charge_point_id=charge_point_id,
                )
            elif connector_id is not None and status is not None:
                await self._connector_statuses.upsert(
                    charger_id=charger.id,
                    connector_id=connector_id,
                    status=normalize_connector_status(status),
                    updated_at=now,
                )
            await self._db.commit()
            return charger
        except Exception:
            await self._db.rollback()
            raise

    async def update_status(
        self,
        charge_point_id: str,
        status: str,
        *,
        connector_id: int | None = None,
    ) -> Charger | None:
        try:
            now = utc_now()
            normalized = normalize_connector_status(status)
            charger = await self._repo.set_status(
                charge_point_id,
                normalized,
                now=now,
            )
            if charger is None:
                logger.warning(
                    "ocpp.status_notification_unknown_charge_point",
                    charge_point_id=charge_point_id,
                    status=status,
                )
            elif connector_id is not None:
                await self._connector_statuses.upsert(
                    charger_id=charger.id,
                    connector_id=connector_id,
                    status=normalized,
                    updated_at=now,
                )
                if connector_id != 0:
                    await self._refresh_legacy_status_from_connectors(charger)
            await self._db.commit()
            return charger
        except Exception:
            await self._db.rollback()
            raise

    async def mark_stale_unavailable(self, timeout_seconds: int) -> int:
        try:
            count = await self._repo.mark_stale_unavailable(older_than=seconds_ago(timeout_seconds))
            await self._db.commit()
            return count
        except Exception:
            await self._db.rollback()
            raise

    async def mark_disconnected(self, charge_point_id: str) -> Charger | None:
        try:
            charger = await self._repo.mark_disconnected(charge_point_id, utc_now())
            await self._db.commit()
            return charger
        except Exception:
            await self._db.rollback()
            raise

    async def clear_disconnected(self, charge_point_id: str) -> Charger | None:
        try:
            charger = await self._repo.clear_disconnected(charge_point_id)
            await self._db.commit()
            return charger
        except Exception:
            await self._db.rollback()
            raise

    async def get(self, charge_point_id: str) -> Charger | None:
        return await self._repo.get_by_charge_point_id(charge_point_id)

    async def get_status_view(self, charge_point_id: str) -> ChargerStatusView | None:
        charger = await self._repo.get_by_charge_point_id(charge_point_id)
        if charger is None:
            return None

        rows = await self._connector_statuses.list_by_charger(charger.id)
        connectors = [
            ConnectorStatusView(
                connector_id=row.connector_id,
                status=row.status,
                updated_at=row.updated_at,
            )
            for row in rows
            if row.connector_id != 0
        ]
        charge_point_status = next(
            (row.status for row in rows if row.connector_id == 0),
            None,
        )
        aggregated = aggregate_station_status([c.status for c in connectors])
        return ChargerStatusView(
            charge_point_id=charger.charge_point_id,
            status=aggregated if aggregated is not None else charger.status,
            legacy_status=charger.status,
            connectors=connectors,
            charge_point_status=charge_point_status,
        )

    async def _refresh_legacy_status_from_connectors(self, charger: Charger) -> None:
        rows = await self._connector_statuses.list_by_charger(charger.id)
        aggregated = aggregate_station_status(
            [row.status for row in rows if row.connector_id != 0]
        )
        if aggregated is not None:
            charger.status = aggregated
            await self._db.flush()
