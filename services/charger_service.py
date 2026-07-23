import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Charger
from db.time import seconds_ago, utc_now
from repositories.charger_repository import ChargerRepository
from services.status import normalize_connector_status

logger = structlog.get_logger(__name__)


class ChargerService:
    def __init__(self, db: AsyncSession) -> None:
        self._repo = ChargerRepository(db)
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

    async def heartbeat(self, charge_point_id: str) -> Charger | None:
        try:
            charger = await self._repo.touch_heartbeat(charge_point_id, utc_now())
            if charger is None:
                logger.warning(
                    "ocpp.heartbeat_unknown_charge_point",
                    charge_point_id=charge_point_id,
                )
            await self._db.commit()
            return charger
        except Exception:
            await self._db.rollback()
            raise

    async def update_status(self, charge_point_id: str, status: str) -> Charger | None:
        try:
            charger = await self._repo.set_status(
                charge_point_id,
                normalize_connector_status(status),
                now=utc_now(),
            )
            if charger is None:
                logger.warning(
                    "ocpp.status_notification_unknown_charge_point",
                    charge_point_id=charge_point_id,
                    status=status,
                )
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

    async def get(self, charge_point_id: str) -> Charger | None:
        return await self._repo.get_by_charge_point_id(charge_point_id)
