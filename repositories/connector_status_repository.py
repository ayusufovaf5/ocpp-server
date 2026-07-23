from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import ConnectorStatus


class ConnectorStatusRepository:
    def __init__(self, db: AsyncSession) -> None:
        self._db = db

    async def upsert(
        self,
        *,
        charger_id: int,
        connector_id: int,
        status: str,
        updated_at: datetime,
    ) -> ConnectorStatus:
        result = await self._db.execute(
            select(ConnectorStatus).where(
                ConnectorStatus.charger_id == charger_id,
                ConnectorStatus.connector_id == connector_id,
            )
        )
        row = result.scalar_one_or_none()
        if row is None:
            row = ConnectorStatus(
                charger_id=charger_id,
                connector_id=connector_id,
                status=status,
                updated_at=updated_at,
            )
            self._db.add(row)
        else:
            row.status = status
            row.updated_at = updated_at
        await self._db.flush()
        return row

    async def list_by_charger(self, charger_id: int) -> list[ConnectorStatus]:
        result = await self._db.execute(
            select(ConnectorStatus)
            .where(ConnectorStatus.charger_id == charger_id)
            .order_by(ConnectorStatus.connector_id)
        )
        return list(result.scalars().all())
