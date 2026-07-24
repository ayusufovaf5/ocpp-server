from __future__ import annotations

from typing import Any, Literal

from sqlalchemy.ext.asyncio import AsyncSession

from ocpp16 import protocol
from repositories.charger_repository import ChargerRepository
from services.errors import ChargerOfflineError
from state.connection_registry import get_connection_registry


class RemoteControlService:
    def __init__(self, db: AsyncSession) -> None:
        self._chargers = ChargerRepository(db)

    async def call(
        self,
        charge_point_id: str,
        action: str,
        payload: dict[str, Any] | None = None,
        *,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        charger = await self._chargers.get_by_charge_point_id(charge_point_id)
        if charger is None or charger.disconnected_at is not None:
            raise ChargerOfflineError(charge_point_id)
        if not get_connection_registry().is_connected(charge_point_id):
            raise ChargerOfflineError(charge_point_id)
        return await protocol.call(
            charge_point_id,
            action,
            payload,
            timeout_seconds=timeout_seconds,
        )

    async def reset(
        self,
        charge_point_id: str,
        reset_type: Literal["hard", "soft"],
        *,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        ocpp_type = "Hard" if reset_type == "hard" else "Soft"
        return await self.call(
            charge_point_id,
            "Reset",
            {"type": ocpp_type},
            timeout_seconds=timeout_seconds,
        )

    async def change_availability(
        self,
        charge_point_id: str,
        *,
        is_available: bool,
        connector_id: int = 0,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        availability = "Operative" if is_available else "Inoperative"
        return await self.call(
            charge_point_id,
            "ChangeAvailability",
            {"connectorId": connector_id, "type": availability},
            timeout_seconds=timeout_seconds,
        )

    async def unlock_connector(
        self,
        charge_point_id: str,
        connector_id: int,
        *,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        return await self.call(
            charge_point_id,
            "UnlockConnector",
            {"connectorId": connector_id},
            timeout_seconds=timeout_seconds,
        )
