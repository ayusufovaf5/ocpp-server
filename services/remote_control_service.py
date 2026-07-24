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

    async def get_configuration(
        self,
        charge_point_id: str,
        keys: list[str] | None = None,
        *,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if keys:
            payload["key"] = keys
        return await self.call(
            charge_point_id,
            "GetConfiguration",
            payload,
            timeout_seconds=timeout_seconds,
        )

    async def change_configuration(
        self,
        charge_point_id: str,
        configuration: dict[str, Any],
        *,
        timeout_seconds: float | None = None,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for key, value in configuration.items():
            result = await self.call(
                charge_point_id,
                "ChangeConfiguration",
                {"key": key, "value": str(value)},
                timeout_seconds=timeout_seconds,
            )
            results.append({"key": key, **result})
        return results

    async def trigger_message(
        self,
        charge_point_id: str,
        message_type: str,
        *,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any] | str:
        requested = _resolve_message_trigger(message_type)
        if requested is None:
            return f"Invalid message trigger: {message_type}"
        return await self.call(
            charge_point_id,
            "TriggerMessage",
            {"requestedMessage": requested},
            timeout_seconds=timeout_seconds,
        )


_MESSAGE_TRIGGER_WIRE = {
    "BootNotification",
    "DiagnosticsStatusNotification",
    "FirmwareStatusNotification",
    "Heartbeat",
    "MeterValues",
    "StatusNotification",
    "LogStatusNotification",
    "SignChargePointCertificate",
}

_MESSAGE_TRIGGER_ALIASES = {
    "boot_notification": "BootNotification",
    "bootNotification": "BootNotification",
    "diagnostics_status_notification": "DiagnosticsStatusNotification",
    "diagnosticsStatusNotification": "DiagnosticsStatusNotification",
    "firmware_status_notification": "FirmwareStatusNotification",
    "firmwareStatusNotification": "FirmwareStatusNotification",
    "heartbeat": "Heartbeat",
    "meter_values": "MeterValues",
    "meterValues": "MeterValues",
    "status_notification": "StatusNotification",
    "statusNotification": "StatusNotification",
    "log_status_notification": "LogStatusNotification",
    "sign_charge_point_certificate": "SignChargePointCertificate",
}


def _resolve_message_trigger(message_type: str) -> str | None:
    if message_type in _MESSAGE_TRIGGER_WIRE:
        return message_type
    return _MESSAGE_TRIGGER_ALIASES.get(message_type)
