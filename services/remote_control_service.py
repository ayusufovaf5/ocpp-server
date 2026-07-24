from __future__ import annotations

from typing import Any, Literal

from sqlalchemy.ext.asyncio import AsyncSession

from ocpp16 import protocol
from repositories.charger_repository import ChargerRepository
from repositories.session_repository import SessionRepository
from services.errors import (
    AmbiguousActiveSessionError,
    ChargerOfflineError,
    NoActiveSessionError,
)
from state.connection_registry import get_connection_registry


class RemoteControlService:
    def __init__(self, db: AsyncSession) -> None:
        self._db = db
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

    async def update_firmware(
        self,
        charge_point_id: str,
        location: str,
        *,
        retrieve_date: str | None = None,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        from db.time import utc_now_iso

        return await self.call(
            charge_point_id,
            "UpdateFirmware",
            {
                "location": location,
                "retrieveDate": retrieve_date or utc_now_iso(),
            },
            timeout_seconds=timeout_seconds,
        )

    async def remote_start(
        self,
        charge_point_id: str,
        *,
        connector_id: int,
        id_tag: str,
        transaction_id: int,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        # transaction_id is accepted for REST parity with EvPoint/old webserver but is
        # not stored and does not create a session (see ADR 016).
        _ = transaction_id
        return await self.call(
            charge_point_id,
            "RemoteStartTransaction",
            {"connectorId": connector_id, "idTag": id_tag},
            timeout_seconds=timeout_seconds,
        )

    async def remote_stop(
        self,
        charge_point_id: str,
        *,
        transaction_id: int,
        connector_id: int | None = None,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        # REST transaction_id is the EvPoint app charging id — do not send it on the
        # wire. Resolve the station ocpp_transaction_id from the active DB session.
        _ = transaction_id

        charger = await self._chargers.get_by_charge_point_id(charge_point_id)
        if charger is None or charger.disconnected_at is not None:
            raise ChargerOfflineError(charge_point_id)
        if not get_connection_registry().is_connected(charge_point_id):
            raise ChargerOfflineError(charge_point_id)

        sessions = SessionRepository(self._db)
        if connector_id is not None:
            active = await sessions.get_active_by_charger_connector(charger.id, connector_id)
            if active is None:
                raise NoActiveSessionError(charge_point_id, connector_id=connector_id)
        else:
            active_list = await sessions.list_active_by_charger(charger.id)
            if not active_list:
                raise NoActiveSessionError(charge_point_id)
            if len(active_list) > 1:
                raise AmbiguousActiveSessionError(charge_point_id, len(active_list))
            active = active_list[0]

        if active.ocpp_transaction_id is None:
            raise NoActiveSessionError(charge_point_id, connector_id=active.connector_id)

        return await self.call(
            charge_point_id,
            "RemoteStopTransaction",
            {"transactionId": active.ocpp_transaction_id},
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
