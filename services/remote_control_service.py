from __future__ import annotations

from typing import Any, Literal

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from ocpp16 import protocol
from repositories.charger_repository import ChargerRepository
from repositories.connector_status_repository import ConnectorStatusRepository
from repositories.session_repository import SessionRepository
from services.errors import (
    AmbiguousActiveSessionError,
    ChargerOfflineError,
    NoActiveSessionError,
)
from services.session_service import SessionService
from state.connection_registry import get_connection_registry
from state.connection_state import get_connection_state

logger = structlog.get_logger(__name__)


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

    async def get_diagnostics(
        self,
        charge_point_id: str,
        *,
        location: str,
        retries: int | None = None,
        retry_interval: int | None = None,
        start_time: str | None = None,
        stop_time: str | None = None,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"location": location}
        if retries is not None:
            payload["retries"] = retries
        if retry_interval is not None:
            payload["retryInterval"] = retry_interval
        if start_time is not None:
            payload["startTime"] = start_time
        if stop_time is not None:
            payload["stopTime"] = stop_time
        return await self.call(
            charge_point_id,
            "GetDiagnostics",
            payload,
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
        await get_connection_state().set_pending_remote_start(
            charge_point_id,
            connector_id,
            id_tag=id_tag,
            transaction_id=transaction_id,
        )
        return await self.call(
            charge_point_id,
            "RemoteStartTransaction",
            {"connectorId": connector_id, "idTag": id_tag},
            timeout_seconds=timeout_seconds,
        )

    async def set_charging_profile(
        self,
        charge_point_id: str,
        *,
        connector_id: int,
        transaction_id: int,
        limit: float,
        charging_rate_unit: Literal["W", "A"] = "W",
        number_phases: int | None = 3,
        stack_level: int = 0,
        charging_profile_id: int | None = None,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        profile_id = (
            int(charging_profile_id)
            if charging_profile_id is not None
            else max(1, int(transaction_id))
        )
        payload = build_set_charging_profile_payload(
            connector_id=connector_id,
            transaction_id=transaction_id,
            limit=limit,
            charging_rate_unit=charging_rate_unit,
            number_phases=number_phases,
            stack_level=stack_level,
            charging_profile_id=profile_id,
        )
        logger.info(
            "ocpp.set_charging_profile.send",
            charge_point_id=charge_point_id,
            connector_id=connector_id,
            transaction_id=transaction_id,
            limit=limit,
            charging_rate_unit=charging_rate_unit,
            charging_profile_id=profile_id,
        )
        response = await self.call(
            charge_point_id,
            "SetChargingProfile",
            payload,
            timeout_seconds=timeout_seconds,
        )
        logger.info(
            "ocpp.set_charging_profile.conf",
            charge_point_id=charge_point_id,
            connector_id=connector_id,
            transaction_id=transaction_id,
            status=response.get("status"),
            response=response,
        )
        return response

    async def remote_stop(
        self,
        charge_point_id: str,
        *,
        transaction_id: int,
        connector_id: int | None = None,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        charger = await self._chargers.get_by_charge_point_id(charge_point_id)
        if charger is None or charger.disconnected_at is not None:
            raise ChargerOfflineError(charge_point_id)
        if not get_connection_registry().is_connected(charge_point_id):
            raise ChargerOfflineError(charge_point_id)

        sessions = SessionRepository(self._db)
        active = None
        if transaction_id > 0:
            candidate = await sessions.get_by_ocpp_transaction_id(transaction_id)
            if candidate is not None and candidate.charger_id == charger.id:
                active = candidate

        if active is None and connector_id is not None:
            active = await sessions.get_active_by_charger_connector(charger.id, connector_id)
        elif active is None:
            active_list = await sessions.list_active_by_charger(charger.id)
            if len(active_list) > 1:
                raise AmbiguousActiveSessionError(charge_point_id, len(active_list))
            if len(active_list) == 1:
                active = active_list[0]

        pending_connector_id: int | None = None
        if active is None:
            pending_connector_id = await self._find_pending_connector(
                charge_point_id,
                charger.id,
                transaction_id=transaction_id,
                connector_id=connector_id,
            )

        if active is None and pending_connector_id is None:
            if connector_id is not None:
                active = await sessions.latest_with_ocpp_transaction_id(
                    charger.id, connector_id=connector_id
                )
            else:
                active = await sessions.latest_with_ocpp_transaction_id(charger.id)
            if active is not None and active.ocpp_transaction_id is None:
                active = None
            if active is None:
                raise NoActiveSessionError(charge_point_id, connector_id=connector_id)

        resolved_connector_id = (
            active.connector_id if active is not None else pending_connector_id
        )
        assert resolved_connector_id is not None

        station_tx_id: int | None = None
        if active is not None and active.ocpp_transaction_id is not None:
            station_tx_id = int(active.ocpp_transaction_id)
        elif transaction_id > 0:
            station_tx_id = int(transaction_id)

        live_tx_id = (
            int(transaction_id)
            if transaction_id > 0
            else int(station_tx_id or 0)
        )
        if live_tx_id <= 0 and active is not None and active.ocpp_transaction_id is not None:
            live_tx_id = int(active.ocpp_transaction_id)
        if live_tx_id <= 0:
            raise NoActiveSessionError(charge_point_id, connector_id=resolved_connector_id)

        response: dict[str, Any]
        try:
            if station_tx_id is not None:
                response = await self.call(
                    charge_point_id,
                    "RemoteStopTransaction",
                    {"transactionId": station_tx_id},
                    timeout_seconds=timeout_seconds,
                )
            else:
                response = {"status": "Accepted"}
        except Exception as exc:
            response = {"status": "Error", "message": str(exc)}

        await SessionService(self._db).finalize_after_remote_stop(
            charge_point_id=charge_point_id,
            connector_id=resolved_connector_id,
            ocpp_transaction_id=live_tx_id,
            session=active,
        )
        return response

    async def _find_pending_connector(
        self,
        charge_point_id: str,
        charger_id: int,
        *,
        transaction_id: int,
        connector_id: int | None,
    ) -> int | None:
        state = get_connection_state()
        if connector_id is not None:
            pending = await state.peek_pending_remote_start(charge_point_id, connector_id)
            if pending is None:
                return None
            if transaction_id > 0 and pending.transaction_id != transaction_id:
                return None
            return connector_id

        if transaction_id <= 0:
            return None

        rows = await ConnectorStatusRepository(self._db).list_by_charger(charger_id)
        for row in rows:
            if row.connector_id == 0:
                continue
            pending = await state.peek_pending_remote_start(
                charge_point_id, row.connector_id
            )
            if pending is not None and pending.transaction_id == transaction_id:
                return int(row.connector_id)
        return None


def build_set_charging_profile_payload(
    *,
    connector_id: int,
    transaction_id: int,
    limit: float,
    charging_rate_unit: Literal["W", "A"] = "W",
    number_phases: int | None = 3,
    stack_level: int = 0,
    charging_profile_id: int = 1,
    charging_profile_kind: Literal["Absolute", "Relative"] = "Relative",
    start_schedule: str | None = None,
) -> dict[str, Any]:
    from datetime import UTC, datetime

    period: dict[str, Any] = {
        "startPeriod": 0,
        "limit": float(limit),
    }
    if number_phases is not None:
        period["numberPhases"] = int(number_phases)

    schedule: dict[str, Any] = {
        "chargingRateUnit": charging_rate_unit,
        "chargingSchedulePeriod": [period],
    }
    # Absolute schedules need an anchor time; many CPs ignore Absolute without it.
    if charging_profile_kind == "Absolute":
        schedule["startSchedule"] = start_schedule or datetime.now(UTC).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    return {
        "connectorId": int(connector_id),
        "csChargingProfiles": {
            "chargingProfileId": int(charging_profile_id),
            "transactionId": int(transaction_id),
            "stackLevel": int(stack_level),
            "chargingProfilePurpose": "TxProfile",
            "chargingProfileKind": charging_profile_kind,
            "chargingSchedule": schedule,
        },
    }


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
