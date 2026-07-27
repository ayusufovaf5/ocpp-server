from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from events.types import EventType

STATUS_TO_API = {
    "Available": "Available",
    "Unavailable": "Unavailable",
    "Preparing": "Preparing",
    "Charging": "Charging",
    "SuspendedEV": "SuspendedEv",
    "SuspendedEVSE": "SuspendedEv",
    "Finishing": "Finishing",
    "Faulted": "Faulted",
    "Not specified": "Unavailable",
}


def normalize_status(status_value: Any) -> str:
    if status_value is None:
        return "Unavailable"

    text = str(status_value)
    return STATUS_TO_API.get(text, text)


def build_payload(
    charge_point_id: str,
    charger_details: dict[str, Any],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    connectors: list[dict[str, Any]] = []

    for connector in charger_details.get("connectors", []):
        connectors.append(
            {
                "number": connector.get("number"),
                "status": normalize_status(connector.get("status")),
                "charging_speed_kw": connector.get("charging_speed_kw"),
                "total_energy_delivered_kwh": connector.get("total_energy_delivered_kwh"),
                "duration_seconds": connector.get("duration_seconds"),
                "transaction_id": connector.get("transaction_id"),
                "battery": connector.get("battery"),
            }
        )

    when = now or datetime.now(UTC)
    return {
        "update_time": when.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "chargers": [
            {
                "charger_id": charge_point_id,
                "connectors": connectors,
            }
        ],
    }


def charger_details_from_event(
    event_type: EventType,
    payload: dict[str, Any],
) -> dict[str, Any]:
    connector_id = payload.get("connector_id")
    if connector_id is None:
        return {"connectors": []}

    status = payload.get("status")
    if status is None:
        if event_type == EventType.SESSION_STARTED:
            status = "Charging"
        elif event_type == EventType.SESSION_STOPPED:
            status = "Available"
        else:
            status = "Not specified"

    transaction_id = payload.get("transaction_id")
    if transaction_id is None:
        transaction_id = payload.get("ocpp_transaction_id")

    return {
        "connectors": [
            {
                "number": connector_id,
                "status": status,
                "charging_speed_kw": payload.get("charging_speed_kw"),
                "total_energy_delivered_kwh": payload.get("total_energy_delivered_kwh"),
                "duration_seconds": payload.get("duration_seconds", 0),
                "transaction_id": transaction_id,
                "battery": payload.get("battery"),
            }
        ]
    }
