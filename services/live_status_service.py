from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from db.time import utc_now
from events.evpoint_payload import normalize_status
from repositories.charger_repository import ChargerRepository
from repositories.connector_status_repository import ConnectorStatusRepository
from repositories.session_repository import SessionRepository
from state.connection_state import get_connection_state
from services.status import (
    ENERGY_IMPORT_MEASURANDS,
    POWER_IMPORT_MEASURANDS,
    SOC_MEASURANDS,
    pick_latest_meter,
    power_watts_to_kw,
)


_IN_PROGRESS_STATUSES = {
    "Preparing",
    "Charging",
    "SuspendedEv",
    "SuspendedEV",
    "SuspendedEVSE",
    "Finishing",
}


def _duration_seconds(started_at: datetime | None, *, now: datetime) -> float:
    if started_at is None:
        return 0
    start = started_at if started_at.tzinfo is not None else started_at.replace(tzinfo=UTC)
    return max(0.0, (now - start).total_seconds())


def _energy_kwh(*, meter_start: int, latest_wh: float | None) -> float | None:
    if latest_wh is None:
        return None
    return round(max(0.0, (latest_wh - meter_start) / 1000.0), 2)


def _connector_payload(
    *,
    connector_id: int,
    status: str,
    session: Any | None,
    latest_wh: float | None,
    battery: float | None,
    charging_speed_kw: float | None,
    now: datetime,
    transaction_id: int | None,
) -> dict[str, Any]:
    normalized = normalize_status(status)
    if session is not None:
        normalized = "Charging"
    elif normalized in _IN_PROGRESS_STATUSES and transaction_id is None:
        # Never advertise in-progress without a transaction id EvPoint can bind to.
        normalized = "Available"

    return {
        "number": connector_id,
        "status": normalized,
        "charging_speed_kw": charging_speed_kw if session is not None else None,
        "total_energy_delivered_kwh": (
            None
            if session is None
            else _energy_kwh(meter_start=session.meter_start, latest_wh=latest_wh)
        ),
        "duration_seconds": (
            0 if session is None else _duration_seconds(session.started_at, now=now)
        ),
        "transaction_id": transaction_id,
        "battery": battery if session is not None else None,
    }


class LiveStatusService:
    def __init__(self, db: AsyncSession) -> None:
        self._db = db
        self._chargers = ChargerRepository(db)
        self._connectors = ConnectorStatusRepository(db)
        self._sessions = SessionRepository(db)

    async def _session_live_metrics(
        self, session: Any
    ) -> tuple[float | None, float | None, float | None]:
        meters = await self._sessions.list_meter_values_desc(session.id)
        energy = pick_latest_meter(meters, ENERGY_IMPORT_MEASURANDS)
        soc = pick_latest_meter(meters, SOC_MEASURANDS)
        power = pick_latest_meter(meters, POWER_IMPORT_MEASURANDS)
        latest_wh = None if energy is None else float(energy.value)
        battery = None if soc is None else float(soc.value)
        speed = (
            None
            if power is None
            else power_watts_to_kw(float(power.value), power.unit)
        )
        return latest_wh, battery, speed

    async def _resolve_transaction_id(
        self,
        *,
        charge_point_id: str,
        connector_id: int,
        status: str,
        session: Any | None,
        now: datetime,
    ) -> int | None:
        if session is not None and session.ocpp_transaction_id is not None:
            return int(session.ocpp_transaction_id)

        connection_state = get_connection_state()
        normalized = normalize_status(status)

        # RemoteStart stores EvPoint charging id before StartTransaction arrives.
        if normalized in _IN_PROGRESS_STATUSES:
            pending = await connection_state.peek_pending_remote_start(
                charge_point_id, connector_id
            )
            if pending is not None and pending.transaction_id > 0:
                return int(pending.transaction_id)

        stopped = await connection_state.peek_stopped_ocpp_transaction_for_live(
            charge_point_id,
            connector_id,
            now=now,
        )
        if stopped is not None:
            return int(stopped)

        return None

    async def build_timed_live_payload(self, *, now: datetime | None = None) -> dict[str, Any]:
        when = now or utc_now()
        chargers = await self._chargers.list_all()
        active_sessions = await self._sessions.list_all_active()
        active_by_charger: dict[int, dict[int, Any]] = {}
        for session in active_sessions:
            active_by_charger.setdefault(session.charger_id, {})[session.connector_id] = session

        chargers_out: list[dict[str, Any]] = []
        for charger in chargers:
            rows = await self._connectors.list_by_charger(charger.id)
            by_connector: dict[int, dict[str, Any]] = {}
            for row in rows:
                if row.connector_id == 0:
                    continue
                session = active_by_charger.get(charger.id, {}).get(row.connector_id)
                transaction_id = await self._resolve_transaction_id(
                    charge_point_id=charger.charge_point_id,
                    connector_id=row.connector_id,
                    status=row.status,
                    session=session,
                    now=when,
                )
                latest_wh = battery = speed = None
                if session is not None:
                    latest_wh, battery, speed = await self._session_live_metrics(session)
                by_connector[row.connector_id] = _connector_payload(
                    connector_id=row.connector_id,
                    status=row.status,
                    session=session,
                    latest_wh=latest_wh,
                    battery=battery,
                    charging_speed_kw=speed,
                    now=when,
                    transaction_id=transaction_id,
                )

            for connector_id, session in active_by_charger.get(charger.id, {}).items():
                if connector_id in by_connector:
                    continue
                latest_wh, battery, speed = await self._session_live_metrics(session)
                transaction_id = (
                    int(session.ocpp_transaction_id)
                    if session.ocpp_transaction_id is not None
                    else None
                )
                by_connector[connector_id] = _connector_payload(
                    connector_id=connector_id,
                    status="Charging",
                    session=session,
                    latest_wh=latest_wh,
                    battery=battery,
                    charging_speed_kw=speed,
                    now=when,
                    transaction_id=transaction_id,
                )

            chargers_out.append(
                {
                    "charger_id": charger.charge_point_id,
                    "connectors": [by_connector[k] for k in sorted(by_connector)],
                }
            )

        return {
            "update_time": when.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "chargers": chargers_out,
        }
