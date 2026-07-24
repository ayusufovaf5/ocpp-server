from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from db.time import utc_now
from events.evpoint_payload import normalize_status
from repositories.charger_repository import ChargerRepository
from repositories.connector_status_repository import ConnectorStatusRepository
from repositories.session_repository import SessionRepository


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
    now: datetime,
) -> dict[str, Any]:
    if session is not None:
        status = "Charging"
    return {
        "number": connector_id,
        "status": normalize_status(status),
        "charging_speed_kw": None,
        "total_energy_delivered_kwh": (
            None
            if session is None
            else _energy_kwh(meter_start=session.meter_start, latest_wh=latest_wh)
        ),
        "duration_seconds": (
            0 if session is None else _duration_seconds(session.started_at, now=now)
        ),
        "transaction_id": None if session is None else session.ocpp_transaction_id,
        "battery": None,
    }


class LiveStatusService:
    def __init__(self, db: AsyncSession) -> None:
        self._db = db
        self._chargers = ChargerRepository(db)
        self._connectors = ConnectorStatusRepository(db)
        self._sessions = SessionRepository(db)

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
                latest = None
                if session is not None:
                    mv = await self._sessions.latest_meter_value(session.id)
                    latest = None if mv is None else float(mv.value)
                by_connector[row.connector_id] = _connector_payload(
                    connector_id=row.connector_id,
                    status=row.status,
                    session=session,
                    latest_wh=latest,
                    now=when,
                )

            for connector_id, session in active_by_charger.get(charger.id, {}).items():
                if connector_id in by_connector:
                    continue
                mv = await self._sessions.latest_meter_value(session.id)
                latest = None if mv is None else float(mv.value)
                by_connector[connector_id] = _connector_payload(
                    connector_id=connector_id,
                    status="Charging",
                    session=session,
                    latest_wh=latest,
                    now=when,
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
