import structlog
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import ChargingSession, MeterValue
from db.time import parse_ocpp_time, seconds_ago, utc_now
from repositories.charger_repository import ChargerRepository
from repositories.session_repository import SessionRepository
from services.errors import UnknownChargerError
from services.ops_alerts import emit_ops_alert
from services.status import normalize_meter_sample

logger = structlog.get_logger(__name__)

END_REASON_STATION_STOP = "station_stop"
END_REASON_CONNECTION_TIMEOUT = "connection_timeout"


class SessionService:
    def __init__(self, db: AsyncSession) -> None:
        self._sessions = SessionRepository(db)
        self._chargers = ChargerRepository(db)
        self._db = db

    async def start_transaction(
        self,
        *,
        charge_point_id: str,
        connector_id: int,
        id_tag: str,
        meter_start: int,
        timestamp: str | None,
    ) -> ChargingSession:
        try:
            charger = await self._chargers.get_by_charge_point_id(charge_point_id)
            if charger is None:
                raise UnknownChargerError(charge_point_id)

            existing = await self._sessions.get_active_by_charger_connector(
                charger.id, connector_id
            )
            if existing is not None:
                return existing

            charger_id = charger.id
            try:
                row = await self._sessions.create(
                    charger_id=charger_id,
                    connector_id=connector_id,
                    id_tag=id_tag,
                    started_at=parse_ocpp_time(timestamp),
                    meter_start=int(meter_start),
                )
                await self._chargers.set_status(charge_point_id, "Charging", now=utc_now())
                await self._db.commit()
                await self._db.refresh(row)
                return row
            except IntegrityError:
                await self._db.rollback()
                raced = await self._sessions.get_active_by_charger_connector(
                    charger_id, connector_id
                )
                if raced is not None:
                    return raced
                raise
        except Exception:
            await self._db.rollback()
            raise

    async def stop_transaction(
        self,
        *,
        charge_point_id: str,
        transaction_id: int,
        meter_stop: int | None,
        timestamp: str | None,
        transaction_data: list[dict] | None = None,
        connector_id: int | None = None,
    ) -> ChargingSession | None:
        try:
            row = await self._sessions.get_by_ocpp_transaction_id(transaction_id)
            if row is None:
                logger.warning(
                    "ocpp.stop_unknown_transaction_id",
                    charge_point_id=charge_point_id,
                    connector_id=connector_id,
                    transaction_id=transaction_id,
                )
                emit_ops_alert(
                    "ocpp.stop_unknown_transaction_id",
                    charge_point_id=charge_point_id,
                    connector_id=connector_id,
                    transaction_id=transaction_id,
                )
                return None

            if transaction_data:
                await self._persist_meter_entries(row.id, transaction_data)

            await self._sessions.stop(
                row,
                stopped_at=parse_ocpp_time(timestamp),
                meter_stop=None if meter_stop is None else int(meter_stop),
                end_reason=END_REASON_STATION_STOP,
                meter_stop_estimated=False,
            )
            await self._chargers.set_status(charge_point_id, "Available", now=utc_now())
            await self._db.commit()
            await self._db.refresh(row)
            return row
        except Exception:
            await self._db.rollback()
            raise

    async def close_offline_timed_out_sessions(self, grace_period_seconds: int) -> int:
        try:
            cutoff = seconds_ago(grace_period_seconds)
            chargers = await self._chargers.list_disconnected_before(before=cutoff)
            closed_total = 0
            now = utc_now()
            for charger in chargers:
                active = await self._sessions.list_active_by_charger(charger.id)
                if not active:
                    continue
                session_ids: list[int] = []
                for session in active:
                    latest = await self._sessions.latest_meter_value(session.id)
                    if latest is not None:
                        meter_stop = int(round(latest.value))
                    else:
                        meter_stop = int(session.meter_start)
                    await self._sessions.stop(
                        session,
                        stopped_at=now,
                        meter_stop=meter_stop,
                        end_reason=END_REASON_CONNECTION_TIMEOUT,
                        meter_stop_estimated=True,
                    )
                    session_ids.append(session.id)
                    closed_total += 1
                logger.warning(
                    "ocpp.offline_session_auto_closed",
                    charger_id=charger.id,
                    charge_point_id=charger.charge_point_id,
                    session_ids=session_ids,
                    closed_count=len(session_ids),
                )
            await self._db.commit()
            return closed_total
        except Exception:
            await self._db.rollback()
            raise

    async def record_meter_values(
        self,
        *,
        charge_point_id: str,
        connector_id: int,
        transaction_id: int | None,
        meter_value: list[dict],
    ) -> list[MeterValue]:
        try:
            charger = await self._chargers.get_by_charge_point_id(charge_point_id)
            if charger is None:
                logger.warning(
                    "ocpp.meter_values_without_active_session",
                    charge_point_id=charge_point_id,
                    connector_id=connector_id,
                    transaction_id=transaction_id,
                    meter_value=meter_value,
                )
                return []

            charging: ChargingSession | None = None
            if transaction_id is not None:
                charging = await self._sessions.get_by_ocpp_transaction_id(transaction_id)
            if charging is None:
                charging = await self._sessions.get_active_by_charger_connector(
                    charger.id, connector_id
                )
            if charging is None or charging.status != "Active":
                logger.warning(
                    "ocpp.meter_values_without_active_session",
                    charge_point_id=charge_point_id,
                    connector_id=connector_id,
                    transaction_id=transaction_id,
                    meter_value=meter_value,
                )
                return []

            created = await self._persist_meter_entries(charging.id, meter_value)
            await self._chargers.set_status(charge_point_id, "Charging", now=utc_now())
            await self._db.commit()
            return created
        except Exception:
            await self._db.rollback()
            raise

    async def _persist_meter_entries(
        self,
        session_id: int,
        meter_value: list[dict],
    ) -> list[MeterValue]:
        created: list[MeterValue] = []
        for entry in meter_value:
            ts = parse_ocpp_time(entry.get("timestamp"))
            samples = entry.get("sampled_value") or entry.get("sampledValue") or []
            for sampled in samples:
                raw = sampled.get("value")
                if raw is None:
                    continue
                try:
                    value = float(raw)
                except (TypeError, ValueError):
                    continue
                value, unit = normalize_meter_sample(value, sampled.get("unit"))
                measurand = sampled.get("measurand") or "Energy.Active.Import.Register"
                created.append(
                    await self._sessions.add_meter_value(
                        session_id=session_id,
                        timestamp=ts,
                        value=value,
                        unit=unit,
                        measurand=measurand,
                    )
                )
        return created
