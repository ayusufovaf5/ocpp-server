import structlog
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import ChargingSession, MeterValue
from db.time import parse_ocpp_time, utc_now
from repositories.charger_repository import ChargerRepository
from repositories.session_repository import SessionRepository
from services.errors import UnknownChargerError
from services.status import normalize_meter_sample

logger = structlog.get_logger(__name__)


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
    ) -> ChargingSession | None:
        try:
            row = await self._sessions.get_by_ocpp_transaction_id(transaction_id)
            if row is None:
                logger.warning(
                    "ocpp.stop_unknown_transaction_id",
                    charge_point_id=charge_point_id,
                    transaction_id=transaction_id,
                )
                charger = await self._chargers.get_by_charge_point_id(charge_point_id)
                if charger is None:
                    return None
                row = await self._sessions.get_any_active_by_charger(charger.id)
                if row is None:
                    return None

            await self._sessions.stop(
                row,
                stopped_at=parse_ocpp_time(timestamp),
                meter_stop=None if meter_stop is None else int(meter_stop),
            )
            await self._chargers.set_status(charge_point_id, "Available", now=utc_now())
            await self._db.commit()
            await self._db.refresh(row)
            return row
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
                            session_id=charging.id,
                            timestamp=ts,
                            value=value,
                            unit=unit,
                            measurand=measurand,
                        )
                    )

            await self._chargers.set_status(charge_point_id, "Charging", now=utc_now())
            await self._db.commit()
            return created
        except Exception:
            await self._db.rollback()
            raise
