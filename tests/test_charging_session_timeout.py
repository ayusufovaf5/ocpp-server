from __future__ import annotations

from datetime import timedelta

import pytest

from db.time import utc_now, utc_now_iso
from services.charger_service import ChargerService
from services.charging_session_timeout_watcher import ChargingSessionTimeoutWatcher
from services.session_service import END_REASON_STATION_STOP, SessionService


@pytest.mark.asyncio
async def test_start_transaction_arms_timeout(db_session) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_TO_ARM", vendor="V", model="M"
    )
    active = await SessionService(db_session).start_transaction(
        charge_point_id="CP_TO_ARM",
        connector_id=1,
        id_tag="TAG",
        meter_start=10,
        timestamp=utc_now_iso(),
    )
    assert active.last_meter_at is not None


@pytest.mark.asyncio
async def test_meter_values_with_transaction_id_extends_timeout(db_session) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_TO_EXT", vendor="V", model="M"
    )
    sessions = SessionService(db_session)
    active = await sessions.start_transaction(
        charge_point_id="CP_TO_EXT",
        connector_id=1,
        id_tag="TAG",
        meter_start=10,
        timestamp=utc_now_iso(),
    )
    assert active.ocpp_transaction_id is not None
    armed_at = active.last_meter_at
    assert armed_at is not None

    active.last_meter_at = utc_now() - timedelta(minutes=20)
    await db_session.commit()

    await sessions.record_meter_values(
        charge_point_id="CP_TO_EXT",
        connector_id=1,
        transaction_id=active.ocpp_transaction_id,
        meter_value=[
            {
                "timestamp": utc_now_iso(),
                "sampledValue": [{"value": "100", "unit": "Wh"}],
            }
        ],
    )
    await db_session.refresh(active)
    assert active.last_meter_at is not None
    assert active.last_meter_at > armed_at - timedelta(minutes=20)


@pytest.mark.asyncio
async def test_meter_values_without_transaction_id_does_not_extend(db_session) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_TO_NOEXT", vendor="V", model="M"
    )
    sessions = SessionService(db_session)
    active = await sessions.start_transaction(
        charge_point_id="CP_TO_NOEXT",
        connector_id=1,
        id_tag="TAG",
        meter_start=10,
        timestamp=utc_now_iso(),
    )
    stale = utc_now() - timedelta(minutes=25)
    active.last_meter_at = stale
    await db_session.commit()

    await sessions.record_meter_values(
        charge_point_id="CP_TO_NOEXT",
        connector_id=1,
        transaction_id=None,
        meter_value=[
            {
                "timestamp": utc_now_iso(),
                "sampledValue": [{"value": "100", "unit": "Wh"}],
            }
        ],
    )
    await db_session.refresh(active)
    assert active.last_meter_at.replace(tzinfo=None) == stale.replace(tzinfo=None)


@pytest.mark.asyncio
async def test_close_expired_uses_stop_transaction_path(db_session) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_TO_CLOSE", vendor="V", model="M"
    )
    sessions = SessionService(db_session)
    active = await sessions.start_transaction(
        charge_point_id="CP_TO_CLOSE",
        connector_id=1,
        id_tag="TAG",
        meter_start=50,
        timestamp=utc_now_iso(),
    )
    assert active.ocpp_transaction_id is not None
    await sessions.record_meter_values(
        charge_point_id="CP_TO_CLOSE",
        connector_id=1,
        transaction_id=active.ocpp_transaction_id,
        meter_value=[
            {
                "timestamp": utc_now_iso(),
                "sampledValue": [{"value": "250", "unit": "Wh"}],
            }
        ],
    )
    active.last_meter_at = utc_now() - timedelta(seconds=1801)
    await db_session.commit()

    closed = await ChargingSessionTimeoutWatcher(db_session).close_expired(1800)
    assert closed == 1
    await db_session.refresh(active)
    assert active.status == "Completed"
    assert active.end_reason == END_REASON_STATION_STOP
    assert active.meter_stop == 250


@pytest.mark.asyncio
async def test_close_expired_before_timeout_keeps_session(db_session) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_TO_KEEP", vendor="V", model="M"
    )
    active = await SessionService(db_session).start_transaction(
        charge_point_id="CP_TO_KEEP",
        connector_id=1,
        id_tag="TAG",
        meter_start=10,
        timestamp=utc_now_iso(),
    )

    closed = await ChargingSessionTimeoutWatcher(db_session).close_expired(1800)
    assert closed == 0
    await db_session.refresh(active)
    assert active.status == "Active"
