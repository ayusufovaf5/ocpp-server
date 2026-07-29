from __future__ import annotations

import pytest
from sqlalchemy import select

import db as db_module
from config import get_settings
from db.models import Charger, ChargingSession, MeterValue

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_full_happy_path_session(ocpp_ws_server, db_engine, connect_cp) -> None:
    interval = get_settings().ocpp_heartbeat_interval
    cp = await connect_cp("INT_CP_FULL")

    boot = await cp.boot(vendor="EvPoint", model="IntegrationSim", firmware_version="9.9.9")
    assert boot["status"] == "Accepted"
    assert boot["interval"] == interval
    assert "currentTime" in boot

    assert await cp.status(1, "Available") == {}
    auth = await cp.authorize("TAG-FULL")
    assert auth["idTagInfo"]["status"] == "Accepted"

    start = await cp.start_transaction(id_tag="TAG-FULL", meter_start=1000)
    assert start["idTagInfo"]["status"] == "Accepted"
    tx_id = int(start["transactionId"])
    assert tx_id > 0

    for energy in (1500, 2000, 2300):
        assert await cp.meter_values(tx_id, energy) == {}

    stop = await cp.stop_transaction(tx_id, meter_stop=2500, reason="Local")
    assert stop == {} or stop.get("idTagInfo") is None

    await cp.close()

    async with db_module.async_session_factory() as db:
        charger = (
            await db.execute(select(Charger).where(Charger.charge_point_id == "INT_CP_FULL"))
        ).scalar_one()
        assert charger.vendor == "EvPoint"
        assert charger.model == "IntegrationSim"
        assert charger.firmware_version == "9.9.9"

        session = (
            await db.execute(
                select(ChargingSession).where(ChargingSession.ocpp_transaction_id == tx_id)
            )
        ).scalar_one()
        assert session.status == "Completed"
        assert session.id_tag == "TAG-FULL"
        assert session.meter_start == 1000
        assert session.meter_stop == 2500
        assert session.stopped_at is not None
        assert session.effective_end_reason == "station_stop"

        meters = (
            (await db.execute(select(MeterValue).where(MeterValue.session_id == session.id)))
            .scalars()
            .all()
        )
        assert sorted(m.value for m in meters) == [1500.0, 2000.0, 2300.0]
