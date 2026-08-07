from __future__ import annotations

import pytest

import db as db_module
from services.live_status_service import LiveStatusService

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_meter_values_populate_soc_and_charging_speed(
    ocpp_ws_server, db_engine, connect_cp
) -> None:
    cp = await connect_cp("INT_CP_METRICS")
    assert (await cp.boot())["status"] == "Accepted"
    await cp.status(1, "Available")

    start = await cp.start_transaction(id_tag="TAG-METRICS", meter_start=1000)
    tx_id = int(start["transactionId"])

    assert (
        await cp.meter_values(
            tx_id,
            2500,
            extra_samples=[
                {"value": "64", "measurand": "SoC", "unit": "Percent"},
                {"value": "7.2", "measurand": "Power.Active.Import", "unit": "kW"},
            ],
        )
        == {}
    )

    async with db_module.async_session_factory() as db:
        payload = await LiveStatusService(db).build_timed_live_payload()

    match = next(
        connector
        for charger in payload["chargers"]
        if charger["charger_id"] == "INT_CP_METRICS"
        for connector in charger["connectors"]
        if connector["number"] == 1
    )
    assert match["status"] == "Charging"
    assert match["transaction_id"] == tx_id
    assert match["battery"] == 64.0
    assert match["charging_speed_kw"] == 7.2
    assert match["total_energy_delivered_kwh"] == 1.5
