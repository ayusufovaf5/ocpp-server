from __future__ import annotations

import pytest
from sqlalchemy import select

import db as db_module
from db.models import ChargingSession

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_concurrent_charge_points_isolated(
    ocpp_ws_server, db_engine, connect_cp
) -> None:
    cp_a = await connect_cp("INT_CP_A")
    cp_b = await connect_cp("INT_CP_B")

    assert (await cp_a.boot(vendor="AVendor", model="AModel"))["status"] == "Accepted"
    assert (await cp_b.boot(vendor="BVendor", model="BModel"))["status"] == "Accepted"
    await cp_a.status(1, "Available")
    await cp_b.status(1, "Available")

    start_a = await cp_a.start_transaction(id_tag="TAG-A", meter_start=10)
    start_b = await cp_b.start_transaction(id_tag="TAG-B", meter_start=20)
    tx_a = int(start_a["transactionId"])
    tx_b = int(start_b["transactionId"])
    assert tx_a != tx_b

    assert await cp_a.meter_values(tx_a, 11) == {}
    assert await cp_b.meter_values(tx_b, 22) == {}

    async with db_module.async_session_factory() as db:
        active = (
            (await db.execute(select(ChargingSession).where(ChargingSession.status == "Active")))
            .scalars()
            .all()
        )
        assert len(active) == 2
        by_tag = {s.id_tag: s for s in active}
        assert by_tag["TAG-A"].meter_start == 10
        assert by_tag["TAG-A"].ocpp_transaction_id == tx_a
        assert by_tag["TAG-B"].meter_start == 20
        assert by_tag["TAG-B"].ocpp_transaction_id == tx_b

    await cp_a.stop_transaction(tx_a, meter_stop=15)
    async with db_module.async_session_factory() as db:
        still_active = (
            (await db.execute(select(ChargingSession).where(ChargingSession.status == "Active")))
            .scalars()
            .all()
        )
        assert len(still_active) == 1
        assert still_active[0].id_tag == "TAG-B"
        assert still_active[0].ocpp_transaction_id == tx_b

    await cp_b.stop_transaction(tx_b, meter_stop=25)
