from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import select

from db.models import Charger, ChargingSession
from services.charger_service import ChargerService
from state.connection_registry import get_connection_registry

from .ws_client import SimulatedChargePoint

pytestmark = pytest.mark.integration


async def _wait_disconnected(db_session, charge_point_id: str, *, timeout: float = 5.0) -> Charger:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        row = await ChargerService(db_session).get(charge_point_id)
        assert row is not None
        await db_session.refresh(row)
        if row.disconnected_at is not None:
            return row
        if loop.time() > deadline:
            raise TimeoutError(f"{charge_point_id} disconnected_at not set")
        await asyncio.sleep(0.05)


async def _wait_connected(db_session, charge_point_id: str, *, timeout: float = 5.0) -> Charger:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        row = await ChargerService(db_session).get(charge_point_id)
        assert row is not None
        await db_session.refresh(row)
        if row.disconnected_at is None:
            return row
        if loop.time() > deadline:
            raise TimeoutError(f"{charge_point_id} still disconnected")
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_abrupt_disconnect_mid_transaction(ocpp_ws_server, db_session, connect_cp) -> None:
    cp = await connect_cp("INT_CP_DISC")
    assert (await cp.boot())["status"] == "Accepted"
    await cp.status(1, "Available")
    start = await cp.start_transaction(id_tag="TAG-DISC", meter_start=50)
    tx_id = int(start["transactionId"])
    assert await cp.meter_values(tx_id, 60) == {}

    await cp.ws.close()

    await _wait_disconnected(db_session, "INT_CP_DISC")
    assert get_connection_registry().is_connected("INT_CP_DISC") is False

    session = (
        await db_session.execute(
            select(ChargingSession).where(ChargingSession.ocpp_transaction_id == tx_id)
        )
    ).scalar_one()
    await db_session.refresh(session)
    assert session.status == "Active"
    assert session.meter_stop is None


@pytest.mark.asyncio
async def test_reconnect_same_id_clears_zombie(ocpp_ws_server, db_session) -> None:
    cp_id = "INT_CP_RE"
    first = await SimulatedChargePoint.connect(ocpp_ws_server, cp_id)
    assert (await first.boot(vendor="Re", model="One"))["status"] == "Accepted"
    assert get_connection_registry().is_connected(cp_id) is True
    first_ws = get_connection_registry().get(cp_id)

    await first.close()
    await _wait_disconnected(db_session, cp_id)
    assert get_connection_registry().is_connected(cp_id) is False

    second = await SimulatedChargePoint.connect(ocpp_ws_server, cp_id)
    try:
        boot = await second.boot(vendor="Re", model="Two")
        assert boot["status"] == "Accepted"
        await _wait_connected(db_session, cp_id)

        assert get_connection_registry().is_connected(cp_id) is True
        second_ws = get_connection_registry().get(cp_id)
        assert second_ws is not None
        assert second_ws is not first_ws

        charger = (
            await db_session.execute(select(Charger).where(Charger.charge_point_id == cp_id))
        ).scalar_one()
        await db_session.refresh(charger)
        assert charger.model == "Two"
        assert charger.disconnected_at is None
    finally:
        await second.close()


@pytest.mark.asyncio
async def test_overlapping_reconnect_keeps_online(ocpp_ws_server, db_session) -> None:
    cp_id = "INT_CP_OVERLAP"
    first = await SimulatedChargePoint.connect(ocpp_ws_server, cp_id)
    assert (await first.boot(vendor="Re", model="One"))["status"] == "Accepted"

    second = await SimulatedChargePoint.connect(ocpp_ws_server, cp_id)
    try:
        assert (await second.boot(vendor="Re", model="Two"))["status"] == "Accepted"
        await asyncio.sleep(0.2)
        charger = await _wait_connected(db_session, cp_id)
        assert charger.disconnected_at is None
        assert get_connection_registry().is_connected(cp_id) is True
    finally:
        await second.close()
