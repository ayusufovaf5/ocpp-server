from __future__ import annotations

import asyncio

import pytest
from httpx import AsyncClient
from sqlalchemy import select

import db as db_module
from db.models import ChargingSession

pytestmark = pytest.mark.integration


def _http_base(ws_url: str) -> str:
    return ws_url.replace("ws://", "http://", 1)


@pytest.mark.asyncio
async def test_remote_start_uses_evpoint_transaction_id(
    ocpp_ws_server, db_engine, connect_cp
) -> None:
    evpoint_tx = 424242
    cp = await connect_cp("INT_CP_EVPOINT_TX")
    assert (await cp.boot())["status"] == "Accepted"
    await cp.status(1, "Available")

    async def accept_remote_start() -> dict:
        unique_id, payload = await cp.expect_call("RemoteStartTransaction")
        assert payload == {"connectorId": 1, "idTag": "USER-QR"}
        await cp.send_result(unique_id, {"status": "Accepted"})
        return payload

    accept_task = asyncio.create_task(accept_remote_start())
    async with AsyncClient(base_url=_http_base(ocpp_ws_server), timeout=5.0) as client:
        response = await client.post(
            "/start/INT_CP_EVPOINT_TX",
            json={
                "connector_id": 1,
                "id_tag": "USER-QR",
                "transaction_id": evpoint_tx,
            },
        )
    assert response.status_code == 200
    assert response.json()["response"]["status"] == "Accepted"
    await asyncio.wait_for(accept_task, timeout=5.0)

    start = await cp.start_transaction(id_tag="USER-QR", meter_start=100)
    assert start["idTagInfo"]["status"] == "Accepted"
    assert int(start["transactionId"]) == evpoint_tx

    async def accept_remote_stop() -> dict:
        unique_id, payload = await cp.expect_call("RemoteStopTransaction")
        assert payload == {"transactionId": evpoint_tx}
        await cp.send_result(unique_id, {"status": "Accepted"})
        return payload

    stop_task = asyncio.create_task(accept_remote_stop())
    async with AsyncClient(base_url=_http_base(ocpp_ws_server), timeout=5.0) as client:
        response = await client.post(
            "/stop/INT_CP_EVPOINT_TX",
            json={"transaction_id": evpoint_tx, "connector_id": 1},
        )
    assert response.status_code == 200
    assert response.json()["response"]["status"] == "Accepted"
    await asyncio.wait_for(stop_task, timeout=5.0)

    await cp.stop_transaction(evpoint_tx, meter_stop=180, reason="Remote")

    async with db_module.async_session_factory() as db:
        session = (
            await db.execute(
                select(ChargingSession).where(ChargingSession.ocpp_transaction_id == evpoint_tx)
            )
        ).scalar_one()
        assert session.id_tag == "USER-QR"
        assert session.status == "Completed"
        assert session.meter_start == 100
        assert session.meter_stop == 180
