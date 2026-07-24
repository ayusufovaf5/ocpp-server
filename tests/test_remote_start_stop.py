from __future__ import annotations

import asyncio
import json

import pytest
import uvicorn
import websockets
from httpx import AsyncClient

from config import DEV_API_KEY
from db.time import utc_now_iso
from ocpp16.app import create_ocpp_app
from ocpp16.protocol import MessageType
from services.charger_service import ChargerService
from services.session_service import SessionService
from state.connection_registry import ConnectionRegistry, set_connection_registry


async def _wait_port(host: str, port: int, timeout: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        try:
            reader, writer = await asyncio.open_connection(host, port)
            writer.close()
            await writer.wait_closed()
            return
        except OSError:
            if loop.time() > deadline:
                raise TimeoutError(f"Server not ready on {host}:{port}") from None
            await asyncio.sleep(0.05)


@pytest.fixture
async def ocpp_http_server(db_engine, unused_tcp_port):
    set_connection_registry(ConnectionRegistry())
    app = create_ocpp_app()
    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=unused_tcp_port,
        log_level="error",
        lifespan="on",
    )
    server = uvicorn.Server(config)
    server.install_signal_handlers = False
    task = asyncio.create_task(server.serve())
    await _wait_port("127.0.0.1", unused_tcp_port)
    yield {
        "ws": f"ws://127.0.0.1:{unused_tcp_port}",
        "http": f"http://127.0.0.1:{unused_tcp_port}",
    }
    server.should_exit = True
    await task
    set_connection_registry(None)


async def _boot(ws, charge_point_id: str) -> None:
    await ws.send(
        json.dumps(
            [
                2,
                "boot",
                "BootNotification",
                {"chargePointVendor": "V", "chargePointModel": "M"},
            ]
        )
    )
    assert json.loads(await asyncio.wait_for(ws.recv(), timeout=5))[0] == 3


@pytest.mark.asyncio
async def test_remote_start_happy_path_via_rest_and_ws(ocpp_http_server, db_session) -> None:
    charge_point_id = "CP_RSTART"
    await ChargerService(db_session).register_boot(
        charge_point_id=charge_point_id, vendor="V", model="M"
    )

    async with websockets.connect(
        f"{ocpp_http_server['ws']}/ocpp/{charge_point_id}",
        subprotocols=["ocpp1.6"],
    ) as ws:
        await _boot(ws, charge_point_id)

        async def station_loop() -> None:
            try:
                while True:
                    frame = json.loads(await ws.recv())
                    if frame[0] == MessageType.CALL and frame[2] == "RemoteStartTransaction":
                        assert frame[3] == {"connectorId": 1, "idTag": "USER_TAG"}
                        await ws.send(
                            json.dumps([MessageType.CALLRESULT, frame[1], {"status": "Accepted"}])
                        )
                        return
            except websockets.ConnectionClosed:
                return

        reply_task = asyncio.create_task(station_loop())
        async with AsyncClient(
            base_url=ocpp_http_server["http"],
            headers={"X-API-Key": DEV_API_KEY},
        ) as client:
            response = await client.post(
                f"/start/{charge_point_id}",
                json={
                    "connector_id": 1,
                    "id_tag": "USER_TAG",
                    "transaction_id": 999001,
                },
            )
        assert response.status_code == 200
        assert response.json() == {
            "status": "success",
            "response": {"status": "Accepted"},
        }
        if not reply_task.done():
            reply_task.cancel()
            try:
                await reply_task
            except asyncio.CancelledError:
                pass
        else:
            await reply_task


@pytest.mark.asyncio
async def test_remote_start_offline_returns_404(ocpp_http_server, db_session) -> None:
    charge_point_id = "CP_RSTART_OFF"
    await ChargerService(db_session).register_boot(
        charge_point_id=charge_point_id, vendor="V", model="M"
    )

    async with AsyncClient(
        base_url=ocpp_http_server["http"],
        headers={"X-API-Key": DEV_API_KEY},
    ) as client:
        response = await client.post(
            f"/start/{charge_point_id}",
            json={
                "connector_id": 1,
                "id_tag": "TAG",
                "transaction_id": 1,
            },
        )
    assert response.status_code == 404
    assert response.json()["detail"] == {
        "status": "error",
        "response": "Charger not found",
    }


@pytest.mark.asyncio
async def test_remote_start_unknown_charger_returns_404(ocpp_http_server) -> None:
    async with AsyncClient(
        base_url=ocpp_http_server["http"],
        headers={"X-API-Key": DEV_API_KEY},
    ) as client:
        response = await client.post(
            "/start/CP_UNKNOWN",
            json={
                "connector_id": 1,
                "id_tag": "TAG",
                "transaction_id": 1,
            },
        )
    assert response.status_code == 404
    assert response.json()["detail"]["response"] == "Charger not found"


@pytest.mark.asyncio
async def test_remote_stop_without_active_session_does_not_call_station(
    ocpp_http_server, db_session
) -> None:
    charge_point_id = "CP_RSTOP_NONE"
    await ChargerService(db_session).register_boot(
        charge_point_id=charge_point_id, vendor="V", model="M"
    )
    outbound: list[str] = []

    async with websockets.connect(
        f"{ocpp_http_server['ws']}/ocpp/{charge_point_id}",
        subprotocols=["ocpp1.6"],
    ) as ws:
        await _boot(ws, charge_point_id)

        async def station_loop() -> None:
            try:
                while True:
                    frame = json.loads(await ws.recv())
                    if frame[0] == MessageType.CALL:
                        outbound.append(frame[2])
                        await ws.send(
                            json.dumps([MessageType.CALLRESULT, frame[1], {"status": "Accepted"}])
                        )
            except websockets.ConnectionClosed:
                return

        reply_task = asyncio.create_task(station_loop())
        async with AsyncClient(
            base_url=ocpp_http_server["http"],
            headers={"X-API-Key": DEV_API_KEY},
        ) as client:
            response = await client.post(
                f"/stop/{charge_point_id}",
                json={"transaction_id": 42},
            )
        assert response.status_code == 404
        assert response.json()["detail"] == {
            "status": "error",
            "response": "No active session",
        }
        await asyncio.sleep(0.2)
        assert outbound == []
        reply_task.cancel()
        try:
            await reply_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_remote_stop_uses_ocpp_transaction_id_from_db(ocpp_http_server, db_session) -> None:
    charge_point_id = "CP_RSTOP_OK"
    await ChargerService(db_session).register_boot(
        charge_point_id=charge_point_id, vendor="V", model="M"
    )
    session = await SessionService(db_session).start_transaction(
        charge_point_id=charge_point_id,
        connector_id=1,
        id_tag="TAG",
        meter_start=0,
        timestamp=utc_now_iso(),
    )
    assert session.ocpp_transaction_id is not None
    app_charging_id = 777777

    async with websockets.connect(
        f"{ocpp_http_server['ws']}/ocpp/{charge_point_id}",
        subprotocols=["ocpp1.6"],
    ) as ws:
        await _boot(ws, charge_point_id)

        async def station_loop() -> None:
            try:
                while True:
                    frame = json.loads(await ws.recv())
                    if frame[0] == MessageType.CALL and frame[2] == "RemoteStopTransaction":
                        assert frame[3] == {"transactionId": session.ocpp_transaction_id}
                        assert frame[3]["transactionId"] != app_charging_id
                        await ws.send(
                            json.dumps([MessageType.CALLRESULT, frame[1], {"status": "Accepted"}])
                        )
                        return
            except websockets.ConnectionClosed:
                return

        reply_task = asyncio.create_task(station_loop())
        async with AsyncClient(
            base_url=ocpp_http_server["http"],
            headers={"X-API-Key": DEV_API_KEY},
        ) as client:
            response = await client.post(
                f"/stop/{charge_point_id}",
                json={"transaction_id": app_charging_id},
            )
        assert response.status_code == 200
        assert response.json() == {
            "status": "success",
            "response": {"status": "Accepted"},
        }
        if not reply_task.done():
            reply_task.cancel()
            try:
                await reply_task
            except asyncio.CancelledError:
                pass
        else:
            await reply_task
