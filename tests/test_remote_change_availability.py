from __future__ import annotations

import asyncio
import json

import pytest
import uvicorn
import websockets
from httpx import AsyncClient

from config import DEV_API_KEY
from ocpp16.app import create_ocpp_app
from ocpp16.protocol import MessageType
from services.charger_service import ChargerService
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


@pytest.mark.asyncio
async def test_change_availability_operative_happy_path_via_rest_and_ws(
    ocpp_http_server, db_session
) -> None:
    charge_point_id = "CP_CA_OK"
    await ChargerService(db_session).register_boot(
        charge_point_id=charge_point_id, vendor="V", model="M"
    )

    async with websockets.connect(
        f"{ocpp_http_server['ws']}/ocpp/{charge_point_id}",
        subprotocols=["ocpp1.6"],
    ) as ws:
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

        async def station_loop() -> None:
            try:
                while True:
                    frame = json.loads(await ws.recv())
                    if frame[0] == MessageType.CALL and frame[2] == "ChangeAvailability":
                        assert frame[3] == {"connectorId": 1, "type": "Operative"}
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
                f"/change-availability/{charge_point_id}",
                json={"is_available": True, "connector_id": 1},
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
