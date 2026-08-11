from __future__ import annotations

import asyncio
import json

import pytest
import uvicorn
import websockets
from httpx import AsyncClient

from db.time import utc_now_iso
from events.publisher import STREAM_NAME
from events.types import EventType
from ocpp16.app import create_ocpp_app
from ocpp16.handler import Ocpp16Handler
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


def _call_frame(unique_id: str, action: str, payload: dict) -> str:
    return json.dumps([2, unique_id, action, payload])


def _parse_result(raw: str) -> tuple[int, str, dict]:
    frame = json.loads(raw)
    return int(frame[0]), str(frame[1]), frame[2]


@pytest.mark.asyncio
async def test_get_diagnostics_happy_path_via_rest_and_ws(ocpp_http_server, db_session) -> None:
    charge_point_id = "CP_DIAG_OK"
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
                    if frame[0] == MessageType.CALL and frame[2] == "GetDiagnostics":
                        assert frame[3]["location"] == "ftp://diagnostics.example/upload"
                        assert frame[3]["retries"] == 2
                        assert frame[3]["retryInterval"] == 60
                        await ws.send(
                            json.dumps(
                                [
                                    MessageType.CALLRESULT,
                                    frame[1],
                                    {"fileName": "diag-CP_DIAG_OK.log"},
                                ]
                            )
                        )
                        return
            except websockets.ConnectionClosed:
                return

        reply_task = asyncio.create_task(station_loop())
        async with AsyncClient(
            base_url=ocpp_http_server["http"],
        ) as client:
            response = await client.post(
                f"/get-diagnostics/{charge_point_id}",
                json={
                    "location": "ftp://diagnostics.example/upload",
                    "retries": 2,
                    "retry_interval": 60,
                    "start_time": utc_now_iso(),
                },
            )
        assert response.status_code == 200
        assert response.json() == {
            "status": "success",
            "response": {"fileName": "diag-CP_DIAG_OK.log"},
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
async def test_get_diagnostics_offline_returns_404(ocpp_http_server, db_session) -> None:
    charge_point_id = "CP_DIAG_OFF"
    await ChargerService(db_session).register_boot(
        charge_point_id=charge_point_id, vendor="V", model="M"
    )

    async with AsyncClient(base_url=ocpp_http_server["http"]) as client:
        response = await client.post(
            f"/get-diagnostics/{charge_point_id}",
            json={"location": "ftp://diagnostics.example/upload"},
        )
    assert response.status_code == 404
    assert response.json()["detail"] == {
        "status": "error",
        "response": "Charger not found",
    }


@pytest.mark.asyncio
async def test_diagnostics_status_notification_publishes_bus_event(db_session, fake_redis) -> None:
    charge_point_id = "CP_DIAG_STATUS"
    await ChargerService(db_session).register_boot(
        charge_point_id=charge_point_id, vendor="V", model="M"
    )
    handler = Ocpp16Handler(charge_point_id, db_session)
    msg_type, uid, payload = _parse_result(
        await handler.handle_raw(
            _call_frame("d1", "DiagnosticsStatusNotification", {"status": "Uploading"})
        )
    )
    assert msg_type == 3
    assert uid == "d1"
    assert payload == {}

    rows = await fake_redis.xrange(STREAM_NAME)
    assert rows
    _msg_id, fields = rows[-1]
    assert fields["type"] == EventType.DIAGNOSTICS_STATUS_CHANGED.value
    event_payload = json.loads(fields["payload"])
    assert event_payload == {
        "charge_point_id": charge_point_id,
        "status": "Uploading",
    }


@pytest.mark.asyncio
async def test_firmware_status_notification_publishes_bus_event(db_session, fake_redis) -> None:
    charge_point_id = "CP_FW_STATUS"
    await ChargerService(db_session).register_boot(
        charge_point_id=charge_point_id, vendor="V", model="M"
    )
    handler = Ocpp16Handler(charge_point_id, db_session)
    msg_type, uid, payload = _parse_result(
        await handler.handle_raw(
            _call_frame("f1", "FirmwareStatusNotification", {"status": "Downloading"})
        )
    )
    assert msg_type == 3
    assert uid == "f1"
    assert payload == {}

    rows = await fake_redis.xrange(STREAM_NAME)
    assert rows
    _msg_id, fields = rows[-1]
    assert fields["type"] == EventType.FIRMWARE_STATUS_CHANGED.value
    event_payload = json.loads(fields["payload"])
    assert event_payload == {
        "charge_point_id": charge_point_id,
        "status": "Downloading",
    }
