from __future__ import annotations

import asyncio
import json

import pytest
import uvicorn
import websockets

from db.time import utc_now
from ocpp16.app import create_ocpp_app
from ocpp16.protocol import MessageType
from repositories.charger_repository import ChargerRepository
from services.charger_service import ChargerService
from services.errors import ChargerOfflineError, ChargerTimeoutError
from services.remote_control_service import RemoteControlService
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
async def ocpp_server(db_engine, unused_tcp_port):
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
    yield f"ws://127.0.0.1:{unused_tcp_port}"
    server.should_exit = True
    await task
    set_connection_registry(None)


@pytest.mark.asyncio
async def test_outbound_call_returns_callresult_from_online_station(
    ocpp_server, db_session
) -> None:
    charge_point_id = "CP_OUT_OK"
    url = f"{ocpp_server}/ocpp/{charge_point_id}"
    await ChargerService(db_session).register_boot(
        charge_point_id=charge_point_id, vendor="V", model="M"
    )

    async with websockets.connect(url, subprotocols=["ocpp1.6"]) as ws:
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
        boot = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
        assert boot[0] == 3

        async def station_reply_loop() -> None:
            try:
                while True:
                    raw = await ws.recv()
                    frame = json.loads(raw)
                    if frame[0] == MessageType.CALL and frame[2] == "Reset":
                        await ws.send(
                            json.dumps([MessageType.CALLRESULT, frame[1], {"status": "Accepted"}])
                        )
                        return
            except websockets.ConnectionClosed:
                return

        reply_task = asyncio.create_task(station_reply_loop())
        try:
            result = await RemoteControlService(db_session).call(
                charge_point_id, "Reset", {"type": "Soft"}
            )
            assert result == {"status": "Accepted"}
        finally:
            if not reply_task.done():
                reply_task.cancel()
                try:
                    await reply_task
                except asyncio.CancelledError:
                    pass
            else:
                await reply_task


@pytest.mark.asyncio
async def test_outbound_call_times_out_without_callresult(
    ocpp_server, db_session, monkeypatch
) -> None:
    charge_point_id = "CP_OUT_TO"
    url = f"{ocpp_server}/ocpp/{charge_point_id}"
    await ChargerService(db_session).register_boot(
        charge_point_id=charge_point_id, vendor="V", model="M"
    )
    monkeypatch.setenv("OUTBOUND_CALL_TIMEOUT_SECONDS", "0.2")
    from config import get_settings

    get_settings.cache_clear()

    async with websockets.connect(url, subprotocols=["ocpp1.6"]) as ws:
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

        drain = asyncio.create_task(ws.recv())
        with pytest.raises(ChargerTimeoutError):
            await RemoteControlService(db_session).call(
                charge_point_id,
                "Reset",
                {"type": "Soft"},
                timeout_seconds=0.2,
            )
        drain.cancel()
        try:
            await drain
        except asyncio.CancelledError:
            pass

    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_outbound_call_to_offline_station_fails_immediately(db_session) -> None:
    set_connection_registry(ConnectionRegistry())
    charge_point_id = "CP_OUT_OFF"
    await ChargerService(db_session).register_boot(
        charge_point_id=charge_point_id, vendor="V", model="M"
    )
    repo = ChargerRepository(db_session)
    await repo.mark_disconnected(charge_point_id, utc_now())
    await db_session.commit()

    started = asyncio.get_running_loop().time()
    with pytest.raises(ChargerOfflineError):
        await RemoteControlService(db_session).call(
            charge_point_id, "Reset", {"type": "Soft"}, timeout_seconds=30
        )
    elapsed = asyncio.get_running_loop().time() - started
    assert elapsed < 1.0
    set_connection_registry(None)
