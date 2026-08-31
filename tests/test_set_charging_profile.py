from __future__ import annotations

import asyncio
import json

import pytest
import uvicorn
import websockets
from httpx import AsyncClient

from ocpp16.app import create_ocpp_app
from ocpp16.protocol import MessageType
from services.charger_service import ChargerService
from services.remote_control_service import build_set_charging_profile_payload
from state.connection_registry import ConnectionRegistry, set_connection_registry


def test_build_set_charging_profile_payload_tx_absolute_watts() -> None:
    payload = build_set_charging_profile_payload(
        connector_id=1,
        transaction_id=4242,
        limit=7000,
        charging_rate_unit="W",
        number_phases=3,
        stack_level=0,
        charging_profile_id=4242,
        charging_profile_kind="Absolute",
        start_schedule="2026-01-01T00:00:00Z",
    )
    assert payload == {
        "connectorId": 1,
        "csChargingProfiles": {
            "chargingProfileId": 4242,
            "transactionId": 4242,
            "stackLevel": 0,
            "chargingProfilePurpose": "TxProfile",
            "chargingProfileKind": "Absolute",
            "chargingSchedule": {
                "chargingRateUnit": "W",
                "startSchedule": "2026-01-01T00:00:00Z",
                "chargingSchedulePeriod": [{"startPeriod": 0, "limit": 7000.0, "numberPhases": 3}],
            },
        },
    }


def test_build_set_charging_profile_payload_amps_without_phases() -> None:
    payload = build_set_charging_profile_payload(
        connector_id=2,
        transaction_id=99,
        limit=16,
        charging_rate_unit="A",
        number_phases=None,
        charging_profile_id=1,
    )
    period = payload["csChargingProfiles"]["chargingSchedule"]["chargingSchedulePeriod"][0]
    assert payload["connectorId"] == 2
    assert payload["csChargingProfiles"]["chargingProfilePurpose"] == "TxProfile"
    assert payload["csChargingProfiles"]["chargingProfileKind"] == "Relative"
    assert "startSchedule" not in payload["csChargingProfiles"]["chargingSchedule"]
    assert payload["csChargingProfiles"]["chargingSchedule"]["chargingRateUnit"] == "A"
    assert period == {"startPeriod": 0, "limit": 16.0}
    assert "numberPhases" not in period


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
async def test_set_charging_profile_accepted_via_rest_and_ws(ocpp_http_server, db_session) -> None:
    charge_point_id = "CP_SET_PROFILE_OK"
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

        seen: dict = {}

        async def station_loop() -> None:
            try:
                while True:
                    frame = json.loads(await ws.recv())
                    if frame[0] == MessageType.CALL and frame[2] == "SetChargingProfile":
                        seen["payload"] = frame[3]
                        await ws.send(
                            json.dumps([MessageType.CALLRESULT, frame[1], {"status": "Accepted"}])
                        )
                        return
            except websockets.ConnectionClosed:
                return

        reply_task = asyncio.create_task(station_loop())
        async with AsyncClient(base_url=ocpp_http_server["http"]) as client:
            response = await client.post(
                f"/set-charging-profile/{charge_point_id}",
                json={
                    "connector_id": 1,
                    "transaction_id": 555001,
                    "limit": 7000,
                    "charging_rate_unit": "W",
                    "number_phases": 3,
                },
            )
        assert response.status_code == 200
        assert response.json() == {
            "status": "success",
            "response": {"status": "Accepted"},
        }
        assert seen["payload"] == build_set_charging_profile_payload(
            connector_id=1,
            transaction_id=555001,
            limit=7000,
            charging_rate_unit="W",
            number_phases=3,
            charging_profile_id=555001,
        )
        if not reply_task.done():
            reply_task.cancel()
            try:
                await reply_task
            except asyncio.CancelledError:
                pass
        else:
            await reply_task


@pytest.mark.asyncio
async def test_set_charging_profile_rejected_via_rest_and_ws(ocpp_http_server, db_session) -> None:
    charge_point_id = "CP_SET_PROFILE_REJ"
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
                    if frame[0] == MessageType.CALL and frame[2] == "SetChargingProfile":
                        await ws.send(
                            json.dumps([MessageType.CALLRESULT, frame[1], {"status": "Rejected"}])
                        )
                        return
            except websockets.ConnectionClosed:
                return

        reply_task = asyncio.create_task(station_loop())
        async with AsyncClient(base_url=ocpp_http_server["http"]) as client:
            response = await client.post(
                f"/set-charging-profile/{charge_point_id}",
                json={
                    "connector_id": 1,
                    "transaction_id": 42,
                    "limit": 10,
                    "charging_rate_unit": "A",
                },
            )
        assert response.status_code == 200
        assert response.json() == {
            "status": "success",
            "response": {"status": "Rejected"},
        }
        if not reply_task.done():
            reply_task.cancel()
            try:
                await reply_task
            except asyncio.CancelledError:
                pass
        else:
            await reply_task
