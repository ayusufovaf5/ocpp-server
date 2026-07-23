import asyncio
import json

import pytest
import uvicorn
import websockets
from sqlalchemy import select

import db as db_module
from config import get_settings
from db.models import Charger, ChargingSession, MeterValue
from db.time import utc_now_iso
from ocpp16.app import create_ocpp_app


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


async def _call(ws, unique_id: str, action: str, payload: dict) -> dict:
    await ws.send(json.dumps([2, unique_id, action, payload]))
    raw = await asyncio.wait_for(ws.recv(), timeout=5)
    frame = json.loads(raw)
    assert frame[0] == 3, frame
    assert frame[1] == unique_id
    return frame[2]


@pytest.mark.asyncio
async def test_ocpp16_full_cycle(ocpp_server, db_engine) -> None:
    charge_point_id = "CP_TEST"
    url = f"{ocpp_server}/ocpp/{charge_point_id}"
    now = utc_now_iso()
    interval = get_settings().ocpp_heartbeat_interval

    async with websockets.connect(url, subprotocols=["ocpp1.6"]) as ws:
        boot = await _call(
            ws,
            "1",
            "BootNotification",
            {
                "chargePointVendor": "EvPoint",
                "chargePointModel": "Simulator",
                "firmwareVersion": "1.0.0",
            },
        )
        assert boot["status"] == "Accepted"
        assert boot["interval"] == interval

        status = await _call(
            ws,
            "2",
            "StatusNotification",
            {"connectorId": 1, "errorCode": "NoError", "status": "Available"},
        )
        assert status == {}

        auth = await _call(ws, "3", "Authorize", {"idTag": "TAG001"})
        assert auth["idTagInfo"]["status"] == "Accepted"

        start = await _call(
            ws,
            "4",
            "StartTransaction",
            {
                "connectorId": 1,
                "idTag": "TAG001",
                "meterStart": 1000,
                "timestamp": now,
            },
        )
        assert start["idTagInfo"]["status"] == "Accepted"
        transaction_id = start["transactionId"]
        assert isinstance(transaction_id, int)
        assert transaction_id > 0

        for i, energy in enumerate((1500, 2000), start=5):
            meter = await _call(
                ws,
                str(i),
                "MeterValues",
                {
                    "connectorId": 1,
                    "transactionId": transaction_id,
                    "meterValue": [
                        {
                            "timestamp": now,
                            "sampledValue": [
                                {
                                    "value": str(energy),
                                    "measurand": "Energy.Active.Import.Register",
                                    "unit": "Wh",
                                }
                            ],
                        }
                    ],
                },
            )
            assert meter == {}

        stop = await _call(
            ws,
            "7",
            "StopTransaction",
            {
                "transactionId": transaction_id,
                "meterStop": 2500,
                "timestamp": now,
                "reason": "Local",
            },
        )
        assert stop == {} or stop.get("idTagInfo") is None

    async with db_module.async_session_factory() as db:
        charger = (
            await db.execute(select(Charger).where(Charger.charge_point_id == charge_point_id))
        ).scalar_one()
        assert charger.vendor == "EvPoint"
        assert charger.model == "Simulator"
        assert charger.status == "Available"
        assert charger.last_heartbeat is not None

        charging = (
            await db.execute(
                select(ChargingSession).where(ChargingSession.ocpp_transaction_id == transaction_id)
            )
        ).scalar_one()
        assert charging.id_tag == "TAG001"
        assert charging.meter_start == 1000
        assert charging.meter_stop == 2500
        assert charging.status == "Completed"
        assert charging.stopped_at is not None

        meters = (
            (await db.execute(select(MeterValue).where(MeterValue.session_id == charging.id)))
            .scalars()
            .all()
        )
        assert len(meters) == 2
        assert sorted(m.value for m in meters) == [1500.0, 2000.0]


@pytest.mark.asyncio
async def test_heartbeat_marks_unavailable(db_session) -> None:
    from datetime import UTC, datetime

    from services.charger_service import ChargerService

    service = ChargerService(db_session)
    charger = await service.register_boot(
        charge_point_id="CP_STALE",
        vendor="X",
        model="Y",
    )
    charger.last_heartbeat = datetime(2000, 1, 1, tzinfo=UTC)
    charger.status = "Available"
    await db_session.commit()

    marked = await service.mark_stale_unavailable(timeout_seconds=120)
    assert marked == 1

    await db_session.refresh(charger)
    assert charger.status == "Unavailable"
