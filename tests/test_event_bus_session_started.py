import asyncio
import json
from typing import Any

import pytest
import uvicorn
import websockets

from db.time import utc_now_iso
from events.consumer import EventConsumer
from events.types import EventType
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
async def ocpp_server(db_engine, unused_tcp_port, fake_redis):
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


class _CapturingConsumer(EventConsumer):
    def __init__(self) -> None:
        super().__init__(
            group="test-session-started",
            consumer_name="integration-test",
            block_ms=200,
        )
        self.received: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

    async def handle(
        self,
        event_type: EventType,
        payload: dict[str, Any],
        message_id: str,
    ) -> None:
        if event_type == EventType.SESSION_STARTED:
            await self.received.put(payload)


@pytest.mark.asyncio
async def test_session_started_event_via_ws(ocpp_server, fake_redis) -> None:
    charge_point_id = "CP_EVENT_BUS"
    url = f"{ocpp_server}/ocpp/{charge_point_id}"
    now = utc_now_iso()

    consumer = _CapturingConsumer()
    consumer_task = asyncio.create_task(consumer.run())
    try:
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

            start = await _call(
                ws,
                "2",
                "StartTransaction",
                {
                    "connectorId": 1,
                    "idTag": "TAG_EVENT",
                    "meterStart": 42,
                    "timestamp": now,
                },
            )
            assert start["idTagInfo"]["status"] == "Accepted"
            transaction_id = start["transactionId"]
            assert isinstance(transaction_id, int)

        payload = await asyncio.wait_for(consumer.received.get(), timeout=5)
        assert payload["charge_point_id"] == charge_point_id
        assert payload["connector_id"] == 1
        assert payload["id_tag"] == "TAG_EVENT"
        assert payload["meter_start"] == 42
        assert payload["ocpp_transaction_id"] == transaction_id
        assert isinstance(payload["session_id"], int)
        assert payload["session_id"] > 0
    finally:
        consumer.stop()
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
