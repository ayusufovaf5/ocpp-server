from __future__ import annotations

import asyncio
import json
from datetime import timedelta

import pytest

from config import get_settings
from db.time import utc_now, utc_now_iso
from ocpp16.app import create_ocpp_app
from services.charger_service import ChargerService
from services.session_service import (
    END_REASON_CONNECTION_TIMEOUT,
    SessionService,
)


def _call_frame(unique_id: str, action: str, payload: dict) -> str:
    return json.dumps([2, unique_id, action, payload])


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
    import uvicorn

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


@pytest.mark.asyncio
async def test_offline_sweep_before_grace_keeps_session_active(db_session) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_OFF_EARLY", vendor="V", model="M"
    )
    active = await SessionService(db_session).start_transaction(
        charge_point_id="CP_OFF_EARLY",
        connector_id=1,
        id_tag="TAG",
        meter_start=10,
        timestamp=utc_now_iso(),
    )
    await ChargerService(db_session).mark_disconnected("CP_OFF_EARLY")

    closed = await SessionService(db_session).close_offline_timed_out_sessions(
        get_settings().offline_session_grace_period_seconds
    )
    assert closed == 0
    await db_session.refresh(active)
    assert active.status == "Active"


@pytest.mark.asyncio
async def test_offline_sweep_after_grace_closes_with_estimated_meter_stop(
    db_session,
) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_OFF_LATE", vendor="V", model="M"
    )
    sessions = SessionService(db_session)
    active = await sessions.start_transaction(
        charge_point_id="CP_OFF_LATE",
        connector_id=1,
        id_tag="TAG",
        meter_start=100,
        timestamp=utc_now_iso(),
    )
    assert active.ocpp_transaction_id is not None
    now = utc_now_iso()
    await sessions.record_meter_values(
        charge_point_id="CP_OFF_LATE",
        connector_id=1,
        transaction_id=active.ocpp_transaction_id,
        meter_value=[
            {
                "timestamp": now,
                "sampledValue": [{"value": "1500", "unit": "Wh"}],
            }
        ],
    )

    charger = await ChargerService(db_session).get("CP_OFF_LATE")
    assert charger is not None
    charger.disconnected_at = utc_now() - timedelta(seconds=301)
    await db_session.commit()

    closed = await SessionService(db_session).close_offline_timed_out_sessions(300)
    assert closed == 1
    await db_session.refresh(active)
    assert active.status == "Completed"
    assert active.end_reason == END_REASON_CONNECTION_TIMEOUT
    assert active.meter_stop_estimated is True
    assert active.meter_stop == 1500
    assert active.effective_end_reason == END_REASON_CONNECTION_TIMEOUT


@pytest.mark.asyncio
async def test_reconnect_clears_disconnected_at_and_cancels_grace(db_session) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_OFF_RE", vendor="V", model="M"
    )
    active = await SessionService(db_session).start_transaction(
        charge_point_id="CP_OFF_RE",
        connector_id=1,
        id_tag="TAG",
        meter_start=10,
        timestamp=utc_now_iso(),
    )
    charger = await ChargerService(db_session).get("CP_OFF_RE")
    assert charger is not None
    original_disconnect = utc_now() - timedelta(seconds=301)
    charger.disconnected_at = original_disconnect
    await db_session.commit()

    await ChargerService(db_session).clear_disconnected("CP_OFF_RE")
    await db_session.refresh(charger)
    assert charger.disconnected_at is None

    closed = await SessionService(db_session).close_offline_timed_out_sessions(300)
    assert closed == 0
    await db_session.refresh(active)
    assert active.status == "Active"


@pytest.mark.asyncio
async def test_offline_sweep_closes_all_active_sessions_multi_connector(
    db_session,
) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_OFF_MULTI",
        vendor="V",
        model="M",
        connector_count=2,
    )
    sessions = SessionService(db_session)
    s1 = await sessions.start_transaction(
        charge_point_id="CP_OFF_MULTI",
        connector_id=1,
        id_tag="A",
        meter_start=10,
        timestamp=utc_now_iso(),
    )
    s2 = await sessions.start_transaction(
        charge_point_id="CP_OFF_MULTI",
        connector_id=2,
        id_tag="B",
        meter_start=20,
        timestamp=utc_now_iso(),
    )
    assert s1.ocpp_transaction_id is not None
    assert s2.ocpp_transaction_id is not None
    now = utc_now_iso()
    await sessions.record_meter_values(
        charge_point_id="CP_OFF_MULTI",
        connector_id=1,
        transaction_id=s1.ocpp_transaction_id,
        meter_value=[{"timestamp": now, "sampledValue": [{"value": "111", "unit": "Wh"}]}],
    )
    await sessions.record_meter_values(
        charge_point_id="CP_OFF_MULTI",
        connector_id=2,
        transaction_id=s2.ocpp_transaction_id,
        meter_value=[{"timestamp": now, "sampledValue": [{"value": "222", "unit": "Wh"}]}],
    )

    charger = await ChargerService(db_session).get("CP_OFF_MULTI")
    assert charger is not None
    charger.disconnected_at = utc_now() - timedelta(seconds=400)
    await db_session.commit()

    closed = await SessionService(db_session).close_offline_timed_out_sessions(300)
    assert closed == 2
    await db_session.refresh(s1)
    await db_session.refresh(s2)
    assert s1.status == "Completed"
    assert s2.status == "Completed"
    assert s1.meter_stop == 111
    assert s2.meter_stop == 222
    assert s1.end_reason == END_REASON_CONNECTION_TIMEOUT
    assert s2.end_reason == END_REASON_CONNECTION_TIMEOUT
    assert s1.meter_stop_estimated is True
    assert s2.meter_stop_estimated is True


@pytest.mark.asyncio
async def test_heartbeat_soft_unavailable_without_disconnect_does_not_close_sessions(
    db_session,
) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_OFF_HB", vendor="V", model="M"
    )
    active = await SessionService(db_session).start_transaction(
        charge_point_id="CP_OFF_HB",
        connector_id=1,
        id_tag="TAG",
        meter_start=10,
        timestamp=utc_now_iso(),
    )
    charger = await ChargerService(db_session).get("CP_OFF_HB")
    assert charger is not None
    charger.last_heartbeat = utc_now() - timedelta(seconds=121)
    charger.disconnected_at = None
    await db_session.commit()

    marked = await ChargerService(db_session).mark_stale_unavailable(120)
    assert marked == 1
    closed = await SessionService(db_session).close_offline_timed_out_sessions(300)
    assert closed == 0
    await db_session.refresh(active)
    assert active.status == "Active"
    await db_session.refresh(charger)
    assert charger.status == "Unavailable"
    assert charger.disconnected_at is None


@pytest.mark.asyncio
async def test_ws_disconnect_sets_disconnected_at_and_reconnect_clears(
    ocpp_server, db_session
) -> None:
    import websockets

    charge_point_id = "CP_OFF_WS"
    await ChargerService(db_session).register_boot(
        charge_point_id=charge_point_id, vendor="V", model="M"
    )
    url = f"{ocpp_server}/ocpp/{charge_point_id}"

    async with websockets.connect(url, subprotocols=["ocpp1.6"]) as ws:
        await ws.send(
            _call_frame(
                "1",
                "BootNotification",
                {"chargePointVendor": "V", "chargePointModel": "M"},
            )
        )
        assert json.loads(await asyncio.wait_for(ws.recv(), timeout=5))[0] == 3

    for _ in range(50):
        row = await ChargerService(db_session).get(charge_point_id)
        assert row is not None
        await db_session.refresh(row)
        if row.disconnected_at is not None:
            break
        await asyncio.sleep(0.05)
    else:
        pytest.fail("disconnected_at was not set after WS close")

    async with websockets.connect(url, subprotocols=["ocpp1.6"]):
        for _ in range(50):
            row = await ChargerService(db_session).get(charge_point_id)
            assert row is not None
            await db_session.refresh(row)
            if row.disconnected_at is None:
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail("disconnected_at was not cleared on reconnect")


@pytest.mark.asyncio
async def test_offline_sweep_without_meter_values_uses_meter_start(db_session) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_OFF_NOM", vendor="V", model="M"
    )
    active = await SessionService(db_session).start_transaction(
        charge_point_id="CP_OFF_NOM",
        connector_id=1,
        id_tag="TAG",
        meter_start=77,
        timestamp=utc_now_iso(),
    )
    charger = await ChargerService(db_session).get("CP_OFF_NOM")
    assert charger is not None
    charger.disconnected_at = utc_now() - timedelta(seconds=400)
    await db_session.commit()

    closed = await SessionService(db_session).close_offline_timed_out_sessions(300)
    assert closed == 1
    await db_session.refresh(active)
    assert active.meter_stop == 77
    assert active.meter_stop_estimated is True


@pytest.mark.asyncio
async def test_null_end_reason_reads_as_station_stop(db_session) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_OFF_NULL", vendor="V", model="M"
    )
    active = await SessionService(db_session).start_transaction(
        charge_point_id="CP_OFF_NULL",
        connector_id=1,
        id_tag="TAG",
        meter_start=1,
        timestamp=utc_now_iso(),
    )
    active.end_reason = None
    active.status = "Completed"
    active.meter_stop = 1
    await db_session.commit()
    await db_session.refresh(active)
    assert active.effective_end_reason == "station_stop"
