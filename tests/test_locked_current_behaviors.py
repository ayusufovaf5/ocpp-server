from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select

from config import get_settings
from db.models import ChargingSession, MeterValue
from db.time import utc_now_iso
from ocpp16.app import create_ocpp_app
from ocpp16.handler import Ocpp16Handler
from services.charger_service import ChargerService
from services.session_service import SessionService


def _call_frame(unique_id: str, action: str, payload: dict) -> str:
    return json.dumps([2, unique_id, action, payload])


def _parse_result(raw: str) -> tuple[int, str, dict | list]:
    frame = json.loads(raw)
    return int(frame[0]), str(frame[1]), frame[2]


def _as_naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


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
async def test_status_notification_finishing_maps_to_available_with_tx(
    db_session, monkeypatch
) -> None:
    published: list[tuple] = []

    async def capture_publish(_self, event_type, payload):
        published.append((event_type, payload))
        return "1-0"

    monkeypatch.setattr(
        "services.charger_service.get_publisher",
        lambda: type("P", (), {"publish": capture_publish})(),
    )

    chargers = ChargerService(db_session)
    sessions = SessionService(db_session)
    await chargers.register_boot(charge_point_id="CP_FIN", vendor="V", model="M")
    session = await sessions.start_transaction(
        charge_point_id="CP_FIN",
        connector_id=1,
        id_tag="TAG",
        meter_start=0,
        timestamp=utc_now_iso(),
    )
    assert session.ocpp_transaction_id is not None
    await sessions.stop_transaction(
        charge_point_id="CP_FIN",
        transaction_id=session.ocpp_transaction_id,
        meter_stop=10,
        timestamp=utc_now_iso(),
        connector_id=1,
    )
    published.clear()

    await chargers.update_status("CP_FIN", "Finishing", connector_id=1)
    row = await chargers.get("CP_FIN")
    assert row is not None
    assert row.status == "Available"

    assert len(published) == 1
    _, payload = published[0]
    assert payload["status"] == "Available"
    assert payload["ocpp_transaction_id"] == session.ocpp_transaction_id


@pytest.mark.asyncio
async def test_stale_charging_status_without_active_session_maps_to_available(
    db_session,
) -> None:
    chargers = ChargerService(db_session)
    sessions = SessionService(db_session)
    await chargers.register_boot(charge_point_id="CP_STALE_CH", vendor="V", model="M")
    session = await sessions.start_transaction(
        charge_point_id="CP_STALE_CH",
        connector_id=1,
        id_tag="TAG",
        meter_start=0,
        timestamp=utc_now_iso(),
    )
    await sessions.stop_transaction(
        charge_point_id="CP_STALE_CH",
        transaction_id=session.ocpp_transaction_id,
        meter_stop=10,
        timestamp=utc_now_iso(),
    )
    await chargers.update_status("CP_STALE_CH", "Charging", connector_id=1)
    row = await chargers.get("CP_STALE_CH")
    assert row is not None
    assert row.status == "Available"


@pytest.mark.asyncio
async def test_stale_charging_outside_remap_window_keeps_reported_status(
    db_session,
) -> None:
    from datetime import timedelta

    from db.time import utc_now
    from repositories.session_repository import SessionRepository

    chargers = ChargerService(db_session)
    sessions = SessionService(db_session)
    await chargers.register_boot(charge_point_id="CP_STALE_OLD", vendor="V", model="M")
    session = await sessions.start_transaction(
        charge_point_id="CP_STALE_OLD",
        connector_id=1,
        id_tag="TAG",
        meter_start=0,
        timestamp=utc_now_iso(),
    )
    await sessions.stop_transaction(
        charge_point_id="CP_STALE_OLD",
        transaction_id=session.ocpp_transaction_id,
        meter_stop=10,
        timestamp=utc_now_iso(),
    )
    row_session = await SessionRepository(db_session).get_by_ocpp_transaction_id(
        session.ocpp_transaction_id
    )
    assert row_session is not None
    row_session.stopped_at = utc_now() - timedelta(seconds=120)
    await db_session.commit()

    await chargers.update_status("CP_STALE_OLD", "Charging", connector_id=1)
    row = await chargers.get("CP_STALE_OLD")
    assert row is not None
    assert row.status == "Charging"


@pytest.mark.asyncio
async def test_preparing_without_session_is_never_remapped(db_session) -> None:
    chargers = ChargerService(db_session)
    await chargers.register_boot(charge_point_id="CP_PREP", vendor="V", model="M")
    await chargers.update_status("CP_PREP", "Preparing", connector_id=1)
    row = await chargers.get("CP_PREP")
    assert row is not None
    assert row.status == "Preparing"


@pytest.mark.asyncio
async def test_status_notification_suspended_evse_maps_to_suspended_ev(db_session) -> None:
    chargers = ChargerService(db_session)
    await chargers.register_boot(charge_point_id="CP_SEV", vendor="V", model="M")
    await chargers.update_status("CP_SEV", "SuspendedEVSE")
    row = await chargers.get("CP_SEV")
    assert row is not None
    assert row.status == "SuspendedEV"


@pytest.mark.asyncio
async def test_authorize_always_returns_accepted_for_any_id_tag(db_session) -> None:
    handler = Ocpp16Handler("CP_AUTH", db_session)
    for tag in ("TAG001", "", "unknown-rfid", "x" * 40):
        msg_type, uid, payload = _parse_result(
            await handler.handle_raw(_call_frame("a1", "Authorize", {"idTag": tag}))
        )
        assert msg_type == 3
        assert uid == "a1"
        assert payload["idTagInfo"]["status"] == "Accepted"


async def _make_stale_unavailable(db_session, charge_point_id: str) -> None:
    settings = get_settings()
    assert settings.ocpp_heartbeat_interval == 60
    assert settings.heartbeat_timeout_seconds == 120

    chargers = ChargerService(db_session)
    charger = await chargers.register_boot(charge_point_id=charge_point_id, vendor="V", model="M")
    charger.last_heartbeat = datetime.now(UTC) - timedelta(seconds=121)
    charger.status = "Available"
    await db_session.commit()

    marked = await chargers.mark_stale_unavailable(settings.heartbeat_timeout_seconds)
    assert marked == 1
    await db_session.refresh(charger)
    assert charger.status == "Unavailable"


@pytest.mark.asyncio
async def test_soft_unavailable_after_120s_timeout_with_60s_interval(db_session) -> None:
    await _make_stale_unavailable(db_session, "CP_STALE_TO")


@pytest.mark.asyncio
async def test_soft_unavailable_heartbeat_restores_via_touch_heartbeat(db_session) -> None:
    charge_point_id = "CP_STALE_HB"
    await _make_stale_unavailable(db_session, charge_point_id)
    chargers = ChargerService(db_session)
    before = (await chargers.get(charge_point_id)).last_heartbeat

    restored = await chargers.heartbeat(charge_point_id)
    assert restored is not None
    assert restored.status == "Available"
    assert restored.last_heartbeat is not None
    if before is not None:
        assert _as_naive_utc(restored.last_heartbeat) >= _as_naive_utc(before)


@pytest.mark.asyncio
async def test_soft_unavailable_authorize_does_not_restore_status(db_session) -> None:
    charge_point_id = "CP_STALE_AUTH"
    await _make_stale_unavailable(db_session, charge_point_id)

    handler = Ocpp16Handler(charge_point_id, db_session)
    msg_type, _, payload = _parse_result(
        await handler.handle_raw(_call_frame("a2", "Authorize", {"idTag": "TAG"}))
    )
    assert msg_type == 3
    assert payload["idTagInfo"]["status"] == "Accepted"

    row = await ChargerService(db_session).get(charge_point_id)
    assert row is not None
    assert row.status == "Unavailable"


@pytest.mark.asyncio
async def test_status_notification_set_status_updates_last_heartbeat(db_session) -> None:
    chargers = ChargerService(db_session)
    charger = await chargers.register_boot(charge_point_id="CP_HB_TS", vendor="V", model="M")
    old_ts = datetime(2000, 1, 1, tzinfo=UTC)
    charger.last_heartbeat = old_ts
    await db_session.commit()

    await chargers.update_status("CP_HB_TS", "Preparing")
    await db_session.refresh(charger)
    assert charger.status == "Preparing"
    assert charger.last_heartbeat is not None
    assert _as_naive_utc(charger.last_heartbeat) > _as_naive_utc(old_ts)


@pytest.mark.asyncio
async def test_soft_unavailable_does_not_tear_down_websocket(ocpp_server, db_session) -> None:
    import websockets

    charge_point_id = "CP_WS_SOFT"
    url = f"{ocpp_server}/ocpp/{charge_point_id}"

    async with websockets.connect(url, subprotocols=["ocpp1.6"]) as ws:
        await ws.send(
            _call_frame(
                "1",
                "BootNotification",
                {"chargePointVendor": "V", "chargePointModel": "M"},
            )
        )
        boot = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
        assert boot[0] == 3
        assert boot[2]["interval"] == get_settings().ocpp_heartbeat_interval

        await _make_stale_unavailable(db_session, charge_point_id)

        await ws.send(_call_frame("2", "Heartbeat", {}))
        hb = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
        assert hb[0] == 3
        assert "currentTime" in hb[2]

    row = await ChargerService(db_session).get(charge_point_id)
    assert row is not None
    assert row.status == "Available"


@pytest.mark.asyncio
async def test_repeat_start_returns_existing_session_without_reject(db_session) -> None:
    chargers = ChargerService(db_session)
    sessions = SessionService(db_session)
    await chargers.register_boot(charge_point_id="CP_RPT", vendor="V", model="M")
    now = utc_now_iso()

    first = await sessions.start_transaction(
        charge_point_id="CP_RPT",
        connector_id=1,
        id_tag="TAG_A",
        meter_start=100,
        timestamp=now,
    )
    second = await sessions.start_transaction(
        charge_point_id="CP_RPT",
        connector_id=1,
        id_tag="TAG_B",
        meter_start=999,
        timestamp=now,
    )
    assert second.id == first.id
    assert second.ocpp_transaction_id == first.ocpp_transaction_id
    assert second.id_tag == "TAG_A"
    assert second.meter_start == 100
    assert second.status == "Active"

    count = (
        await db_session.execute(
            select(func.count())
            .select_from(ChargingSession)
            .where(
                ChargingSession.charger_id == first.charger_id,
                ChargingSession.status == "Active",
            )
        )
    ).scalar_one()
    assert count == 1


@pytest.mark.asyncio
async def test_meter_values_without_active_session_still_returns_empty_success(
    db_session,
) -> None:
    await ChargerService(db_session).register_boot(charge_point_id="CP_MV", vendor="V", model="M")
    handler = Ocpp16Handler("CP_MV", db_session)
    now = utc_now_iso()
    raw = await handler.handle_raw(
        _call_frame(
            "m1",
            "MeterValues",
            {
                "connectorId": 1,
                "transactionId": 999999,
                "meterValue": [
                    {
                        "timestamp": now,
                        "sampledValue": [{"value": "12", "unit": "Wh"}],
                    }
                ],
            },
        )
    )
    msg_type, uid, payload = _parse_result(raw)
    assert msg_type == 3
    assert uid == "m1"
    assert payload == {}

    meters = (await db_session.execute(select(MeterValue))).scalars().all()
    assert meters == []


@pytest.mark.asyncio
async def test_stop_unknown_transaction_id_with_no_active_session_returns_empty_success(
    db_session,
) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_STOP0", vendor="V", model="M"
    )
    handler = Ocpp16Handler("CP_STOP0", db_session)
    raw = await handler.handle_raw(
        _call_frame(
            "s0",
            "StopTransaction",
            {
                "transactionId": 424242,
                "meterStop": 0,
                "timestamp": utc_now_iso(),
            },
        )
    )
    msg_type, uid, payload = _parse_result(raw)
    assert msg_type == 3
    assert uid == "s0"
    assert payload == {}

    sessions = (await db_session.execute(select(ChargingSession))).scalars().all()
    assert sessions == []


@pytest.mark.asyncio
async def test_stop_unknown_transaction_id_does_not_close_active_session(
    db_session,
) -> None:
    chargers = ChargerService(db_session)
    sessions = SessionService(db_session)
    await chargers.register_boot(charge_point_id="CP_STOP_FB", vendor="V", model="M")
    active = await sessions.start_transaction(
        charge_point_id="CP_STOP_FB",
        connector_id=1,
        id_tag="TAG",
        meter_start=10,
        timestamp=utc_now_iso(),
    )
    real_tx = active.ocpp_transaction_id
    assert real_tx is not None
    wrong_tx = real_tx + 10_000

    handler = Ocpp16Handler("CP_STOP_FB", db_session)
    raw = await handler.handle_raw(
        _call_frame(
            "s1",
            "StopTransaction",
            {
                "transactionId": wrong_tx,
                "meterStop": 50,
                "timestamp": utc_now_iso(),
            },
        )
    )
    msg_type, uid, payload = _parse_result(raw)
    assert msg_type == 3
    assert uid == "s1"
    assert payload == {}

    await db_session.refresh(active)
    assert active.status == "Active"
    assert active.meter_stop is None
    assert active.ocpp_transaction_id == real_tx
