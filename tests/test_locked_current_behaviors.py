"""Locks CURRENT behaviour from audit-report (do not change production code to pass these)."""

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


# --- 1 & 2: status aliases (StatusNotification path via ChargerService.update_status) ---


@pytest.mark.asyncio
async def test_status_notification_finishing_maps_to_available(db_session) -> None:
    # EvPoint UI quirk: transitional Finishing is stored as Available (see services/status.py).
    chargers = ChargerService(db_session)
    await chargers.register_boot(charge_point_id="CP_FIN", vendor="V", model="M")
    await chargers.update_status("CP_FIN", "Finishing")
    row = await chargers.get("CP_FIN")
    assert row is not None
    assert row.status == "Available"


@pytest.mark.asyncio
async def test_status_notification_suspended_evse_maps_to_suspended_ev(db_session) -> None:
    # EvPoint normalizes SuspendedEVSE → SuspendedEV for downstream consumers.
    chargers = ChargerService(db_session)
    await chargers.register_boot(charge_point_id="CP_SEV", vendor="V", model="M")
    await chargers.update_status("CP_SEV", "SuspendedEVSE")
    row = await chargers.get("CP_SEV")
    assert row is not None
    assert row.status == "SuspendedEV"


# --- 3: Authorize always Accepted ---


@pytest.mark.asyncio
async def test_authorize_always_returns_accepted_for_any_id_tag(db_session) -> None:
    # ADR 004: id_tag is a plain string; Authorize always Accepted (EvPoint-compatible).
    handler = Ocpp16Handler("CP_AUTH", db_session)
    for tag in ("TAG001", "", "unknown-rfid", "';" * 40):
        msg_type, uid, payload = _parse_result(
            await handler.handle_raw(_call_frame("a1", "Authorize", {"idTag": tag}))
        )
        assert msg_type == 3
        assert uid == "a1"
        assert payload["idTagInfo"]["status"] == "Accepted"


# --- 4.2: soft-unavailable ---


async def _make_stale_unavailable(db_session, charge_point_id: str) -> None:
    settings = get_settings()
    assert settings.ocpp_heartbeat_interval == 60
    assert settings.heartbeat_timeout_seconds == 120

    chargers = ChargerService(db_session)
    charger = await chargers.register_boot(
        charge_point_id=charge_point_id, vendor="V", model="M"
    )
    charger.last_heartbeat = datetime.now(UTC) - timedelta(seconds=121)
    charger.status = "Available"
    await db_session.commit()

    marked = await chargers.mark_stale_unavailable(settings.heartbeat_timeout_seconds)
    assert marked == 1
    await db_session.refresh(charger)
    assert charger.status == "Unavailable"


@pytest.mark.asyncio
async def test_soft_unavailable_after_120s_timeout_with_60s_interval(db_session) -> None:
    # Soft offline: interval=60 (Boot) and timeout=120 mark DB Unavailable without closing WS.
    await _make_stale_unavailable(db_session, "CP_STALE_TO")


@pytest.mark.asyncio
async def test_soft_unavailable_heartbeat_restores_via_touch_heartbeat(db_session) -> None:
    # Heartbeat → touch_heartbeat: Unavailable → Available and last_heartbeat refreshed.
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
    # Authorize does not call touch_heartbeat / set_status — status stays Unavailable.
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
    # StatusNotification (connector!=0) uses set_status, not touch_heartbeat, but refreshes last_heartbeat.
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
    # Monitor/soft-unavailable only flips DB status; the open WS still accepts Heartbeat.
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


# --- 5: repeat StartTransaction ---


@pytest.mark.asyncio
async def test_repeat_start_returns_existing_session_without_reject(db_session) -> None:
    # Current idempotency: second Start on same connector returns the existing Active session.
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
            select(func.count()).select_from(ChargingSession).where(
                ChargingSession.charger_id == first.charger_id,
                ChargingSession.status == "Active",
            )
        )
    ).scalar_one()
    assert count == 1


# --- 6: MeterValues without active session ---


@pytest.mark.asyncio
async def test_meter_values_without_active_session_still_returns_empty_success(
    db_session,
) -> None:
    # Until R1 logging: no Active session → no rows written, but CallResult is still empty success.
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_MV", vendor="V", model="M"
    )
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


# --- 7.2: StopTransaction unknown transactionId ---


@pytest.mark.asyncio
async def test_stop_unknown_transaction_id_with_no_active_session_returns_empty_success(
    db_session,
) -> None:
    # No Active session and unknown txId → conf OK, nothing to close.
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
async def test_stop_unknown_transaction_id_fallback_closes_any_active_session(
    db_session,
) -> None:
    # Known bug/current behaviour: unknown txId falls back to any Active session and stops it.
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
    assert active.status == "Completed"
    assert active.meter_stop == 50
    assert active.ocpp_transaction_id == real_tx
