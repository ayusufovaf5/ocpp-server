from __future__ import annotations

import json
import logging

import pytest
from sqlalchemy import select

from db.models import ChargingSession
from db.time import utc_now_iso
from ocpp16.handler import Ocpp16Handler
from services.charger_service import ChargerService
from services.session_service import SessionService


def _call_frame(unique_id: str, action: str, payload: dict) -> str:
    return json.dumps([2, unique_id, action, payload])


def _parse(raw: str) -> tuple[int, str, object]:
    frame = json.loads(raw)
    msg_type = int(frame[0])
    uid = str(frame[1])
    if msg_type == 4:
        return msg_type, uid, {
            "error_code": frame[2],
            "error_description": frame[3],
            "error_details": frame[4] if len(frame) > 4 else {},
        }
    return msg_type, uid, frame[2]


def _assert_logged(event: str, caplog, capsys) -> None:
    captured = capsys.readouterr()
    blob = caplog.text + captured.out + captured.err
    assert event in blob


@pytest.mark.asyncio
async def test_long_id_tag_returns_formation_violation_not_db_error(db_session) -> None:
    handler = Ocpp16Handler("CP_LONG", db_session)
    too_long = "x" * 256
    msg_type, uid, payload = _parse(
        await handler.handle_raw(_call_frame("v1", "Authorize", {"idTag": too_long}))
    )
    assert msg_type == 4
    assert uid == "v1"
    assert payload["error_code"] == "FormationViolation"


@pytest.mark.asyncio
async def test_meter_values_without_session_logs_warning(
    db_session, caplog, capsys
) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_LOG_MV", vendor="V", model="M"
    )
    handler = Ocpp16Handler("CP_LOG_MV", db_session)
    body = {
        "connectorId": 1,
        "transactionId": 999,
        "meterValue": [{"timestamp": utc_now_iso(), "sampledValue": [{"value": "1"}]}],
    }
    with caplog.at_level(logging.WARNING):
        msg_type, _, payload = _parse(
            await handler.handle_raw(_call_frame("m1", "MeterValues", body))
        )
    assert msg_type == 3
    assert payload == {}
    _assert_logged("ocpp.meter_values_without_active_session", caplog, capsys)


@pytest.mark.asyncio
async def test_stop_unknown_transaction_id_logs_warning(
    db_session, caplog, capsys
) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_LOG_STOP", vendor="V", model="M"
    )
    handler = Ocpp16Handler("CP_LOG_STOP", db_session)
    with caplog.at_level(logging.WARNING):
        msg_type, _, payload = _parse(
            await handler.handle_raw(
                _call_frame(
                    "s1",
                    "StopTransaction",
                    {
                        "transactionId": 777,
                        "meterStop": 0,
                        "timestamp": utc_now_iso(),
                    },
                )
            )
        )
    assert msg_type == 3
    assert payload == {}
    _assert_logged("ocpp.stop_unknown_transaction_id", caplog, capsys)


@pytest.mark.asyncio
async def test_heartbeat_unknown_charge_point_logs_warning(
    db_session, caplog, capsys
) -> None:
    handler = Ocpp16Handler("CP_UNKNOWN_HB", db_session)
    with caplog.at_level(logging.WARNING):
        msg_type, _, payload = _parse(
            await handler.handle_raw(_call_frame("h1", "Heartbeat", {}))
        )
    assert msg_type == 3
    assert "currentTime" in payload
    _assert_logged("ocpp.heartbeat_unknown_charge_point", caplog, capsys)


@pytest.mark.asyncio
async def test_status_notification_unknown_charge_point_logs_warning(
    db_session, caplog, capsys
) -> None:
    handler = Ocpp16Handler("CP_UNKNOWN_ST", db_session)
    with caplog.at_level(logging.WARNING):
        msg_type, _, payload = _parse(
            await handler.handle_raw(
                _call_frame(
                    "st1",
                    "StatusNotification",
                    {"connectorId": 1, "errorCode": "NoError", "status": "Available"},
                )
            )
        )
    assert msg_type == 3
    assert payload == {}
    _assert_logged("ocpp.status_notification_unknown_charge_point", caplog, capsys)


@pytest.mark.asyncio
async def test_start_without_boot_returns_internal_error(db_session) -> None:
    handler = Ocpp16Handler("CP_NO_BOOT", db_session)
    msg_type, uid, payload = _parse(
        await handler.handle_raw(
            _call_frame(
                "s0",
                "StartTransaction",
                {
                    "connectorId": 1,
                    "idTag": "TAG",
                    "meterStart": 0,
                    "timestamp": utc_now_iso(),
                },
            )
        )
    )
    assert msg_type == 4
    assert uid == "s0"
    assert payload["error_code"] == "InternalError"
    assert "BootNotification" in payload["error_description"]


@pytest.mark.asyncio
async def test_stop_without_transaction_id_does_not_close_session(db_session) -> None:
    chargers = ChargerService(db_session)
    sessions = SessionService(db_session)
    await chargers.register_boot(charge_point_id="CP_STOP_MISS", vendor="V", model="M")
    active = await sessions.start_transaction(
        charge_point_id="CP_STOP_MISS",
        connector_id=1,
        id_tag="TAG",
        meter_start=1,
        timestamp=utc_now_iso(),
    )

    handler = Ocpp16Handler("CP_STOP_MISS", db_session)
    msg_type, uid, payload = _parse(
        await handler.handle_raw(
            _call_frame(
                "sx",
                "StopTransaction",
                {"meterStop": 10, "timestamp": utc_now_iso()},
            )
        )
    )
    assert msg_type == 4
    assert uid == "sx"
    assert payload["error_code"] == "FormationViolation"

    await db_session.refresh(active)
    assert active.status == "Active"
    still = (
        await db_session.execute(
            select(ChargingSession).where(ChargingSession.id == active.id)
        )
    ).scalar_one()
    assert still.status == "Active"
