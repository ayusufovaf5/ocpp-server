from __future__ import annotations

import json
import logging

import pytest
from sqlalchemy import select

from db.models import ChargingSession, MeterValue
from db.time import utc_now_iso
from ocpp16.handler import Ocpp16Handler
from services import ops_alerts
from services.charger_service import ChargerService
from services.session_service import SessionService


def _call_frame(unique_id: str, action: str, payload: dict) -> str:
    return json.dumps([2, unique_id, action, payload])


@pytest.fixture(autouse=True)
def _reset_ops_metrics() -> None:
    ops_alerts.reset_for_tests()


@pytest.mark.asyncio
async def test_stop_unknown_transaction_id_warns_and_keeps_active_session(
    db_session, caplog, capsys
) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_STOP_KEEP", vendor="V", model="M"
    )
    active = await SessionService(db_session).start_transaction(
        charge_point_id="CP_STOP_KEEP",
        connector_id=1,
        id_tag="TAG",
        meter_start=10,
        timestamp=utc_now_iso(),
    )
    assert active.ocpp_transaction_id is not None
    wrong_tx = active.ocpp_transaction_id + 999

    handler = Ocpp16Handler("CP_STOP_KEEP", db_session)
    with caplog.at_level(logging.WARNING):
        frame = json.loads(
            await handler.handle_raw(
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
        )

    assert frame[0] == 3
    assert frame[2] == {}
    captured = capsys.readouterr()
    blob = caplog.text + captured.out + captured.err
    assert "ocpp.stop_unknown_transaction_id" in blob
    assert "ops.alert" in blob
    assert ops_alerts.get_count("alert.ocpp.stop_unknown_transaction_id") == 1

    await db_session.refresh(active)
    assert active.status == "Active"
    assert active.meter_stop is None

    active_count = len(
        (
            await db_session.execute(
                select(ChargingSession).where(ChargingSession.status == "Active")
            )
        )
        .scalars()
        .all()
    )
    assert active_count == 1


@pytest.mark.asyncio
async def test_stop_transaction_persists_transaction_data_as_meter_values(
    db_session,
) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_STOP_TD", vendor="V", model="M"
    )
    active = await SessionService(db_session).start_transaction(
        charge_point_id="CP_STOP_TD",
        connector_id=1,
        id_tag="TAG",
        meter_start=100,
        timestamp=utc_now_iso(),
    )
    assert active.ocpp_transaction_id is not None
    now = utc_now_iso()

    handler = Ocpp16Handler("CP_STOP_TD", db_session)
    frame = json.loads(
        await handler.handle_raw(
            _call_frame(
                "s2",
                "StopTransaction",
                {
                    "transactionId": active.ocpp_transaction_id,
                    "meterStop": 2500,
                    "timestamp": now,
                    "transactionData": [
                        {
                            "timestamp": now,
                            "sampledValue": [
                                {
                                    "value": "2.5",
                                    "unit": "kWh",
                                    "measurand": "Energy.Active.Import.Register",
                                }
                            ],
                        }
                    ],
                },
            )
        )
    )
    assert frame[0] == 3

    await db_session.refresh(active)
    assert active.status == "Completed"
    assert active.meter_stop == 2500

    meters = (
        await db_session.execute(
            select(MeterValue).where(MeterValue.session_id == active.id)
        )
    ).scalars().all()
    assert len(meters) == 1
    assert meters[0].value == 2500.0
    assert meters[0].unit == "Wh"
    assert meters[0].measurand == "Energy.Active.Import.Register"
