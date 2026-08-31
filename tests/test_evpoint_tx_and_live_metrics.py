from __future__ import annotations

from datetime import timedelta

import pytest

from config import get_settings
from db.time import utc_now, utc_now_iso
from services.charger_service import ChargerService
from services.evpoint_live_context import should_skip_preparing_status_push
from services.live_status_service import LiveStatusService
from services.session_service import SessionService
from state.connection_state import get_connection_state


@pytest.mark.asyncio
async def test_start_transaction_uses_pending_evpoint_transaction_id(db_session) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_TX_BIND", vendor="V", model="M"
    )
    await get_connection_state().set_pending_remote_start(
        "CP_TX_BIND",
        1,
        id_tag="USER",
        transaction_id=424242,
    )

    session = await SessionService(db_session).start_transaction(
        charge_point_id="CP_TX_BIND",
        connector_id=1,
        id_tag="OTHER",
        meter_start=0,
        timestamp=utc_now_iso(),
    )
    assert session.ocpp_transaction_id == 424242
    assert session.id_tag == "USER"
    assert await get_connection_state().take_pending_remote_start("CP_TX_BIND", 1) is None


@pytest.mark.asyncio
async def test_start_without_pending_keeps_session_id_as_ocpp_tx(db_session) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_TX_AUTO", vendor="V", model="M"
    )
    session = await SessionService(db_session).start_transaction(
        charge_point_id="CP_TX_AUTO",
        connector_id=1,
        id_tag="TAG",
        meter_start=0,
        timestamp=utc_now_iso(),
    )
    assert session.ocpp_transaction_id == session.id


@pytest.mark.asyncio
async def test_live_payload_includes_soc_and_charging_speed(db_session) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_METRICS", vendor="V", model="M"
    )
    await ChargerService(db_session).update_status("CP_METRICS", "Available", connector_id=1)
    session = await SessionService(db_session).start_transaction(
        charge_point_id="CP_METRICS",
        connector_id=1,
        id_tag="TAG",
        meter_start=1000,
        timestamp=utc_now_iso(),
    )
    assert session.ocpp_transaction_id is not None

    await SessionService(db_session).record_meter_values(
        charge_point_id="CP_METRICS",
        connector_id=1,
        transaction_id=session.ocpp_transaction_id,
        meter_value=[
            {
                "timestamp": utc_now_iso(),
                "sampledValue": [
                    {
                        "value": "2500",
                        "measurand": "Energy.Active.Import.Register",
                        "unit": "Wh",
                    },
                    {"value": "64", "measurand": "SoC", "unit": "Percent"},
                    {
                        "value": "7.2",
                        "measurand": "Power.Active.Import",
                        "unit": "kW",
                    },
                ],
            }
        ],
    )

    payload = await LiveStatusService(db_session).build_timed_live_payload()
    match = next(
        c
        for charger in payload["chargers"]
        if charger["charger_id"] == "CP_METRICS"
        for c in charger["connectors"]
        if c["number"] == 1
    )
    assert match["transaction_id"] == session.ocpp_transaction_id
    assert match["battery"] == 64.0
    assert match["charging_speed_kw"] == 7.2
    assert match["total_energy_delivered_kwh"] == 1.5


@pytest.mark.asyncio
async def test_live_payload_keeps_transaction_id_during_grace_after_stop(
    db_session,
) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_STOP_TX", vendor="V", model="M"
    )
    await ChargerService(db_session).update_status("CP_STOP_TX", "Available", connector_id=1)
    session = await SessionService(db_session).start_transaction(
        charge_point_id="CP_STOP_TX",
        connector_id=1,
        id_tag="TAG",
        meter_start=0,
        timestamp=utc_now_iso(),
    )
    assert session.ocpp_transaction_id is not None

    await SessionService(db_session).stop_transaction(
        charge_point_id="CP_STOP_TX",
        transaction_id=session.ocpp_transaction_id,
        meter_stop=10,
        timestamp=utc_now_iso(),
        connector_id=1,
    )

    service = LiveStatusService(db_session)
    first = await service.build_timed_live_payload()
    match = next(
        c
        for charger in first["chargers"]
        if charger["charger_id"] == "CP_STOP_TX"
        for c in charger["connectors"]
        if c["number"] == 1
    )
    assert match["status"] == "Available"
    assert match["transaction_id"] == session.ocpp_transaction_id

    second = await service.build_timed_live_payload()
    match = next(
        c
        for charger in second["chargers"]
        if charger["charger_id"] == "CP_STOP_TX"
        for c in charger["connectors"]
        if c["number"] == 1
    )
    assert match["status"] == "Available"
    assert match["transaction_id"] == session.ocpp_transaction_id

    grace = get_settings().evpoint_live_tx_grace_seconds
    expired = await service.build_timed_live_payload(now=utc_now() + timedelta(seconds=grace + 1))
    match = next(
        c
        for charger in expired["chargers"]
        if charger["charger_id"] == "CP_STOP_TX"
        for c in charger["connectors"]
        if c["number"] == 1
    )
    assert match["status"] == "Available"
    assert match["transaction_id"] is None


@pytest.mark.asyncio
async def test_preparing_push_skipped_without_transaction_id(db_session, monkeypatch) -> None:
    published: list[tuple] = []

    async def capture_publish(_self, event_type, payload):
        published.append((event_type, payload))
        return "1-0"

    monkeypatch.setattr(
        "services.charger_service.get_publisher",
        lambda: type("P", (), {"publish": capture_publish})(),
    )

    await ChargerService(db_session).register_boot(
        charge_point_id="CP_PREP_SKIP", vendor="V", model="M"
    )
    await ChargerService(db_session).update_status("CP_PREP_SKIP", "Preparing", connector_id=1)

    assert published == []


@pytest.mark.asyncio
async def test_preparing_push_includes_pending_transaction_id(db_session, monkeypatch) -> None:
    published: list[tuple] = []

    async def capture_publish(_self, event_type, payload):
        published.append((event_type, payload))
        return "1-0"

    monkeypatch.setattr(
        "services.charger_service.get_publisher",
        lambda: type("P", (), {"publish": capture_publish})(),
    )

    await ChargerService(db_session).register_boot(
        charge_point_id="CP_PREP_TX", vendor="V", model="M"
    )
    await get_connection_state().set_pending_remote_start(
        "CP_PREP_TX",
        1,
        id_tag="TAG",
        transaction_id=424242,
    )

    await ChargerService(db_session).update_status("CP_PREP_TX", "Preparing", connector_id=1)

    assert len(published) == 1
    _, payload = published[0]
    assert payload["status"] == "Preparing"
    assert payload["ocpp_transaction_id"] == 424242


def test_should_skip_preparing_status_push_only_without_tx() -> None:
    assert should_skip_preparing_status_push("Preparing", None) is True
    assert should_skip_preparing_status_push("Preparing", 42) is False
    assert should_skip_preparing_status_push("Charging", None) is False


@pytest.mark.asyncio
async def test_live_payload_includes_pending_transaction_id_while_preparing(
    db_session,
) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_LIVE_PREP", vendor="V", model="M"
    )
    await ChargerService(db_session).update_status("CP_LIVE_PREP", "Preparing", connector_id=1)
    await get_connection_state().set_pending_remote_start(
        "CP_LIVE_PREP",
        1,
        id_tag="TAG",
        transaction_id=777001,
    )

    payload = await LiveStatusService(db_session).build_timed_live_payload()
    match = next(
        c
        for charger in payload["chargers"]
        if charger["charger_id"] == "CP_LIVE_PREP"
        for c in charger["connectors"]
        if c["number"] == 1
    )
    assert match["status"] == "Preparing"
    assert match["transaction_id"] == 777001


@pytest.mark.asyncio
async def test_live_payload_finishing_maps_to_available_with_transaction_id(
    db_session,
) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_LIVE_FIN", vendor="V", model="M"
    )
    session = await SessionService(db_session).start_transaction(
        charge_point_id="CP_LIVE_FIN",
        connector_id=1,
        id_tag="TAG",
        meter_start=0,
        timestamp=utc_now_iso(),
    )
    assert session.ocpp_transaction_id is not None
    await SessionService(db_session).stop_transaction(
        charge_point_id="CP_LIVE_FIN",
        transaction_id=session.ocpp_transaction_id,
        meter_stop=10,
        timestamp=utc_now_iso(),
        connector_id=1,
    )
    await ChargerService(db_session).update_status("CP_LIVE_FIN", "Finishing", connector_id=1)

    payload = await LiveStatusService(db_session).build_timed_live_payload()
    match = next(
        c
        for charger in payload["chargers"]
        if charger["charger_id"] == "CP_LIVE_FIN"
        for c in charger["connectors"]
        if c["number"] == 1
    )
    assert match["status"] == "Available"
    assert match["transaction_id"] == session.ocpp_transaction_id


@pytest.mark.asyncio
async def test_live_payload_keeps_suspended_ev_with_transaction_id(db_session) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_LIVE_SEV", vendor="V", model="M"
    )
    session = await SessionService(db_session).start_transaction(
        charge_point_id="CP_LIVE_SEV",
        connector_id=1,
        id_tag="TAG",
        meter_start=0,
        timestamp=utc_now_iso(),
    )
    assert session.ocpp_transaction_id is not None
    await ChargerService(db_session).update_status("CP_LIVE_SEV", "SuspendedEV", connector_id=1)

    payload = await LiveStatusService(db_session).build_timed_live_payload()
    match = next(
        c
        for charger in payload["chargers"]
        if charger["charger_id"] == "CP_LIVE_SEV"
        for c in charger["connectors"]
        if c["number"] == 1
    )
    assert match["status"] == "SuspendedEv"
    assert match["transaction_id"] == session.ocpp_transaction_id


@pytest.mark.asyncio
async def test_live_payload_remaps_stale_charging_without_transaction_id(
    db_session,
) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_STALE_CH", vendor="V", model="M"
    )
    await ChargerService(db_session).update_status("CP_STALE_CH", "Charging", connector_id=1)

    payload = await LiveStatusService(db_session).build_timed_live_payload()
    match = next(
        c
        for charger in payload["chargers"]
        if charger["charger_id"] == "CP_STALE_CH"
        for c in charger["connectors"]
        if c["number"] == 1
    )
    assert match["status"] == "Available"
    assert match["transaction_id"] is None
