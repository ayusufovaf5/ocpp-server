from __future__ import annotations

import json
from collections.abc import AsyncGenerator

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

import db as db_module
from db import get_db_session
from db.models import ConnectorStatus
from main import create_app
from ocpp16.handler import Ocpp16Handler
from services.charger_service import ChargerService


def _call_frame(unique_id: str, action: str, payload: dict) -> str:
    return json.dumps([2, unique_id, action, payload])


@pytest.fixture
async def api_client(db_engine) -> AsyncGenerator[AsyncClient, None]:
    application = create_app()

    async def _db_session():
        async with db_module.async_session_factory() as session:
            yield session

    application.dependency_overrides[get_db_session] = _db_session
    transport = ASGITransport(app=application)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
    ) as ac:
        yield ac
    application.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_status_notification_dual_writes_per_connector(db_session) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_DUAL",
        vendor="V",
        model="M",
        connector_count=2,
    )
    handler = Ocpp16Handler("CP_DUAL", db_session)

    for uid, connector_id, status in (
        ("s1", 1, "Available"),
        ("s2", 2, "Preparing"),
    ):
        raw = await handler.handle_raw(
            _call_frame(
                uid,
                "StatusNotification",
                {
                    "connectorId": connector_id,
                    "errorCode": "NoError",
                    "status": status,
                },
            )
        )
        frame = json.loads(raw)
        assert frame[0] == 3

    charger = await ChargerService(db_session).get("CP_DUAL")
    assert charger is not None
    assert charger.status == "Preparing"

    rows = (
        (
            await db_session.execute(
                select(ConnectorStatus)
                .where(ConnectorStatus.charger_id == charger.id)
                .order_by(ConnectorStatus.connector_id)
            )
        )
        .scalars()
        .all()
    )
    assert [(r.connector_id, r.status) for r in rows] == [
        (1, "Available"),
        (2, "Preparing"),
    ]

    view = await ChargerService(db_session).get_status_view("CP_DUAL")
    assert view is not None
    assert view.status == "Preparing"
    assert [(c.connector_id, c.status) for c in view.connectors] == [
        (1, "Available"),
        (2, "Preparing"),
    ]


@pytest.mark.asyncio
async def test_status_notification_connector_zero_writes_connector_status_only(
    db_session,
) -> None:
    await ChargerService(db_session).register_boot(charge_point_id="CP_C0", vendor="V", model="M")
    await ChargerService(db_session).update_status("CP_C0", "Charging")
    handler = Ocpp16Handler("CP_C0", db_session)

    raw = await handler.handle_raw(
        _call_frame(
            "z0",
            "StatusNotification",
            {"connectorId": 0, "errorCode": "NoError", "status": "Available"},
        )
    )
    assert json.loads(raw)[0] == 3

    charger = await ChargerService(db_session).get("CP_C0")
    assert charger is not None
    assert charger.status == "Charging"

    row = (
        await db_session.execute(
            select(ConnectorStatus).where(
                ConnectorStatus.charger_id == charger.id,
                ConnectorStatus.connector_id == 0,
            )
        )
    ).scalar_one()
    assert row.status == "Available"

    view = await ChargerService(db_session).get_status_view("CP_C0")
    assert view is not None
    assert view.charge_point_status == "Available"
    assert view.connectors == []
    assert view.status == "Charging"


@pytest.mark.asyncio
async def test_rest_charger_status_returns_per_connector(api_client, db_session) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_API", vendor="V", model="M", connector_count=2
    )
    handler = Ocpp16Handler("CP_API", db_session)
    await handler.handle_raw(
        _call_frame(
            "a1",
            "StatusNotification",
            {"connectorId": 1, "errorCode": "NoError", "status": "Available"},
        )
    )
    await handler.handle_raw(
        _call_frame(
            "a2",
            "StatusNotification",
            {"connectorId": 2, "errorCode": "NoError", "status": "Charging"},
        )
    )

    missing = await api_client.get("/chargers/CP_MISSING/status")
    assert missing.status_code == 404

    response = await api_client.get("/chargers/CP_API/status")
    assert response.status_code == 200
    body = response.json()
    assert body["charge_point_id"] == "CP_API"
    assert body["status"] == "Charging"
    assert body["legacy_status"] == "Charging"
    assert body["charge_point_status"] is None
    assert [(c["connector_id"], c["status"]) for c in body["connectors"]] == [
        (1, "Available"),
        (2, "Charging"),
    ]
