from __future__ import annotations

import asyncio
import json

import pytest
from httpx import ASGITransport, AsyncClient

import db as db_module
from config import DEV_API_KEY, get_settings
from db import get_db_session
from main import create_app
from ocpp16.app import create_ocpp_app
from services.charger_service import ChargerService


@pytest.mark.asyncio
async def test_rest_rejects_missing_api_key() -> None:
    application = create_app()
    transport = ASGITransport(app=application)
    async with AsyncClient(transport=transport, base_url="http://test") as bare:
        response = await bare.get("/version")
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or missing API key"


@pytest.mark.asyncio
async def test_rest_rejects_wrong_api_key() -> None:
    application = create_app()
    transport = ASGITransport(app=application)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/version", headers={"X-API-Key": "wrong-key"})
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_rest_accepts_valid_api_key(client) -> None:
    response = await client.get("/version")
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_health_remains_public() -> None:
    application = create_app()

    async def _fake_db():
        from unittest.mock import AsyncMock, MagicMock

        session = MagicMock()
        session.execute = AsyncMock(return_value=None)
        yield session

    application.dependency_overrides[get_db_session] = _fake_db
    transport = ASGITransport(app=application)
    async with AsyncClient(transport=transport, base_url="http://test") as bare:
        response = await bare.get("/health")
    assert response.status_code == 200
    application.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_charger_status_requires_api_key(db_engine) -> None:
    application = create_app()

    async def _db():
        async with db_module.async_session_factory() as session:
            yield session

    application.dependency_overrides[get_db_session] = _db
    transport = ASGITransport(app=application)
    async with AsyncClient(transport=transport, base_url="http://test") as bare:
        denied = await bare.get("/chargers/CP_X/status")
        assert denied.status_code == 401

        async with db_module.async_session_factory() as session:
            await ChargerService(session).register_boot(
                charge_point_id="CP_X", vendor="V", model="M"
            )
        ok = await bare.get(
            "/chargers/CP_X/status", headers={"X-API-Key": DEV_API_KEY}
        )
        assert ok.status_code == 200
    application.dependency_overrides.clear()


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
async def ocpp_server_restricted(db_engine, unused_tcp_port, monkeypatch):
    import uvicorn

    monkeypatch.setenv("OCPP_CHARGE_POINT_ALLOWLIST", "CP_ALLOWED")
    get_settings.cache_clear()

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
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_ocpp_ws_rejects_charge_point_not_on_allowlist(
    ocpp_server_restricted,
) -> None:
    import websockets
    from websockets.exceptions import InvalidStatus

    url = f"{ocpp_server_restricted}/ocpp/CP_UNKNOWN"
    with pytest.raises(InvalidStatus):
        async with websockets.connect(url, subprotocols=["ocpp1.6"]):
            pytest.fail("handshake must not succeed for unknown charge_point_id")


@pytest.mark.asyncio
async def test_ocpp_ws_allows_allowlisted_charge_point(
    ocpp_server_restricted, db_session
) -> None:
    import websockets

    await ChargerService(db_session).register_boot(
        charge_point_id="CP_ALLOWED", vendor="V", model="M"
    )
    url = f"{ocpp_server_restricted}/ocpp/CP_ALLOWED"
    async with websockets.connect(url, subprotocols=["ocpp1.6"]) as ws:
        await ws.send(
            json.dumps(
                [
                    2,
                    "1",
                    "BootNotification",
                    {"chargePointVendor": "V", "chargePointModel": "M"},
                ]
            )
        )
        frame = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
        assert frame[0] == 3
