from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
import uvicorn
from sqlalchemy import select

import db as db_module
from config import get_settings
from db.models import Charger
from ocpp16.app import create_ocpp_app
from services.charger_service import ChargerService

from .ws_client import SimulatedChargePoint

pytestmark = pytest.mark.integration


async def _wait_port(host: str, port: int, timeout: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        try:
            _reader, writer = await asyncio.open_connection(host, port)
            writer.close()
            await writer.wait_closed()
            return
        except OSError:
            if loop.time() > deadline:
                raise TimeoutError(f"Server not ready on {host}:{port}") from None
            await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_heartbeat_interval_and_expectation(monkeypatch, db_engine, unused_tcp_port) -> None:
    monkeypatch.setenv("OCPP_HEARTBEAT_INTERVAL", "7")
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

    base = f"ws://127.0.0.1:{unused_tcp_port}"
    cp = await SimulatedChargePoint.connect(base, "INT_CP_HB")
    try:
        boot = await cp.boot()
        assert boot["status"] == "Accepted"
        assert boot["interval"] == 7

        before = datetime.now(UTC)
        hb = await cp.heartbeat()
        assert "currentTime" in hb

        async with db_module.async_session_factory() as db:
            charger = (
                await db.execute(select(Charger).where(Charger.charge_point_id == "INT_CP_HB"))
            ).scalar_one()
            assert charger.last_heartbeat is not None
            last_hb = charger.last_heartbeat
            if last_hb.tzinfo is None:
                last_hb = last_hb.replace(tzinfo=UTC)
            assert last_hb >= before - timedelta(seconds=2)

        async with db_module.async_session_factory() as db:
            charger = (
                await db.execute(select(Charger).where(Charger.charge_point_id == "INT_CP_HB"))
            ).scalar_one()
            charger.last_heartbeat = datetime.now(UTC) - timedelta(seconds=30)
            charger.status = "Available"
            await db.commit()

        async with db_module.async_session_factory() as db:
            marked = await ChargerService(db).mark_stale_unavailable(timeout_seconds=5)
            assert marked == 1
            charger = (
                await db.execute(select(Charger).where(Charger.charge_point_id == "INT_CP_HB"))
            ).scalar_one()
            assert charger.status == "Unavailable"
    finally:
        await cp.close()
        server.should_exit = True
        await asyncio.wait_for(task, timeout=10.0)
        get_settings.cache_clear()
