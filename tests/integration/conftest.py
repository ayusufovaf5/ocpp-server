from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
import uvicorn
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import db as db_module
from config import get_settings
from db import Base
from ocpp16.app import create_ocpp_app

from .ws_client import SimulatedChargePoint


@pytest.fixture(autouse=True)
def _disable_evpoint_http_in_integration(monkeypatch):
    monkeypatch.setenv(
        "EVPOINT_LIVE_UPDATE_URL",
        "http://127.0.0.1:9/evpoint-disabled-in-tests",
    )
    monkeypatch.setenv("EVPOINT_PUSH_MAX_ATTEMPTS", "1")
    monkeypatch.setenv("EVPOINT_PUSH_TIMEOUT_SECONDS", "0.1")
    monkeypatch.setenv("EVPOINT_PUSH_BACKOFF_SECONDS", "0")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


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


@pytest.fixture
async def db_engine(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "integration.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path.as_posix()}")
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    monkeypatch.setattr(db_module, "engine", engine)
    monkeypatch.setattr(db_module, "async_session_factory", session_factory)

    yield engine

    await engine.dispose()


@pytest.fixture
async def ocpp_ws_server(db_engine, unused_tcp_port):
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
    await asyncio.wait_for(task, timeout=10.0)


@pytest.fixture
async def connect_cp(ocpp_ws_server):
    clients: list[SimulatedChargePoint] = []

    async def _connect(charge_point_id: str) -> SimulatedChargePoint:
        cp = await SimulatedChargePoint.connect(ocpp_ws_server, charge_point_id)
        clients.append(cp)
        return cp

    yield _connect

    for cp in clients:
        try:
            await cp.close()
        except Exception:
            pass
