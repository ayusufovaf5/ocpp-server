from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fakeredis import FakeAsyncRedis
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import db as db_module
from config import DEV_API_KEY, get_settings
from db import Base, get_db_session
from events.publisher import EventPublisher, set_publisher
from main import create_app
from state.connection_state import ConnectionState, set_connection_state
from state.redis_client import set_redis


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture(autouse=True)
async def fake_redis():
    client = FakeAsyncRedis(decode_responses=True)
    set_redis(client)
    set_connection_state(ConnectionState(ttl_seconds=3600))
    set_publisher(EventPublisher())
    yield client
    set_publisher(None)
    set_connection_state(None)
    set_redis(None)
    await client.aclose()


@pytest.fixture
def app():
    application = create_app()

    async def _fake_db_session() -> AsyncGenerator[Any, None]:
        session = MagicMock()
        session.execute = AsyncMock(return_value=None)
        yield session

    application.dependency_overrides[get_db_session] = _fake_db_session
    yield application
    application.dependency_overrides.clear()


@pytest.fixture
async def client(app) -> AsyncGenerator[AsyncClient, None]:
    transport = ASGITransport(app=app)
    headers = {"X-API-Key": DEV_API_KEY}
    async with AsyncClient(transport=transport, base_url="http://test", headers=headers) as ac:
        yield ac


@pytest.fixture
def unused_tcp_port() -> int:
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture
async def db_engine(monkeypatch):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    monkeypatch.setattr(db_module, "engine", engine)
    monkeypatch.setattr(db_module, "async_session_factory", session_factory)

    yield engine

    await engine.dispose()


@pytest.fixture
async def db_session(db_engine) -> AsyncGenerator[AsyncSession, None]:
    async with db_module.async_session_factory() as session:
        yield session
