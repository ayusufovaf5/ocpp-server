from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from db import get_db_session
from main import create_app


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
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
