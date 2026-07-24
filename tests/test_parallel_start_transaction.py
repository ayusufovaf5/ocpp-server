from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from db import Base
from db.models import ChargingSession
from db.time import utc_now_iso
from ocpp16.handler import Ocpp16Handler
from repositories.session_repository import SessionRepository
from services.charger_service import ChargerService


def _call_frame(unique_id: str, action: str, payload: dict) -> str:
    return json.dumps([2, unique_id, action, payload])


async def _run_parallel_start_once(
    db_path: Path, charge_point_id: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{db_path.as_posix()}",
        connect_args={"timeout": 30},
    )
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with factory() as session:
        await ChargerService(session).register_boot(
            charge_point_id=charge_point_id, vendor="V", model="M"
        )

    barrier = asyncio.Barrier(2)
    original_get = SessionRepository.get_active_by_charger_connector

    async def gated_get(self, charger_id: int, connector_id: int):
        row = await original_get(self, charger_id, connector_id)
        if row is None:
            await barrier.wait()
        return row

    monkeypatch.setattr(SessionRepository, "get_active_by_charger_connector", gated_get)

    now = utc_now_iso()

    async def start_one(unique_id: str, id_tag: str) -> dict:
        async with factory() as session:
            handler = Ocpp16Handler(charge_point_id, session)
            raw = await handler.handle_raw(
                _call_frame(
                    unique_id,
                    "StartTransaction",
                    {
                        "connectorId": 1,
                        "idTag": id_tag,
                        "meterStart": 0,
                        "timestamp": now,
                    },
                )
            )
            return json.loads(raw)

    first, second = await asyncio.gather(
        start_one("u1", "TAG_A"),
        start_one("u2", "TAG_B"),
    )

    for frame in (first, second):
        assert frame[0] == 3, frame
        assert frame[2]["idTagInfo"]["status"] == "Accepted"
        assert isinstance(frame[2]["transactionId"], int)

    assert first[2]["transactionId"] == second[2]["transactionId"]

    async with factory() as session:
        charger = await ChargerService(session).get(charge_point_id)
        assert charger is not None
        active_count = (
            await session.execute(
                select(func.count())
                .select_from(ChargingSession)
                .where(
                    ChargingSession.charger_id == charger.id,
                    ChargingSession.connector_id == 1,
                    ChargingSession.status == "Active",
                )
            )
        ).scalar_one()
        assert active_count == 1

    await engine.dispose()


@pytest.mark.asyncio
async def test_parallel_start_transaction_single_active_session(tmp_path, monkeypatch) -> None:
    for i in range(20):
        await _run_parallel_start_once(tmp_path / f"race_{i}.db", f"CP_RACE_{i}", monkeypatch)
