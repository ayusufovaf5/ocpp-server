from __future__ import annotations

import pytest

from repositories.charger_repository import ChargerRepository
from services.charger_service import ChargerService


@pytest.mark.asyncio
async def test_ensure_connected_creates_row_when_missing(db_session) -> None:
    service = ChargerService(db_session)
    charge_point_id = "CP_ENSURE_NEW"

    assert await ChargerRepository(db_session).get_by_charge_point_id(charge_point_id) is None

    charger = await service.ensure_connected(charge_point_id)

    assert charger.charge_point_id == charge_point_id
    assert charger.disconnected_at is None
    assert charger.status == "Available"

    again = await ChargerRepository(db_session).get_by_charge_point_id(charge_point_id)
    assert again is not None
    assert again.id == charger.id


@pytest.mark.asyncio
async def test_ensure_connected_clears_disconnected_at(db_session) -> None:
    service = ChargerService(db_session)
    charge_point_id = "CP_ENSURE_RE"
    await service.register_boot(charge_point_id=charge_point_id, vendor="V", model="M")
    await service.mark_disconnected(charge_point_id)

    row = await ChargerRepository(db_session).get_by_charge_point_id(charge_point_id)
    assert row is not None
    assert row.disconnected_at is not None

    restored = await service.ensure_connected(charge_point_id)
    assert restored.disconnected_at is None
