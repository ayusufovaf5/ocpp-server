from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from repositories.charger_repository import ChargerRepository
from repositories.session_repository import SessionRepository
from state.connection_state import get_connection_state


def should_skip_preparing_status_push(status: str, transaction_id: int | None) -> bool:
    if status != "Preparing":
        return False
    if transaction_id is not None:
        return False
    return True


async def resolve_connector_transaction_id(
    db: AsyncSession,
    *,
    charge_point_id: str,
    connector_id: int,
    status: str,
) -> int | None:
    if connector_id == 0:
        return None

    charger = await ChargerRepository(db).get_by_charge_point_id(charge_point_id)
    if charger is None:
        return None

    active = await SessionRepository(db).get_active_by_charger_connector(
        charger.id, connector_id
    )
    if active is not None and active.ocpp_transaction_id is not None:
        return int(active.ocpp_transaction_id)

    if status == "Preparing":
        pending = await get_connection_state().peek_pending_remote_start(
            charge_point_id, connector_id
        )
        if pending is not None and pending.transaction_id > 0:
            return int(pending.transaction_id)

    if status == "Available":
        stopped = await get_connection_state().peek_stopped_ocpp_transaction_for_live(
            charge_point_id, connector_id
        )
        if stopped is not None:
            return int(stopped)

    return None
