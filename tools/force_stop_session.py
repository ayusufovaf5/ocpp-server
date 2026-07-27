from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import selectinload

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

END_REASON = "manual_ops_intervention"


async def _find_session(
    *,
    session_id: int | None,
    charger_id: str | None,
    connector_id: int | None,
) -> tuple[int, str, int, int | None, str, object]:
    from db import async_session_factory
    from db.models import Charger, ChargingSession
    from repositories.session_repository import SessionRepository

    async with async_session_factory() as db:
        if session_id is not None:
            row = (
                await db.execute(
                    select(ChargingSession)
                    .options(selectinload(ChargingSession.charger))
                    .where(ChargingSession.id == session_id)
                )
            ).scalar_one_or_none()
            if row is None:
                raise SystemExit(f"Session id={session_id} not found")
            if row.status != "Active":
                raise SystemExit(f"Session id={session_id} is {row.status}, not Active")
            return (
                row.id,
                row.charger.charge_point_id,
                row.connector_id,
                row.ocpp_transaction_id,
                row.id_tag,
                row.started_at,
            )

        assert charger_id is not None and connector_id is not None
        charger = (
            await db.execute(select(Charger).where(Charger.charge_point_id == charger_id))
        ).scalar_one_or_none()
        if charger is None:
            raise SystemExit(f"Charger ocpp id={charger_id!r} not found")
        row = await SessionRepository(db).get_active_by_charger_connector(charger.id, connector_id)
        if row is None:
            raise SystemExit(
                f"No Active session for charger={charger_id!r} connector={connector_id}"
            )
        return (
            row.id,
            charger.charge_point_id,
            row.connector_id,
            row.ocpp_transaction_id,
            row.id_tag,
            row.started_at,
        )


async def _force_stop(session_id: int, charge_point_id: str) -> None:
    from db import async_session_factory
    from db.models import ChargingSession
    from db.time import utc_now
    from repositories.session_repository import SessionRepository
    from state.connection_state import get_connection_state

    async with async_session_factory() as db:
        row = await db.get(ChargingSession, session_id)
        assert row is not None
        repo = SessionRepository(db)
        latest = await repo.latest_meter_value(row.id)
        if latest is not None:
            meter_stop = int(round(latest.value))
        else:
            meter_stop = int(row.meter_start)

        await repo.stop(
            row,
            stopped_at=utc_now(),
            meter_stop=meter_stop,
            end_reason=END_REASON,
            meter_stop_estimated=True,
        )
        await db.commit()

        await get_connection_state().clear_active_session(charge_point_id, row.connector_id)
        print(
            f"Closed session id={row.id} charger={charge_point_id} "
            f"connector={row.connector_id} end_reason={END_REASON} "
            f"meter_stop={meter_stop}"
        )


async def run(args: argparse.Namespace) -> int:
    from state.redis_client import close_redis

    (
        session_id,
        charge_point_id,
        connector_id,
        ocpp_tx,
        id_tag,
        started_at,
    ) = await _find_session(
        session_id=args.session_id,
        charger_id=args.charger_id,
        connector_id=args.connector_id,
    )

    print("About to force-close Active session:")
    print(f"  session_id={session_id}")
    print(f"  charger={charge_point_id}")
    print(f"  connector_id={connector_id}")
    print(f"  ocpp_transaction_id={ocpp_tx}")
    print(f"  id_tag={id_tag}")
    print(f"  started_at={started_at}")
    print(f"  end_reason will be set to {END_REASON!r}")
    answer = input("Proceed? [y/N] ").strip().lower()
    if answer not in {"y", "yes"}:
        print("Aborted.")
        await close_redis()
        return 1

    await _force_stop(session_id, charge_point_id)
    await close_redis()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Force-complete a stuck Active charging session in Postgres "
            f"(end_reason={END_REASON!r})."
        )
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--session-id", type=int, help="Postgres sessions.id")
    group.add_argument(
        "--charger-id",
        help="OCPP charge_point_id (use with --connector-id)",
    )
    parser.add_argument(
        "--connector-id",
        type=int,
        help="Connector number (required with --charger-id)",
    )
    args = parser.parse_args()
    if args.charger_id is not None and args.connector_id is None:
        parser.error("--connector-id is required with --charger-id")
    if args.session_id is not None and args.connector_id is not None:
        parser.error("--connector-id cannot be used with --session-id")
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
