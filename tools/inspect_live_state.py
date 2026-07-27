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


async def _scan_connected(redis) -> set[str]:
    online: set[str] = set()
    async for key in redis.scan_iter(match="cp:*:connected"):
        parts = str(key).split(":")
        if len(parts) >= 3 and parts[0] == "cp" and parts[-1] == "connected":
            online.add(":".join(parts[1:-1]))
    return online


async def run() -> int:
    from db import async_session_factory
    from db.models import Charger, ChargingSession, ConnectorStatus
    from state.redis_client import close_redis, get_redis

    redis = await get_redis()
    online = await _scan_connected(redis)

    async with async_session_factory() as db:
        chargers = (
            (
                await db.execute(
                    select(Charger)
                    .options(selectinload(Charger.connector_statuses))
                    .order_by(Charger.charge_point_id)
                )
            )
            .scalars()
            .all()
        )

        active_sessions = (
            (
                await db.execute(
                    select(ChargingSession)
                    .where(ChargingSession.status == "Active")
                    .order_by(ChargingSession.id)
                )
            )
            .scalars()
            .all()
        )
        sessions_by_charger: dict[int, list[ChargingSession]] = {}
        for session in active_sessions:
            sessions_by_charger.setdefault(session.charger_id, []).append(session)

    print("=== Live state ===")
    print(f"Redis online flags: {len(online)}")
    if online:
        print("  " + ", ".join(sorted(online)))
    else:
        print("  (none)")
    print()

    if not chargers:
        print("No chargers in Postgres.")
        await close_redis()
        return 0

    for charger in chargers:
        redis_online = charger.charge_point_id in online
        db_online = charger.disconnected_at is None
        print(f"charger {charger.charge_point_id} (db_id={charger.id})")
        print(
            f"  db_status={charger.status}  redis_online={redis_online}  "
            f"db_connected={db_online}  disconnected_at={charger.disconnected_at}"
        )

        connectors: list[ConnectorStatus] = sorted(
            charger.connector_statuses, key=lambda c: c.connector_id
        )
        if connectors:
            for row in connectors:
                print(f"  connector {row.connector_id}: {row.status} (updated {row.updated_at})")
        else:
            print("  connectors: (none in connector_status)")

        for session in sessions_by_charger.get(charger.id, []):
            print(
                f"  ACTIVE session id={session.id} connector={session.connector_id} "
                f"ocpp_tx={session.ocpp_transaction_id} id_tag={session.id_tag} "
                f"started={session.started_at}"
            )
        if charger.id not in sessions_by_charger:
            print("  active sessions: (none)")
        print()

    print(f"Total active sessions: {len(active_sessions)}")
    await close_redis()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read-only dump of online chargers, connector status, and active sessions."
    )
    parser.parse_args()
    raise SystemExit(asyncio.run(run()))


if __name__ == "__main__":
    main()
