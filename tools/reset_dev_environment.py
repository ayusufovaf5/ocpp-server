from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from urllib.parse import urlparse

from sqlalchemy import delete, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_LOCAL_HOSTS = frozenset(
    {
        "localhost",
        "127.0.0.1",
        "::1",
        "postgres",
        "redis",
        "0.0.0.0",
    }
)


def _assert_local_targets() -> None:
    from config import get_settings

    settings = get_settings()
    pg_host = (settings.pg_host or "").strip().lower()
    if pg_host not in _LOCAL_HOSTS:
        raise SystemExit(
            f"Refusing to run: PG_HOST={settings.pg_host!r} is not a local/docker-compose host. "
            f"Allowed: {sorted(_LOCAL_HOSTS)}"
        )

    parsed = urlparse(settings.redis_url)
    redis_host = (parsed.hostname or "").strip().lower()
    if redis_host not in _LOCAL_HOSTS:
        raise SystemExit(
            f"Refusing to run: REDIS_URL host={redis_host!r} is not a local/docker-compose host. "
            f"Allowed: {sorted(_LOCAL_HOSTS)}"
        )

    if settings.database_url:
        db_parsed = urlparse(settings.database_url)
        db_host = (db_parsed.hostname or "").strip().lower()
        if db_host and db_host not in _LOCAL_HOSTS:
            raise SystemExit(
                f"Refusing to run: DATABASE_URL host={db_host!r} is not local. "
                f"Allowed: {sorted(_LOCAL_HOSTS)}"
            )


async def _clear_postgres() -> tuple[int, int, int]:
    from db import async_session_factory
    from db.models import ChargingSession, ConnectorStatus, MeterValue

    async with async_session_factory() as db:
        meters = await db.execute(delete(MeterValue))
        sessions = await db.execute(delete(ChargingSession))
        connectors = await db.execute(delete(ConnectorStatus))
        await db.commit()
        return (
            int(meters.rowcount or 0),
            int(sessions.rowcount or 0),
            int(connectors.rowcount or 0),
        )


async def _clear_redis() -> int:
    from state.redis_client import get_redis

    client = await get_redis()
    deleted = 0
    async for key in client.scan_iter(match="cp:*"):
        deleted += int(await client.delete(key))
    return deleted


async def run() -> int:
    from config import get_settings
    from db import engine
    from state.redis_client import close_redis

    get_settings.cache_clear()
    _assert_local_targets()
    settings = get_settings()

    print("About to wipe LOCAL dev data:")
    print(f"  PG_HOST={settings.pg_host} DB={settings.pg_database}")
    print(f"  REDIS_URL={settings.redis_url}")
    print("  Postgres: DELETE FROM meter_values, sessions, connector_status")
    print("  Redis: DELETE keys matching cp:*")
    print("  chargers table is kept")
    answer = input("Proceed? [y/N] ").strip().lower()
    if answer not in {"y", "yes"}:
        print("Aborted.")
        return 1

    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))

    meters, sessions, connectors = await _clear_postgres()
    redis_deleted = await _clear_redis()
    await close_redis()
    await engine.dispose()

    print(
        f"Done. deleted meter_values={meters} sessions={sessions} "
        f"connector_status={connectors} redis_keys={redis_deleted}"
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Wipe local docker-compose test data (sessions/meter_values/connector_status "
            "and Redis cp:* live-state). Refuses non-local hosts."
        )
    )
    parser.parse_args()
    raise SystemExit(asyncio.run(run()))


if __name__ == "__main__":
    main()
