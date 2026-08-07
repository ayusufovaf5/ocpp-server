from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog

from config import get_settings
from state.redis_client import get_redis

logger = structlog.get_logger(__name__)

DEFAULT_TTL_SECONDS = 24 * 60 * 60


@dataclass(frozen=True)
class PendingRemoteStart:
    id_tag: str
    transaction_id: int


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _to_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class ConnectionState:
    def __init__(self, *, ttl_seconds: int = DEFAULT_TTL_SECONDS) -> None:
        self._ttl = ttl_seconds

    def _profile_key(self, charge_point_id: str) -> str:
        return f"cp:{charge_point_id}:profile"

    def _connector_key(self, charge_point_id: str, connector_id: int) -> str:
        return f"cp:{charge_point_id}:connector:{connector_id}"

    def _connected_key(self, charge_point_id: str) -> str:
        return f"cp:{charge_point_id}:connected"

    async def set_profile(
        self,
        charge_point_id: str,
        *,
        vendor: str,
        model: str,
        firmware_version: str | None = None,
    ) -> None:
        try:
            client = await get_redis()
            payload = {
                "vendor": vendor,
                "model": model,
                "firmware_version": firmware_version,
            }
            key = self._profile_key(charge_point_id)
            await client.set(key, json.dumps(payload), ex=self._ttl)
        except Exception:
            logger.exception("state.set_profile_failed", charge_point_id=charge_point_id)

    async def get_profile(self, charge_point_id: str) -> dict[str, Any] | None:
        try:
            client = await get_redis()
            raw = await client.get(self._profile_key(charge_point_id))
            if raw is None:
                return None
            return json.loads(raw)
        except Exception:
            logger.exception("state.get_profile_failed", charge_point_id=charge_point_id)
            return None

    async def set_connector_status(
        self,
        charge_point_id: str,
        connector_id: int,
        status: str,
    ) -> None:
        try:
            client = await get_redis()
            key = self._connector_key(charge_point_id, connector_id)
            data = await self._load_connector(client, key)
            data["status"] = status
            await client.set(key, json.dumps(data), ex=self._ttl)
        except Exception:
            logger.exception(
                "state.set_connector_status_failed",
                charge_point_id=charge_point_id,
                connector_id=connector_id,
            )

    async def set_active_session(
        self,
        charge_point_id: str,
        connector_id: int,
        session_id: int,
    ) -> None:
        try:
            client = await get_redis()
            key = self._connector_key(charge_point_id, connector_id)
            data = await self._load_connector(client, key)
            data["active_session_id"] = session_id
            await client.set(key, json.dumps(data), ex=self._ttl)
        except Exception:
            logger.exception(
                "state.set_active_session_failed",
                charge_point_id=charge_point_id,
                connector_id=connector_id,
            )

    async def clear_active_session(self, charge_point_id: str, connector_id: int) -> None:
        try:
            client = await get_redis()
            key = self._connector_key(charge_point_id, connector_id)
            data = await self._load_connector(client, key)
            data.pop("active_session_id", None)
            await client.set(key, json.dumps(data), ex=self._ttl)
        except Exception:
            logger.exception(
                "state.clear_active_session_failed",
                charge_point_id=charge_point_id,
                connector_id=connector_id,
            )

    async def set_pending_remote_start(
        self,
        charge_point_id: str,
        connector_id: int,
        *,
        id_tag: str,
        transaction_id: int,
    ) -> None:
        try:
            client = await get_redis()
            key = self._connector_key(charge_point_id, connector_id)
            data = await self._load_connector(client, key)
            data["pending_remote_start"] = {
                "id_tag": id_tag,
                "transaction_id": int(transaction_id),
            }
            data.pop("stopped_ocpp_transaction_id_for_live", None)
            data.pop("stopped_ocpp_transaction_available_since", None)
            await client.set(key, json.dumps(data), ex=self._ttl)
        except Exception:
            logger.exception(
                "state.set_pending_remote_start_failed",
                charge_point_id=charge_point_id,
                connector_id=connector_id,
            )

    async def peek_pending_remote_start(
        self,
        charge_point_id: str,
        connector_id: int,
    ) -> PendingRemoteStart | None:
        try:
            client = await get_redis()
            key = self._connector_key(charge_point_id, connector_id)
            data = await self._load_connector(client, key)
            raw = data.get("pending_remote_start")
            if not isinstance(raw, dict):
                return None
            tx = raw.get("transaction_id")
            if tx is None:
                return None
            return PendingRemoteStart(
                id_tag=str(raw.get("id_tag") or ""),
                transaction_id=int(tx),
            )
        except Exception:
            logger.exception(
                "state.peek_pending_remote_start_failed",
                charge_point_id=charge_point_id,
                connector_id=connector_id,
            )
            return None

    async def take_pending_remote_start(
        self,
        charge_point_id: str,
        connector_id: int,
    ) -> PendingRemoteStart | None:
        try:
            client = await get_redis()
            key = self._connector_key(charge_point_id, connector_id)
            data = await self._load_connector(client, key)
            raw = data.pop("pending_remote_start", None)
            if raw is None:
                return None
            await client.set(key, json.dumps(data), ex=self._ttl)
            if not isinstance(raw, dict) or raw.get("transaction_id") is None:
                return None
            return PendingRemoteStart(
                id_tag=str(raw.get("id_tag") or ""),
                transaction_id=int(raw["transaction_id"]),
            )
        except Exception:
            logger.exception(
                "state.take_pending_remote_start_failed",
                charge_point_id=charge_point_id,
                connector_id=connector_id,
            )
            return None

    async def set_stopped_ocpp_transaction_for_live(
        self,
        charge_point_id: str,
        connector_id: int,
        ocpp_transaction_id: int,
        *,
        available_since: datetime | None = None,
    ) -> None:
        try:
            client = await get_redis()
            key = self._connector_key(charge_point_id, connector_id)
            data = await self._load_connector(client, key)
            data["stopped_ocpp_transaction_id_for_live"] = int(ocpp_transaction_id)
            since = available_since or datetime.now(UTC)
            data["stopped_ocpp_transaction_available_since"] = _to_iso(since)
            await client.set(key, json.dumps(data), ex=self._ttl)
        except Exception:
            logger.exception(
                "state.set_stopped_ocpp_transaction_for_live_failed",
                charge_point_id=charge_point_id,
                connector_id=connector_id,
            )

    async def peek_stopped_ocpp_transaction_for_live(
        self,
        charge_point_id: str,
        connector_id: int,
        *,
        now: datetime | None = None,
        grace_seconds: int | None = None,
    ) -> int | None:
        try:
            client = await get_redis()
            key = self._connector_key(charge_point_id, connector_id)
            data = await self._load_connector(client, key)
            raw = data.get("stopped_ocpp_transaction_id_for_live")
            if raw is None:
                return None

            when = now or datetime.now(UTC)
            if when.tzinfo is None:
                when = when.replace(tzinfo=UTC)
            grace = (
                get_settings().evpoint_live_tx_grace_seconds
                if grace_seconds is None
                else grace_seconds
            )
            since = _parse_iso(data.get("stopped_ocpp_transaction_available_since"))
            if since is None:
                data["stopped_ocpp_transaction_available_since"] = _to_iso(when)
                await client.set(key, json.dumps(data), ex=self._ttl)
                return int(raw)

            if when - since > timedelta(seconds=grace):
                data.pop("stopped_ocpp_transaction_id_for_live", None)
                data.pop("stopped_ocpp_transaction_available_since", None)
                await client.set(key, json.dumps(data), ex=self._ttl)
                return None

            return int(raw)
        except Exception:
            logger.exception(
                "state.peek_stopped_ocpp_transaction_for_live_failed",
                charge_point_id=charge_point_id,
                connector_id=connector_id,
            )
            return None

    async def clear_stopped_ocpp_transaction_for_live(
        self,
        charge_point_id: str,
        connector_id: int,
    ) -> None:
        try:
            client = await get_redis()
            key = self._connector_key(charge_point_id, connector_id)
            data = await self._load_connector(client, key)
            changed = False
            if "stopped_ocpp_transaction_id_for_live" in data:
                data.pop("stopped_ocpp_transaction_id_for_live", None)
                changed = True
            if "stopped_ocpp_transaction_available_since" in data:
                data.pop("stopped_ocpp_transaction_available_since", None)
                changed = True
            if not changed:
                return
            await client.set(key, json.dumps(data), ex=self._ttl)
        except Exception:
            logger.exception(
                "state.clear_stopped_ocpp_transaction_for_live_failed",
                charge_point_id=charge_point_id,
                connector_id=connector_id,
            )

    async def get_connector(
        self,
        charge_point_id: str,
        connector_id: int,
    ) -> dict[str, Any] | None:
        try:
            client = await get_redis()
            raw = await client.get(self._connector_key(charge_point_id, connector_id))
            if raw is None:
                return None
            return json.loads(raw)
        except Exception:
            logger.exception(
                "state.get_connector_failed",
                charge_point_id=charge_point_id,
                connector_id=connector_id,
            )
            return None

    async def mark_connected(self, charge_point_id: str) -> None:
        try:
            client = await get_redis()
            await client.set(self._connected_key(charge_point_id), "1", ex=self._ttl)
        except Exception:
            logger.exception("state.mark_connected_failed", charge_point_id=charge_point_id)

    async def mark_disconnected(self, charge_point_id: str) -> None:
        try:
            client = await get_redis()
            await client.delete(self._connected_key(charge_point_id))
        except Exception:
            logger.exception(
                "state.mark_disconnected_failed",
                charge_point_id=charge_point_id,
            )

    async def is_connected(self, charge_point_id: str) -> bool:
        try:
            client = await get_redis()
            return bool(await client.exists(self._connected_key(charge_point_id)))
        except Exception:
            logger.exception("state.is_connected_failed", charge_point_id=charge_point_id)
            return False

    async def _load_connector(self, client, key: str) -> dict[str, Any]:
        raw = await client.get(key)
        if raw is None:
            return {}
        return json.loads(raw)


_connection_state: ConnectionState | None = None


def get_connection_state() -> ConnectionState:
    global _connection_state
    if _connection_state is None:
        settings = get_settings()
        _connection_state = ConnectionState(ttl_seconds=settings.redis_state_ttl_seconds)
    return _connection_state


def set_connection_state(state: ConnectionState | None) -> None:
    global _connection_state
    _connection_state = state
