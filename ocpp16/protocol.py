from __future__ import annotations

import asyncio
import json
import uuid
from enum import IntEnum
from typing import Any

from config import get_settings
from services.errors import ChargerCallError, ChargerOfflineError, ChargerTimeoutError
from state.connection_registry import get_connection_registry


class MessageType(IntEnum):
    CALL = 2
    CALLRESULT = 3
    CALLERROR = 4


_pending: dict[str, tuple[str, asyncio.Future[Any]]] = {}


def parse_frame(raw: str | bytes) -> tuple[int, str, str | None, Any]:
    data = json.loads(raw)
    if not isinstance(data, list) or len(data) < 3:
        raise ValueError("Invalid OCPP-J frame")
    message_type = int(data[0])
    unique_id = str(data[1])
    if message_type == MessageType.CALL:
        if len(data) != 4:
            raise ValueError("Invalid CALL frame")
        return message_type, unique_id, str(data[2]), data[3]
    if message_type == MessageType.CALLRESULT:
        return message_type, unique_id, None, data[2]
    if message_type == MessageType.CALLERROR:
        return message_type, unique_id, None, data[2:]
    raise ValueError(f"Unsupported message type: {message_type}")


def call_frame(unique_id: str, action: str, payload: dict[str, Any]) -> str:
    return json.dumps(
        [MessageType.CALL, unique_id, action, payload],
        separators=(",", ":"),
    )


def call_result(unique_id: str, payload: dict[str, Any]) -> str:
    return json.dumps([MessageType.CALLRESULT, unique_id, payload], separators=(",", ":"))


def call_error(
    unique_id: str,
    error_code: str,
    error_description: str,
    error_details: dict[str, Any] | None = None,
) -> str:
    return json.dumps(
        [
            MessageType.CALLERROR,
            unique_id,
            error_code,
            error_description,
            error_details or {},
        ],
        separators=(",", ":"),
    )


def resolve_outbound_response(unique_id: str, message_type: int, payload: Any) -> bool:
    entry = _pending.pop(unique_id, None)
    if entry is None:
        return False
    _charge_point_id, future = entry
    if future.done():
        return True
    if message_type == MessageType.CALLRESULT:
        future.set_result(payload if isinstance(payload, dict) else {})
    elif message_type == MessageType.CALLERROR:
        parts = payload if isinstance(payload, list) else [str(payload), "", {}]
        error_code = str(parts[0]) if len(parts) > 0 else "InternalError"
        error_description = str(parts[1]) if len(parts) > 1 else ""
        error_details = parts[2] if len(parts) > 2 and isinstance(parts[2], dict) else {}
        future.set_exception(
            ChargerCallError(
                error_code=error_code,
                error_description=error_description,
                error_details=error_details,
            )
        )
    else:
        future.set_exception(
            ChargerCallError(error_code="InternalError", error_description="Unexpected frame")
        )
    return True


def fail_pending_for_charge_point(charge_point_id: str, exc: BaseException) -> None:
    for unique_id, (cp_id, future) in list(_pending.items()):
        if cp_id != charge_point_id or future.done():
            continue
        _pending.pop(unique_id, None)
        future.set_exception(exc)


def clear_pending_outbound() -> None:
    for _unique_id, (_cp_id, future) in list(_pending.items()):
        if not future.done():
            future.cancel()
    _pending.clear()


async def call(
    charge_point_id: str,
    action: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout_seconds: float | None = None,
) -> dict[str, Any]:
    websocket = get_connection_registry().get(charge_point_id)
    if websocket is None:
        raise ChargerOfflineError(charge_point_id)

    settings = get_settings()
    timeout = settings.outbound_call_timeout_seconds if timeout_seconds is None else timeout_seconds
    unique_id = uuid.uuid4().hex
    loop = asyncio.get_running_loop()
    future: asyncio.Future[dict[str, Any]] = loop.create_future()
    _pending[unique_id] = (charge_point_id, future)

    try:
        await websocket.send_text(call_frame(unique_id, action, payload or {}))
        return await asyncio.wait_for(future, timeout=timeout)
    except TimeoutError as exc:
        _pending.pop(unique_id, None)
        if not future.done():
            future.cancel()
        raise ChargerTimeoutError(charge_point_id, action, timeout) from exc
    except asyncio.CancelledError:
        _pending.pop(unique_id, None)
        raise
    except Exception:
        _pending.pop(unique_id, None)
        if not future.done():
            future.cancel()
        raise
