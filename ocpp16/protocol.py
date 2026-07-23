import json
from enum import IntEnum
from typing import Any


class MessageType(IntEnum):
    CALL = 2
    CALLRESULT = 3
    CALLERROR = 4


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
