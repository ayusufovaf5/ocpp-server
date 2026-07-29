from __future__ import annotations

import json

import pytest

from .ws_client import CALLERROR, new_message_id

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_malformed_frames_return_callerror_and_keep_connection(
    ocpp_ws_server, db_engine, connect_cp
) -> None:
    cp = await connect_cp("INT_CP_BAD")
    assert (await cp.boot())["status"] == "Accepted"

    await cp.send_raw("{not-json")
    bad_json = await cp.recv_frame(timeout=5.0)
    assert bad_json[0] == CALLERROR
    assert bad_json[2] == "FormationViolation"

    await cp.send_raw(json.dumps([2, new_message_id(), "TotallyUnknownAction", {}]))
    unknown = await cp.recv_frame(timeout=5.0)
    assert unknown[0] == CALLERROR
    assert unknown[2] == "NotImplemented"

    await cp.send_raw(json.dumps([2, "shape-id"]))
    wrong_shape = await cp.recv_frame(timeout=5.0)
    assert wrong_shape[0] == CALLERROR
    assert wrong_shape[2] == "FormationViolation"

    hb = await cp.heartbeat()
    assert "currentTime" in hb
