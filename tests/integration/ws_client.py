from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime
from typing import Any

import websockets
from websockets.asyncio.client import ClientConnection

CALL = 2
CALLRESULT = 3
CALLERROR = 4

DEFAULT_RECV_TIMEOUT = 5.0


def utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def new_message_id() -> str:
    return uuid.uuid4().hex[:12]


class SimulatedChargePoint:
    def __init__(self, charge_point_id: str, ws: ClientConnection) -> None:
        self.charge_point_id = charge_point_id
        self.ws = ws

    @classmethod
    async def connect(cls, base_ws_url: str, charge_point_id: str) -> SimulatedChargePoint:
        url = f"{base_ws_url.rstrip('/')}/ocpp/{charge_point_id}"
        ws = await websockets.connect(url, subprotocols=["ocpp1.6"])
        return cls(charge_point_id, ws)

    async def close(self) -> None:
        await self.ws.close()

    async def __aenter__(self) -> SimulatedChargePoint:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    async def call(
        self,
        action: str,
        payload: dict[str, Any],
        *,
        unique_id: str | None = None,
        timeout: float = DEFAULT_RECV_TIMEOUT,
    ) -> dict[str, Any]:
        msg_id = unique_id or new_message_id()
        await self.ws.send(json.dumps([CALL, msg_id, action, payload]))
        raw = await asyncio.wait_for(self.ws.recv(), timeout=timeout)
        frame = json.loads(raw)
        assert frame[0] == CALLRESULT, frame
        assert frame[1] == msg_id, frame
        assert isinstance(frame[2], dict), frame
        return frame[2]

    async def call_expect_error(
        self,
        action: str,
        payload: Any,
        *,
        unique_id: str | None = None,
        timeout: float = DEFAULT_RECV_TIMEOUT,
    ) -> list[Any]:
        msg_id = unique_id or new_message_id()
        await self.ws.send(json.dumps([CALL, msg_id, action, payload]))
        raw = await asyncio.wait_for(self.ws.recv(), timeout=timeout)
        frame = json.loads(raw)
        assert frame[0] == CALLERROR, frame
        assert frame[1] == msg_id, frame
        return frame

    async def send_raw(self, text: str) -> None:
        await self.ws.send(text)

    async def recv_frame(self, *, timeout: float = DEFAULT_RECV_TIMEOUT) -> list[Any]:
        raw = await asyncio.wait_for(self.ws.recv(), timeout=timeout)
        return json.loads(raw)

    async def boot(
        self,
        *,
        vendor: str = "EvPoint",
        model: str = "IntegrationSim",
        firmware_version: str = "1.0.0",
    ) -> dict[str, Any]:
        return await self.call(
            "BootNotification",
            {
                "chargePointVendor": vendor,
                "chargePointModel": model,
                "firmwareVersion": firmware_version,
            },
        )

    async def authorize(self, id_tag: str = "TAG001") -> dict[str, Any]:
        return await self.call("Authorize", {"idTag": id_tag})

    async def status(self, connector_id: int = 1, status: str = "Available") -> dict[str, Any]:
        return await self.call(
            "StatusNotification",
            {
                "connectorId": connector_id,
                "errorCode": "NoError",
                "status": status,
                "timestamp": utc_now_iso(),
            },
        )

    async def start_transaction(
        self,
        *,
        connector_id: int = 1,
        id_tag: str = "TAG001",
        meter_start: int = 1000,
    ) -> dict[str, Any]:
        return await self.call(
            "StartTransaction",
            {
                "connectorId": connector_id,
                "idTag": id_tag,
                "meterStart": meter_start,
                "timestamp": utc_now_iso(),
            },
        )

    async def meter_values(
        self,
        transaction_id: int,
        energy_wh: float,
        *,
        connector_id: int = 1,
    ) -> dict[str, Any]:
        return await self.call(
            "MeterValues",
            {
                "connectorId": connector_id,
                "transactionId": transaction_id,
                "meterValue": [
                    {
                        "timestamp": utc_now_iso(),
                        "sampledValue": [
                            {
                                "value": str(energy_wh),
                                "measurand": "Energy.Active.Import.Register",
                                "unit": "Wh",
                            }
                        ],
                    }
                ],
            },
        )

    async def stop_transaction(
        self,
        transaction_id: int,
        meter_stop: int,
        *,
        reason: str = "Local",
    ) -> dict[str, Any]:
        return await self.call(
            "StopTransaction",
            {
                "transactionId": transaction_id,
                "meterStop": meter_stop,
                "timestamp": utc_now_iso(),
                "reason": reason,
            },
        )

    async def heartbeat(self) -> dict[str, Any]:
        return await self.call("Heartbeat", {})
