from __future__ import annotations

import argparse
import asyncio
import json
import logging
import uuid
from datetime import UTC, datetime

import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("cp-sim")

CALL = 2
CALLRESULT = 3
CALLERROR = 4


def _uid() -> str:
    return uuid.uuid4().hex[:12]


def _now() -> str:
    return datetime.now(UTC).isoformat()


class Simulator:
    def __init__(self, charge_point_id: str, ws) -> None:
        self.id = charge_point_id
        self.ws = ws
        self.known_connectors: set[int] = set()
        self.active_tx: dict[int, int] = {}
        self._meter_tasks: dict[int, asyncio.Task] = {}
        self._pending: dict[str, asyncio.Future] = {}
        self._energy_wh: dict[int, float] = {}
        self._soc: dict[int, float] = {}

    async def send_call(self, action: str, payload: dict) -> dict:
        msg_id = _uid()
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[msg_id] = fut
        await self.ws.send(json.dumps([CALL, msg_id, action, payload]))
        return await asyncio.wait_for(fut, timeout=30)

    async def boot(self) -> None:
        resp = await self.send_call(
            "BootNotification",
            {"chargePointVendor": "EvPoint", "chargePointModel": "Sim_22kW"},
        )
        logger.info("BootNotification → %s", resp)

    async def status(self, connector_id: int, status: str) -> None:
        self.known_connectors.add(connector_id)
        await self.send_call(
            "StatusNotification",
            {
                "connectorId": connector_id,
                "status": status,
                "errorCode": "NoError",
                "timestamp": _now(),
            },
        )

    async def start_transaction(self, connector_id: int, id_tag: str) -> None:
        resp = await self.send_call(
            "StartTransaction",
            {
                "connectorId": connector_id,
                "idTag": id_tag,
                "meterStart": 0,
                "timestamp": _now(),
            },
        )
        tx = resp.get("transactionId")
        if tx is not None:
            self.active_tx[connector_id] = int(tx)
            logger.info("StartTransaction OK tx=%s connector=%s", tx, connector_id)

    async def stop_transaction(self, transaction_id: int, connector_id: int) -> None:
        self._stop_meter_loop(connector_id)
        energy = self._energy_wh.get(connector_id, 1000)
        await self.send_call(
            "StopTransaction",
            {
                "transactionId": transaction_id,
                "meterStop": int(energy),
                "timestamp": _now(),
                "reason": "Remote",
            },
        )
        self.active_tx.pop(connector_id, None)
        self._energy_wh.pop(connector_id, None)
        self._soc.pop(connector_id, None)
        logger.info("StopTransaction OK tx=%s", transaction_id)

    async def send_meter_values(self, connector_id: int, transaction_id: int) -> None:
        energy = self._energy_wh.get(connector_id, 0.0)
        soc = self._soc.get(connector_id, 20.0)
        await self.send_call(
            "MeterValues",
            {
                "connectorId": connector_id,
                "transactionId": transaction_id,
                "meterValue": [
                    {
                        "timestamp": _now(),
                        "sampledValue": [
                            {
                                "value": f"{energy:.0f}",
                                "measurand": "Energy.Active.Import.Register",
                                "unit": "Wh",
                            },
                            {
                                "value": "7200",
                                "measurand": "Power.Active.Import",
                                "unit": "W",
                            },
                            {
                                "value": f"{soc:.0f}",
                                "measurand": "SoC",
                                "unit": "Percent",
                            },
                        ],
                    }
                ],
            },
        )
        logger.info(
            "MeterValues sent tx=%s energy=%.0fWh (%.2fkWh) soc=%.0f",
            transaction_id,
            energy,
            energy / 1000.0,
            soc,
        )

    def _stop_meter_loop(self, connector_id: int) -> None:
        task = self._meter_tasks.pop(connector_id, None)
        if task is not None and not task.done():
            task.cancel()

    def _start_meter_loop(self, connector_id: int, transaction_id: int) -> None:
        self._stop_meter_loop(connector_id)
        self._energy_wh[connector_id] = 0.0
        self._soc[connector_id] = 20.0

        async def loop() -> None:
            interval_s = 5.0
            wh_per_tick = 7.2 * 1000.0 * (interval_s / 3600.0)
            try:
                while connector_id in self.active_tx:
                    await self.send_meter_values(connector_id, transaction_id)
                    await asyncio.sleep(interval_s)
                    self._energy_wh[connector_id] = self._energy_wh.get(connector_id, 0.0) + wh_per_tick
                    self._soc[connector_id] = min(
                        100.0, self._soc.get(connector_id, 20.0) + 0.15
                    )
            except asyncio.CancelledError:
                return

        self._meter_tasks[connector_id] = asyncio.create_task(loop())

    async def handle_inbound(self, frame: list) -> None:
        if frame[0] == CALLRESULT:
            msg_id, payload = frame[1], frame[2]
            fut = self._pending.pop(msg_id, None)
            if fut and not fut.done():
                fut.set_result(payload)
            return

        if frame[0] == CALLERROR:
            msg_id = frame[1]
            fut = self._pending.pop(msg_id, None)
            if fut and not fut.done():
                fut.set_exception(RuntimeError(f"CALLERROR {frame[2]}: {frame[3]}"))
            return

        if frame[0] != CALL:
            return

        msg_id, action, payload = frame[1], frame[2], frame[3]

        if action == "RemoteStartTransaction":
            connector_id = int(payload.get("connectorId") or 1)
            id_tag = str(payload.get("idTag") or "admin")
            await self.ws.send(json.dumps([CALLRESULT, msg_id, {"status": "Accepted"}]))

            async def follow_up() -> None:
                await self.status(connector_id, "Preparing")
                await self.start_transaction(connector_id, id_tag)
                await self.status(connector_id, "Charging")
                tx = self.active_tx.get(connector_id)
                if tx is not None:
                    self._start_meter_loop(connector_id, tx)

            asyncio.create_task(follow_up())
            return

        if action == "RemoteStopTransaction":
            transaction_id = int(payload["transactionId"])
            connector_id = next(
                (cid for cid, tx in self.active_tx.items() if tx == transaction_id),
                1,
            )
            await self.ws.send(json.dumps([CALLRESULT, msg_id, {"status": "Accepted"}]))

            async def follow_up() -> None:
                await self.stop_transaction(transaction_id, connector_id)
                await self.status(connector_id, "Available")

            asyncio.create_task(follow_up())
            return

        if action == "ChangeAvailability":
            connector_id = int(payload.get("connectorId") or 0)
            avail_type = payload.get("type")
            status = "Available" if avail_type == "Operative" else "Unavailable"
            connectors = (
                sorted(self.known_connectors) or [1] if connector_id == 0 else [connector_id]
            )
            await self.ws.send(json.dumps([CALLRESULT, msg_id, {"status": "Accepted"}]))

            async def follow_up() -> None:
                for cid in connectors:
                    await self.status(cid, status)

            asyncio.create_task(follow_up())
            return

        if action == "Reset":
            await self.ws.send(json.dumps([CALLRESULT, msg_id, {"status": "Accepted"}]))

            async def follow_up() -> None:
                for cid in sorted(self.known_connectors) or [1]:
                    await self.status(cid, "Available")

            asyncio.create_task(follow_up())
            return

        await self.ws.send(json.dumps([CALLRESULT, msg_id, {"status": "Accepted"}]))
        logger.info("Unhandled inbound %s → Accepted stub", action)

    async def _read_loop(self) -> None:
        async for raw in self.ws:
            frame = json.loads(raw)
            await self.handle_inbound(frame)

    async def run(self, connector_count: int) -> None:
        reader = asyncio.create_task(self._read_loop())
        try:
            await self.boot()
            for cid in range(1, connector_count + 1):
                await self.status(cid, "Available")
            logger.info("Simulator ready as %s (%s connectors)", self.id, connector_count)
            await reader
        finally:
            reader.cancel()
            try:
                await reader
            except asyncio.CancelledError:
                pass


async def main() -> None:
    parser = argparse.ArgumentParser(description="OCPP 1.6 charge-point simulator")
    parser.add_argument("--id", default="test")
    parser.add_argument("--url", default="ws://localhost:9000/ocpp")
    parser.add_argument("--connectors", type=int, default=1)
    args = parser.parse_args()

    ws_url = f"{args.url.rstrip('/')}/{args.id}"
    logger.info("Connecting to %s", ws_url)
    async with websockets.connect(ws_url, subprotocols=["ocpp1.6"]) as ws:
        await Simulator(args.id, ws).run(args.connectors)


if __name__ == "__main__":
    asyncio.run(main())
