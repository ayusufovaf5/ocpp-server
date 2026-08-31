from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
import urllib.error
import urllib.request
import uuid
from datetime import UTC, datetime
from ftplib import FTP, error_perm
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("cp-sim")
ocpp_log = logging.getLogger("ocpp")

CALL = 2
CALLRESULT = 3
CALLERROR = 4

LOG_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
_LOCAL_DIAG_DIR = Path("C:/diag-upload")


def _uid() -> str:
    return uuid.uuid4().hex[:12]


def _now() -> str:
    return datetime.now(UTC).isoformat()


def setup_simulator_log(charge_point_id: str) -> Path:
    log_dir = Path("logs") / "simulator"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{charge_point_id}.log"

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt=LOG_TIME_FORMAT,
    )
    formatter.converter = time.gmtime

    ocpp_log.setLevel(logging.INFO)
    ocpp_log.propagate = False
    abs_path = str(log_path.resolve())
    for existing in list(ocpp_log.handlers):
        if (
            isinstance(existing, logging.FileHandler)
            and getattr(existing, "baseFilename", None) == abs_path
        ):
            return log_path

    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    ocpp_log.addHandler(handler)

    with open(log_path, "a", encoding="utf-8") as f:
        f.write(
            f"{datetime.now(UTC).strftime(LOG_TIME_FORMAT)} INFO simulator "
            f"Diagnostics log started for charge point {charge_point_id}\n"
        )
    return log_path


def _parse_iso_utc(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def read_diagnostics_log(
    log_path: Path, start_time: Any = None, stop_time: Any = None
) -> str:
    if not log_path.exists():
        return f"No diagnostics log found at {log_path}\n"

    for handler in ocpp_log.handlers:
        handler.flush()

    all_lines = log_path.read_text(encoding="utf-8").splitlines(keepends=True)
    if not all_lines:
        return f"Diagnostics log is empty for {log_path}\n"

    start_dt = _parse_iso_utc(start_time)
    stop_dt = _parse_iso_utc(stop_time)
    if not start_dt and not stop_dt:
        return "".join(all_lines)

    filtered: list[str] = []
    for line in all_lines:
        try:
            line_dt = datetime.strptime(line[:19], LOG_TIME_FORMAT)
        except ValueError:
            continue
        if start_dt and line_dt < start_dt:
            continue
        if stop_dt and line_dt > stop_dt:
            continue
        filtered.append(line)

    if not filtered:
        return (
            f"No log entries in range "
            f"startTime={start_time} stopTime={stop_time}\n"
        )
    return "".join(filtered)


def _http_put_sync(url: str, body: bytes, content_type: str = "text/plain") -> int:
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": content_type, "Content-Length": str(len(body))},
        method="PUT",
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        return int(response.status)


def _ftp_upload_sync(location: str, file_name: str, body: bytes) -> str:
    """Upload body to ftp://user:pass@host[:port]/path/ using STOR file_name."""
    parsed = urlparse(location)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 21
    user = unquote(parsed.username or "anonymous")
    password = unquote(parsed.password or "anonymous@")
    remote_dir = unquote(parsed.path or "/")

    with FTP() as ftp:
        ftp.connect(host, port, timeout=15)
        ftp.login(user, password)
        for part in [p for p in remote_dir.strip("/").split("/") if p]:
            try:
                ftp.cwd(part)
            except error_perm:
                ftp.mkd(part)
                ftp.cwd(part)
        ftp.storbinary(f"STOR {file_name}", BytesIO(body))
        return f"ftp://{host}:{port}/{remote_dir.strip('/')}/{file_name}"


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
        self._power_w: dict[int, float] = {}
        self._last_schedule: dict[int, dict[str, Any]] = {}
        self._default_power_w = 7200.0
        self.diagnostics_log_path = setup_simulator_log(charge_point_id)

    def _record(self, direction: str, frame: list | dict, *, note: str = "") -> None:
        extra = f" {note}" if note else ""
        if isinstance(frame, list) and frame:
            if frame[0] == CALL and len(frame) >= 4:
                ocpp_log.info(
                    "%s CALL %s %s %s%s",
                    direction,
                    frame[1],
                    frame[2],
                    json.dumps(frame[3], ensure_ascii=False),
                    extra,
                )
            elif frame[0] == CALLRESULT and len(frame) >= 3:
                ocpp_log.info(
                    "%s CALLRESULT %s %s%s",
                    direction,
                    frame[1],
                    json.dumps(frame[2], ensure_ascii=False),
                    extra,
                )
            elif frame[0] == CALLERROR and len(frame) >= 3:
                ocpp_log.info(
                    "%s CALLERROR %s %s%s",
                    direction,
                    frame[1],
                    json.dumps(frame[2:], ensure_ascii=False),
                    extra,
                )
            else:
                ocpp_log.info("%s %s%s", direction, json.dumps(frame, ensure_ascii=False), extra)
        else:
            ocpp_log.info("%s %s%s", direction, json.dumps(frame, ensure_ascii=False), extra)

    async def _reply(self, msg_id: str, payload: dict) -> None:
        frame = [CALLRESULT, msg_id, payload]
        self._record("out", frame)
        await self.ws.send(json.dumps(frame))

    def _power_for(self, connector_id: int) -> float:
        return float(self._power_w.get(connector_id, self._default_power_w))

    def _apply_charging_profile(self, connector_id: int, profiles: dict) -> float | None:
        schedule = profiles.get("chargingSchedule") or {}
        periods = schedule.get("chargingSchedulePeriod") or []
        if not periods:
            return None
        limit = periods[0].get("limit")
        if limit is None:
            return None
        limit_f = float(limit)
        unit = str(schedule.get("chargingRateUnit") or "W").upper()
        if unit == "A":
            phases = int(periods[0].get("numberPhases") or 3)
            power_w = limit_f * 230.0 * phases
        else:
            power_w = limit_f
        self._power_w[connector_id] = max(0.0, power_w)
        self._last_schedule[connector_id] = schedule if isinstance(schedule, dict) else {}
        return self._power_w[connector_id]

    def _build_composite_schedule(
        self,
        connector_id: int,
        duration: int,
        charging_rate_unit: str,
    ) -> dict[str, Any]:
        stored = self._last_schedule.get(connector_id)
        if stored:
            schedule = dict(stored)
            schedule.setdefault("chargingRateUnit", charging_rate_unit)
            schedule["duration"] = duration
            return schedule

        power_w = self._power_for(connector_id)
        unit = charging_rate_unit.upper()
        period: dict[str, Any] = {"startPeriod": 0}
        if unit == "A":
            phases = 3
            period["numberPhases"] = phases
            period["limit"] = round(power_w / (230.0 * phases), 1)
        else:
            period["limit"] = power_w
        return {
            "duration": duration,
            "chargingRateUnit": unit,
            "chargingSchedulePeriod": [period],
        }

    async def send_call(self, action: str, payload: dict) -> dict:
        msg_id = _uid()
        frame = [CALL, msg_id, action, payload]
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[msg_id] = fut
        self._record("out", frame)
        await self.ws.send(json.dumps(frame))
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
        self._power_w.pop(connector_id, None)
        self._last_schedule.pop(connector_id, None)
        logger.info("StopTransaction OK tx=%s", transaction_id)

    async def send_meter_values(self, connector_id: int, transaction_id: int) -> None:
        energy = self._energy_wh.get(connector_id, 0.0)
        soc = self._soc.get(connector_id, 20.0)
        power_w = self._power_for(connector_id)
        # Fire-and-forget so inbound CSMS calls stay responsive during metering.
        msg_id = _uid()
        payload = {
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
                            "value": f"{power_w:.0f}",
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
        }
        await self.ws.send(json.dumps([CALL, msg_id, "MeterValues", payload]))
        self._record("out", [CALL, msg_id, "MeterValues", payload])
        logger.info(
            "MeterValues sent tx=%s power=%.0fW (%.2fkW) energy=%.0fWh (%.2fkWh) soc=%.0f",
            transaction_id,
            power_w,
            power_w / 1000.0,
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
        self._power_w.setdefault(connector_id, self._default_power_w)

        async def loop() -> None:
            interval_s = 5.0
            try:
                while connector_id in self.active_tx:
                    await self.send_meter_values(connector_id, transaction_id)
                    await asyncio.sleep(interval_s)
                    power_kw = self._power_for(connector_id) / 1000.0
                    wh_per_tick = power_kw * 1000.0 * (interval_s / 3600.0)
                    self._energy_wh[connector_id] = (
                        self._energy_wh.get(connector_id, 0.0) + wh_per_tick
                    )
                    self._soc[connector_id] = min(
                        100.0, self._soc.get(connector_id, 20.0) + 0.15
                    )
            except asyncio.CancelledError:
                return

        self._meter_tasks[connector_id] = asyncio.create_task(loop())

    async def handle_inbound(self, frame: list) -> None:
        if frame[0] == CALLRESULT:
            self._record("in", frame)
            msg_id, payload = frame[1], frame[2]
            fut = self._pending.pop(msg_id, None)
            if fut and not fut.done():
                fut.set_result(payload)
            return

        if frame[0] == CALLERROR:
            self._record("in", frame)
            msg_id = frame[1]
            fut = self._pending.pop(msg_id, None)
            if fut and not fut.done():
                fut.set_exception(RuntimeError(f"CALLERROR {frame[2]}: {frame[3]}"))
            return

        if frame[0] != CALL:
            return

        self._record("in", frame)
        msg_id, action, payload = frame[1], frame[2], frame[3]

        if action == "RemoteStartTransaction":
            connector_id = int(payload.get("connectorId") or 1)
            id_tag = str(payload.get("idTag") or "admin")
            await self._reply(msg_id, {"status": "Accepted"})

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
            await self._reply(msg_id, {"status": "Accepted"})

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
            await self._reply(msg_id, {"status": "Accepted"})

            async def follow_up() -> None:
                for cid in connectors:
                    await self.status(cid, status)

            asyncio.create_task(follow_up())
            return

        if action == "Reset":
            await self._reply(msg_id, {"status": "Accepted"})

            async def follow_up() -> None:
                for cid in sorted(self.known_connectors) or [1]:
                    await self.status(cid, "Available")

            asyncio.create_task(follow_up())
            return

        if action == "SetChargingProfile":
            await self._reply(msg_id, {"status": "Accepted"})
            connector_id = int(payload.get("connectorId") or 1)
            profiles = payload.get("csChargingProfiles") or {}
            applied = None
            if isinstance(profiles, dict):
                applied = self._apply_charging_profile(connector_id, profiles)
            logger.info(
                "SetChargingProfile → Accepted connector=%s tx=%s applied_power_w=%s",
                connector_id,
                profiles.get("transactionId") if isinstance(profiles, dict) else None,
                applied,
            )
            return

        if action == "GetCompositeSchedule":
            connector_id = int(payload.get("connectorId") or 1)
            duration = int(payload.get("duration") or 3600)
            charging_rate_unit = str(payload.get("chargingRateUnit") or "W").upper()
            if connector_id <= 0:
                await self._reply(msg_id, {"status": "Rejected"})
                logger.info("GetCompositeSchedule → Rejected connector=%s", connector_id)
                return
            schedule = self._build_composite_schedule(
                connector_id, duration, charging_rate_unit
            )
            await self._reply(
                msg_id,
                {
                    "status": "Accepted",
                    "connectorId": connector_id,
                    "scheduleStart": _now(),
                    "chargingSchedule": schedule,
                },
            )
            logger.info(
                "GetCompositeSchedule → Accepted connector=%s duration=%s unit=%s",
                connector_id,
                duration,
                charging_rate_unit,
            )
            return

        if action == "GetDiagnostics":
            location = str(payload.get("location") or "")
            file_name = f"diagnostics_{self.id}_{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}.txt"
            await self._reply(msg_id, {"fileName": file_name})
            logger.info("GetDiagnostics → fileName=%s location=%s", file_name, location)
            asyncio.create_task(self._upload_diagnostics(location, file_name, payload))
            return

        await self._reply(msg_id, {"status": "Accepted"})
        logger.info("Unhandled inbound %s → Accepted stub", action)

    async def _upload_diagnostics(
        self, location: str, file_name: str, req: dict
    ) -> None:
        try:
            await self.send_call(
                "DiagnosticsStatusNotification", {"status": "Uploading"}
            )
            text = read_diagnostics_log(
                self.diagnostics_log_path,
                start_time=req.get("startTime"),
                stop_time=req.get("stopTime"),
            )
            content = text.encode("utf-8")

            _LOCAL_DIAG_DIR.mkdir(parents=True, exist_ok=True)
            local_path = _LOCAL_DIAG_DIR / file_name
            local_path.write_bytes(content)
            logger.info(
                "Diagnostics content ready: %s bytes from %s",
                len(content),
                self.diagnostics_log_path,
            )

            scheme = (urlparse(location).scheme or "").lower()
            if scheme in ("http", "https"):
                target = location.rstrip("/") + "/" + file_name
                status = await asyncio.to_thread(_http_put_sync, target, content)
                logger.info("Diagnostics uploaded %s → HTTP %s", target, status)
            elif scheme == "ftp":
                target = await asyncio.to_thread(
                    _ftp_upload_sync, location, file_name, content
                )
                logger.info("Diagnostics uploaded %s → FTP OK", target)
            else:
                logger.warning(
                    "GetDiagnostics upload supports http(s)/ftp, got %s — "
                    "local file kept, UploadFailed",
                    scheme or "(empty)",
                )
                await self.send_call(
                    "DiagnosticsStatusNotification", {"status": "UploadFailed"}
                )
                return

            await self.send_call(
                "DiagnosticsStatusNotification", {"status": "Uploaded"}
            )
        except (
            urllib.error.URLError,
            urllib.error.HTTPError,
            OSError,
            TimeoutError,
            error_perm,
        ) as exc:
            logger.warning("Diagnostics upload failed: %s (local file may still exist)", exc)
            try:
                await self.send_call(
                    "DiagnosticsStatusNotification", {"status": "UploadFailed"}
                )
            except Exception:
                logger.exception("DiagnosticsStatusNotification UploadFailed failed")
        except Exception:
            logger.exception("GetDiagnostics follow-up failed")
            try:
                await self.send_call(
                    "DiagnosticsStatusNotification", {"status": "UploadFailed"}
                )
            except Exception:
                pass

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
