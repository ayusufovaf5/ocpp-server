from __future__ import annotations

import asyncio
import json

import pytest
import uvicorn
import websockets
from httpx import AsyncClient

from db.time import utc_now_iso
from ocpp16.app import create_ocpp_app
from ocpp16.protocol import MessageType
from services.charger_service import ChargerService
from services.session_service import SessionService
from state.connection_registry import ConnectionRegistry, set_connection_registry
from state.connection_state import get_connection_state


async def _wait_port(host: str, port: int, timeout: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        try:
            reader, writer = await asyncio.open_connection(host, port)
            writer.close()
            await writer.wait_closed()
            return
        except OSError:
            if loop.time() > deadline:
                raise TimeoutError(f"Server not ready on {host}:{port}") from None
            await asyncio.sleep(0.05)


@pytest.fixture
async def ocpp_http_server(db_engine, unused_tcp_port):
    set_connection_registry(ConnectionRegistry())
    app = create_ocpp_app()
    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=unused_tcp_port,
        log_level="error",
        lifespan="on",
    )
    server = uvicorn.Server(config)
    server.install_signal_handlers = False
    task = asyncio.create_task(server.serve())
    await _wait_port("127.0.0.1", unused_tcp_port)
    yield {
        "ws": f"ws://127.0.0.1:{unused_tcp_port}",
        "http": f"http://127.0.0.1:{unused_tcp_port}",
    }
    server.should_exit = True
    await task
    set_connection_registry(None)


async def _boot(ws, charge_point_id: str) -> None:
    await ws.send(
        json.dumps(
            [
                2,
                "boot",
                "BootNotification",
                {"chargePointVendor": "V", "chargePointModel": "M"},
            ]
        )
    )
    assert json.loads(await asyncio.wait_for(ws.recv(), timeout=5))[0] == 3


@pytest.mark.asyncio
async def test_remote_start_happy_path_via_rest_and_ws(ocpp_http_server, db_session) -> None:
    charge_point_id = "CP_RSTART"
    await ChargerService(db_session).register_boot(
        charge_point_id=charge_point_id, vendor="V", model="M"
    )

    async with websockets.connect(
        f"{ocpp_http_server['ws']}/ocpp/{charge_point_id}",
        subprotocols=["ocpp1.6"],
    ) as ws:
        await _boot(ws, charge_point_id)

        async def station_loop() -> None:
            try:
                while True:
                    frame = json.loads(await ws.recv())
                    if frame[0] == MessageType.CALL and frame[2] == "RemoteStartTransaction":
                        assert frame[3] == {"connectorId": 1, "idTag": "USER_TAG"}
                        await ws.send(
                            json.dumps([MessageType.CALLRESULT, frame[1], {"status": "Accepted"}])
                        )
                        return
            except websockets.ConnectionClosed:
                return

        reply_task = asyncio.create_task(station_loop())
        async with AsyncClient(
            base_url=ocpp_http_server["http"],
        ) as client:
            response = await client.post(
                f"/start/{charge_point_id}",
                json={
                    "connector_id": 1,
                    "id_tag": "USER_TAG",
                    "transaction_id": 999001,
                },
            )
        assert response.status_code == 200
        assert response.json() == {
            "status": "success",
            "response": {"status": "Accepted"},
        }
        if not reply_task.done():
            reply_task.cancel()
            try:
                await reply_task
            except asyncio.CancelledError:
                pass
        else:
            await reply_task


@pytest.mark.asyncio
async def test_remote_start_offline_returns_404(ocpp_http_server, db_session) -> None:
    charge_point_id = "CP_RSTART_OFF"
    await ChargerService(db_session).register_boot(
        charge_point_id=charge_point_id, vendor="V", model="M"
    )

    async with AsyncClient(
            base_url=ocpp_http_server["http"],
        ) as client:
        response = await client.post(
            f"/start/{charge_point_id}",
            json={
                "connector_id": 1,
                "id_tag": "TAG",
                "transaction_id": 1,
            },
        )
    assert response.status_code == 404
    assert response.json()["detail"] == {
        "status": "error",
        "response": "Charger not found",
    }


@pytest.mark.asyncio
async def test_remote_start_unknown_charger_returns_404(ocpp_http_server) -> None:
    async with AsyncClient(
            base_url=ocpp_http_server["http"],
        ) as client:
        response = await client.post(
            "/start/CP_UNKNOWN",
            json={
                "connector_id": 1,
                "id_tag": "TAG",
                "transaction_id": 1,
            },
        )
    assert response.status_code == 404
    assert response.json()["detail"]["response"] == "Charger not found"


@pytest.mark.asyncio
async def test_remote_stop_without_active_session_does_not_call_station(
    ocpp_http_server, db_session
) -> None:
    charge_point_id = "CP_RSTOP_NONE"
    await ChargerService(db_session).register_boot(
        charge_point_id=charge_point_id, vendor="V", model="M"
    )
    outbound: list[str] = []

    async with websockets.connect(
        f"{ocpp_http_server['ws']}/ocpp/{charge_point_id}",
        subprotocols=["ocpp1.6"],
    ) as ws:
        await _boot(ws, charge_point_id)

        async def station_loop() -> None:
            try:
                while True:
                    frame = json.loads(await ws.recv())
                    if frame[0] == MessageType.CALL:
                        outbound.append(frame[2])
                        await ws.send(
                            json.dumps([MessageType.CALLRESULT, frame[1], {"status": "Accepted"}])
                        )
            except websockets.ConnectionClosed:
                return

        reply_task = asyncio.create_task(station_loop())
        async with AsyncClient(
            base_url=ocpp_http_server["http"],
        ) as client:
            response = await client.post(
                f"/stop/{charge_point_id}",
                json={"transaction_id": 42},
            )
        assert response.status_code == 404
        assert response.json()["detail"] == {
            "status": "error",
            "response": "No active session",
        }
        await asyncio.sleep(0.2)
        assert outbound == []
        reply_task.cancel()
        try:
            await reply_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_remote_stop_uses_ocpp_transaction_id_from_db(ocpp_http_server, db_session) -> None:
    charge_point_id = "CP_RSTOP_OK"
    await ChargerService(db_session).register_boot(
        charge_point_id=charge_point_id, vendor="V", model="M"
    )
    session = await SessionService(db_session).start_transaction(
        charge_point_id=charge_point_id,
        connector_id=1,
        id_tag="TAG",
        meter_start=0,
        timestamp=utc_now_iso(),
    )
    assert session.ocpp_transaction_id is not None
    app_charging_id = 777777

    async with websockets.connect(
        f"{ocpp_http_server['ws']}/ocpp/{charge_point_id}",
        subprotocols=["ocpp1.6"],
    ) as ws:
        await _boot(ws, charge_point_id)

        async def station_loop() -> None:
            try:
                while True:
                    frame = json.loads(await ws.recv())
                    if frame[0] == MessageType.CALL and frame[2] == "RemoteStopTransaction":
                        assert frame[3] == {"transactionId": session.ocpp_transaction_id}
                        assert frame[3]["transactionId"] != app_charging_id
                        await ws.send(
                            json.dumps([MessageType.CALLRESULT, frame[1], {"status": "Accepted"}])
                        )
                        return
            except websockets.ConnectionClosed:
                return

        reply_task = asyncio.create_task(station_loop())
        async with AsyncClient(
            base_url=ocpp_http_server["http"],
        ) as client:
            response = await client.post(
                f"/stop/{charge_point_id}",
                json={"transaction_id": app_charging_id},
            )
        assert response.status_code == 200
        assert response.json() == {
            "status": "success",
            "response": {"status": "Accepted"},
        }
        if not reply_task.done():
            reply_task.cancel()
            try:
                await reply_task
            except asyncio.CancelledError:
                pass
        else:
            await reply_task


@pytest.mark.asyncio
async def test_remote_stop_falls_back_to_latest_completed_session(
    ocpp_http_server, db_session
) -> None:
    charge_point_id = "CP_RSTOP_FALLBACK"
    await ChargerService(db_session).register_boot(
        charge_point_id=charge_point_id, vendor="V", model="M"
    )
    session = await SessionService(db_session).start_transaction(
        charge_point_id=charge_point_id,
        connector_id=1,
        id_tag="TAG",
        meter_start=0,
        timestamp=utc_now_iso(),
    )
    assert session.ocpp_transaction_id is not None
    await SessionService(db_session).stop_transaction(
        charge_point_id=charge_point_id,
        transaction_id=session.ocpp_transaction_id,
        meter_stop=10,
        timestamp=utc_now_iso(),
    )

    outbound: list[dict] = []

    async with websockets.connect(
        f"{ocpp_http_server['ws']}/ocpp/{charge_point_id}",
        subprotocols=["ocpp1.6"],
    ) as ws:
        await _boot(ws, charge_point_id)

        async def station_loop() -> None:
            try:
                while True:
                    frame = json.loads(await ws.recv())
                    if frame[0] == MessageType.CALL and frame[2] == "RemoteStopTransaction":
                        outbound.append(frame[3])
                        await ws.send(
                            json.dumps([MessageType.CALLRESULT, frame[1], {"status": "Accepted"}])
                        )
                        return
            except websockets.ConnectionClosed:
                return

        reply_task = asyncio.create_task(station_loop())
        async with AsyncClient(
            base_url=ocpp_http_server["http"],
        ) as client:
            response = await client.post(
                f"/stop/{charge_point_id}",
                json={"transaction_id": 999},
            )
        assert response.status_code == 200
        assert outbound == [{"transactionId": session.ocpp_transaction_id}]
        if not reply_task.done():
            reply_task.cancel()
            try:
                await reply_task
            except asyncio.CancelledError:
                pass
        else:
            await reply_task


@pytest.mark.asyncio
async def test_remote_start_stop_full_station_cycle_station_stop_reason(
    ocpp_http_server, db_session
) -> None:
    charge_point_id = "CP_FULL_CYCLE"
    await ChargerService(db_session).register_boot(
        charge_point_id=charge_point_id, vendor="V", model="M"
    )

    async with websockets.connect(
        f"{ocpp_http_server['ws']}/ocpp/{charge_point_id}",
        subprotocols=["ocpp1.6"],
    ) as ws:
        await _boot(ws, charge_point_id)
        station_tx_id: int | None = None
        done = asyncio.Event()

        async def station_loop() -> None:
            nonlocal station_tx_id
            try:
                while True:
                    frame = json.loads(await ws.recv())
                    if frame[0] != MessageType.CALL:
                        continue
                    action = frame[2]
                    if action == "RemoteStartTransaction":
                        await ws.send(
                            json.dumps([MessageType.CALLRESULT, frame[1], {"status": "Accepted"}])
                        )
                        await ws.send(
                            json.dumps(
                                [
                                    MessageType.CALL,
                                    "st1",
                                    "StartTransaction",
                                    {
                                        "connectorId": 1,
                                        "idTag": frame[3]["idTag"],
                                        "meterStart": 0,
                                        "timestamp": utc_now_iso(),
                                    },
                                ]
                            )
                        )
                        reply = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
                        assert reply[0] == MessageType.CALLRESULT
                        station_tx_id = reply[2]["transactionId"]
                        await ws.send(
                            json.dumps(
                                [
                                    MessageType.CALL,
                                    "sn1",
                                    "StatusNotification",
                                    {
                                        "connectorId": 1,
                                        "status": "Charging",
                                        "errorCode": "NoError",
                                        "timestamp": utc_now_iso(),
                                    },
                                ]
                            )
                        )
                        assert json.loads(await asyncio.wait_for(ws.recv(), timeout=5))[0] == 3
                    elif action == "RemoteStopTransaction":
                        assert station_tx_id is not None
                        assert frame[3]["transactionId"] == station_tx_id
                        await ws.send(
                            json.dumps([MessageType.CALLRESULT, frame[1], {"status": "Accepted"}])
                        )
                        await ws.send(
                            json.dumps(
                                [
                                    MessageType.CALL,
                                    "stop1",
                                    "StopTransaction",
                                    {
                                        "transactionId": station_tx_id,
                                        "meterStop": 50,
                                        "timestamp": utc_now_iso(),
                                        "reason": "Remote",
                                    },
                                ]
                            )
                        )
                        assert json.loads(await asyncio.wait_for(ws.recv(), timeout=5))[0] == 3
                        await ws.send(
                            json.dumps(
                                [
                                    MessageType.CALL,
                                    "sn2",
                                    "StatusNotification",
                                    {
                                        "connectorId": 1,
                                        "status": "Available",
                                        "errorCode": "NoError",
                                        "timestamp": utc_now_iso(),
                                    },
                                ]
                            )
                        )
                        assert json.loads(await asyncio.wait_for(ws.recv(), timeout=5))[0] == 3
                        done.set()
                        return
            except websockets.ConnectionClosed:
                return

        loop_task = asyncio.create_task(station_loop())
        async with AsyncClient(base_url=ocpp_http_server["http"]) as client:
            start = await client.post(
                f"/start/{charge_point_id}",
                json={"connector_id": 1, "id_tag": "ADMIN", "transaction_id": 4242},
            )
            assert start.status_code == 200
            assert start.json()["response"]["status"] == "Accepted"

            for _ in range(40):
                if station_tx_id is not None:
                    break
                await asyncio.sleep(0.05)
            assert station_tx_id is not None

            stop = await client.post(
                f"/stop/{charge_point_id}",
                json={"transaction_id": 4242, "connector_id": 1},
            )
            assert stop.status_code == 200
            assert stop.json()["response"]["status"] == "Accepted"

        await asyncio.wait_for(done.wait(), timeout=5)
        loop_task.cancel()
        try:
            await loop_task
        except asyncio.CancelledError:
            pass

    from repositories.charger_repository import ChargerRepository
    from repositories.session_repository import SessionRepository
    from services.session_service import END_REASON_REMOTE_STOP

    await db_session.commit()
    db_session.expire_all()
    charger = await ChargerRepository(db_session).get_by_charge_point_id(charge_point_id)
    assert charger is not None
    latest = await SessionRepository(db_session).latest_with_ocpp_transaction_id(charger.id)
    assert latest is not None
    assert latest.status == "Completed"
    assert latest.effective_end_reason == END_REASON_REMOTE_STOP


@pytest.mark.asyncio
async def test_remote_stop_finalizes_and_keeps_tx_for_live(
    ocpp_http_server, db_session
) -> None:
    """Old EvPointOCPP contract: after RemoteStop, Available + transaction_id grace."""
    charge_point_id = "CP_RSTOP_FINALIZE"
    await ChargerService(db_session).register_boot(
        charge_point_id=charge_point_id, vendor="V", model="M"
    )
    app_charging_id = 1780646101
    await get_connection_state().set_pending_remote_start(
        charge_point_id,
        1,
        id_tag="TAG",
        transaction_id=app_charging_id,
    )
    session = await SessionService(db_session).start_transaction(
        charge_point_id=charge_point_id,
        connector_id=1,
        id_tag="TAG",
        meter_start=0,
        timestamp=utc_now_iso(),
    )
    assert session.ocpp_transaction_id == app_charging_id

    async with websockets.connect(
        f"{ocpp_http_server['ws']}/ocpp/{charge_point_id}",
        subprotocols=["ocpp1.6"],
    ) as ws:
        await _boot(ws, charge_point_id)

        async def station_loop() -> None:
            try:
                while True:
                    frame = json.loads(await ws.recv())
                    if frame[0] == MessageType.CALL and frame[2] == "RemoteStopTransaction":
                        assert frame[3] == {"transactionId": app_charging_id}
                        await ws.send(
                            json.dumps(
                                [MessageType.CALLRESULT, frame[1], {"status": "Accepted"}]
                            )
                        )
                        return
            except websockets.ConnectionClosed:
                return

        reply_task = asyncio.create_task(station_loop())
        async with AsyncClient(
            base_url=ocpp_http_server["http"],
        ) as client:
            response = await client.post(
                f"/stop/{charge_point_id}",
                json={"transaction_id": app_charging_id},
            )
        assert response.status_code == 200
        if not reply_task.done():
            reply_task.cancel()
            try:
                await reply_task
            except asyncio.CancelledError:
                pass
        else:
            await reply_task

    from repositories.session_repository import SessionRepository

    await db_session.commit()
    db_session.expire_all()
    row = await SessionRepository(db_session).get_by_ocpp_transaction_id(app_charging_id)
    assert row is not None
    assert row.status == "Completed"

    stopped = await get_connection_state().peek_stopped_ocpp_transaction_for_live(
        charge_point_id, 1
    )
    assert stopped == app_charging_id


@pytest.mark.asyncio
async def test_remote_stop_pending_preparing_without_session(
    ocpp_http_server, db_session
) -> None:
    charge_point_id = "CP_RSTOP_PENDING"
    await ChargerService(db_session).register_boot(
        charge_point_id=charge_point_id, vendor="V", model="M"
    )
    from db.time import utc_now
    from repositories.charger_repository import ChargerRepository
    from repositories.connector_status_repository import ConnectorStatusRepository

    charger = await ChargerRepository(db_session).get_by_charge_point_id(charge_point_id)
    assert charger is not None
    await ConnectorStatusRepository(db_session).upsert(
        charger_id=charger.id,
        connector_id=1,
        status="Preparing",
        updated_at=utc_now(),
    )
    await db_session.commit()

    app_charging_id = 424242
    await get_connection_state().set_pending_remote_start(
        charge_point_id,
        1,
        id_tag="TAG",
        transaction_id=app_charging_id,
    )

    async with websockets.connect(
        f"{ocpp_http_server['ws']}/ocpp/{charge_point_id}",
        subprotocols=["ocpp1.6"],
    ) as ws:
        await _boot(ws, charge_point_id)

        async def station_loop() -> None:
            try:
                while True:
                    frame = json.loads(await ws.recv())
                    if frame[0] == MessageType.CALL and frame[2] == "RemoteStopTransaction":
                        await ws.send(
                            json.dumps(
                                [MessageType.CALLRESULT, frame[1], {"status": "Accepted"}]
                            )
                        )
                        return
            except websockets.ConnectionClosed:
                return

        reply_task = asyncio.create_task(station_loop())
        async with AsyncClient(
            base_url=ocpp_http_server["http"],
        ) as client:
            response = await client.post(
                f"/stop/{charge_point_id}",
                json={"transaction_id": app_charging_id},
            )
        assert response.status_code == 200
        reply_task.cancel()
        try:
            await reply_task
        except asyncio.CancelledError:
            pass

    pending = await get_connection_state().peek_pending_remote_start(charge_point_id, 1)
    assert pending is None
    stopped = await get_connection_state().peek_stopped_ocpp_transaction_for_live(
        charge_point_id, 1
    )
    assert stopped == app_charging_id


@pytest.mark.asyncio
async def test_remote_stop_with_connector_id_scopes_session(ocpp_http_server, db_session) -> None:
    charge_point_id = "CP_RSTOP_CONN"
    await ChargerService(db_session).register_boot(
        charge_point_id=charge_point_id, vendor="V", model="M"
    )
    s1 = await SessionService(db_session).start_transaction(
        charge_point_id=charge_point_id,
        connector_id=1,
        id_tag="A",
        meter_start=0,
        timestamp=utc_now_iso(),
    )
    s2 = await SessionService(db_session).start_transaction(
        charge_point_id=charge_point_id,
        connector_id=2,
        id_tag="B",
        meter_start=0,
        timestamp=utc_now_iso(),
    )
    assert s1.ocpp_transaction_id != s2.ocpp_transaction_id

    seen: list[int] = []
    async with websockets.connect(
        f"{ocpp_http_server['ws']}/ocpp/{charge_point_id}",
        subprotocols=["ocpp1.6"],
    ) as ws:
        await _boot(ws, charge_point_id)

        async def station_loop() -> None:
            try:
                while True:
                    frame = json.loads(await ws.recv())
                    if frame[0] == MessageType.CALL and frame[2] == "RemoteStopTransaction":
                        seen.append(frame[3]["transactionId"])
                        await ws.send(
                            json.dumps([MessageType.CALLRESULT, frame[1], {"status": "Accepted"}])
                        )
                        return
            except websockets.ConnectionClosed:
                return

        task = asyncio.create_task(station_loop())
        async with AsyncClient(
            base_url=ocpp_http_server["http"],
        ) as client:
            response = await client.post(
                f"/stop/{charge_point_id}",
                json={"transaction_id": 0, "connector_id": 2},
            )
        assert response.status_code == 200
        assert seen == [s2.ocpp_transaction_id]
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
