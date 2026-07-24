from __future__ import annotations

import asyncio
import json

import pytest
import uvicorn
from httpx import AsyncClient

from db.time import utc_now_iso
from ocpp16.app import create_ocpp_app
from services.charger_service import ChargerService
from services.live_status_service import LiveStatusService
from services.session_service import SessionService
from state.connection_registry import ConnectionRegistry, set_connection_registry
from state.redis_client import get_redis


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
    yield f"http://127.0.0.1:{unused_tcp_port}"
    server.should_exit = True
    await task
    set_connection_registry(None)


async def _read_json_line(lines, timeout: float = 5.0) -> dict:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        remaining = max(0.05, deadline - asyncio.get_running_loop().time())
        try:
            chunk = await asyncio.wait_for(lines.__anext__(), timeout=remaining)
        except TimeoutError:
            continue
        except StopAsyncIteration as exc:
            raise AssertionError("SSE stream ended before JSON line") from exc
        if chunk.strip():
            return json.loads(chunk)
    raise AssertionError("Timed out waiting for SSE JSON line")


@pytest.mark.asyncio
async def test_live_snapshot_includes_connector_status(db_session) -> None:
    await ChargerService(db_session).register_boot(charge_point_id="CP_SNAP", vendor="V", model="M")
    await ChargerService(db_session).update_status("CP_SNAP", "Available", connector_id=1)
    payload = await LiveStatusService(db_session).build_timed_live_payload()
    chargers = {c["charger_id"]: c for c in payload["chargers"]}
    assert "CP_SNAP" in chargers
    assert any(
        c["number"] == 1 and c["status"] == "Available" for c in chargers["CP_SNAP"]["connectors"]
    )


@pytest.mark.asyncio
async def test_timed_live_details_initial_snapshot(ocpp_http_server, db_session) -> None:
    await ChargerService(db_session).register_boot(charge_point_id="CP_LIVE", vendor="V", model="M")
    await ChargerService(db_session).update_status("CP_LIVE", "Available", connector_id=1)

    async with AsyncClient(base_url=ocpp_http_server, timeout=5.0) as client:
        async with client.stream("GET", "/timed-live-details") as response:
            assert response.status_code == 200
            assert "text/event-stream" in response.headers["content-type"]
            payload = await _read_json_line(response.aiter_lines())
            assert payload["update_time"].endswith("Z")
            chargers = {c["charger_id"]: c for c in payload["chargers"]}
            assert "CP_LIVE" in chargers
            connectors = chargers["CP_LIVE"]["connectors"]
            assert any(c["number"] == 1 and c["status"] == "Available" for c in connectors)


@pytest.mark.asyncio
async def test_timed_live_details_emits_on_bus_event(ocpp_http_server, db_session) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_LIVE2", vendor="V", model="M"
    )
    await ChargerService(db_session).update_status("CP_LIVE2", "Available", connector_id=1)

    async with AsyncClient(base_url=ocpp_http_server, timeout=8.0) as client:
        async with client.stream("GET", "/timed-live-details") as response:
            assert response.status_code == 200
            lines = response.aiter_lines()
            first = await _read_json_line(lines)
            assert first is not None
            await asyncio.sleep(0.4)

            session = await SessionService(db_session).start_transaction(
                charge_point_id="CP_LIVE2",
                connector_id=1,
                id_tag="TAG",
                meter_start=0,
                timestamp=utc_now_iso(),
            )
            assert session.ocpp_transaction_id is not None

            second = await _read_json_line(lines, timeout=5.0)
            chargers = {c["charger_id"]: c for c in second["chargers"]}
            match = next(c for c in chargers["CP_LIVE2"]["connectors"] if c["number"] == 1)
            assert match["status"] == "Charging"
            assert match["transaction_id"] == session.ocpp_transaction_id


@pytest.mark.asyncio
async def test_timed_live_details_two_clients_both_see_events(ocpp_http_server, db_session) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_FANOUT", vendor="V", model="M"
    )
    await ChargerService(db_session).update_status("CP_FANOUT", "Available", connector_id=1)

    async with AsyncClient(base_url=ocpp_http_server, timeout=10.0) as client:
        stream_a = client.stream("GET", "/timed-live-details")
        stream_b = client.stream("GET", "/timed-live-details")
        response_a = await stream_a.__aenter__()
        response_b = await stream_b.__aenter__()
        try:
            assert response_a.status_code == 200
            assert response_b.status_code == 200
            lines_a = response_a.aiter_lines()
            lines_b = response_b.aiter_lines()
            await _read_json_line(lines_a)
            await _read_json_line(lines_b)
            await asyncio.sleep(0.4)

            session = await SessionService(db_session).start_transaction(
                charge_point_id="CP_FANOUT",
                connector_id=1,
                id_tag="TAG",
                meter_start=0,
                timestamp=utc_now_iso(),
            )
            assert session.ocpp_transaction_id is not None

            update_a = await _read_json_line(lines_a, timeout=5.0)
            update_b = await _read_json_line(lines_b, timeout=5.0)
            for payload in (update_a, update_b):
                chargers = {c["charger_id"]: c for c in payload["chargers"]}
                match = next(c for c in chargers["CP_FANOUT"]["connectors"] if c["number"] == 1)
                assert match["status"] == "Charging"
                assert match["transaction_id"] == session.ocpp_transaction_id

            await stream_a.__aexit__(None, None, None)
            stream_a = None
            await asyncio.sleep(0.2)

            await SessionService(db_session).stop_transaction(
                charge_point_id="CP_FANOUT",
                transaction_id=session.ocpp_transaction_id,
                meter_stop=10,
                timestamp=utc_now_iso(),
                connector_id=1,
            )
            after = await _read_json_line(lines_b, timeout=5.0)
            chargers = {c["charger_id"]: c for c in after["chargers"]}
            match = next(c for c in chargers["CP_FANOUT"]["connectors"] if c["number"] == 1)
            assert match["transaction_id"] is None
            assert match["status"] != "Charging"
        finally:
            if stream_a is not None:
                await stream_a.__aexit__(None, None, None)
            await stream_b.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_timed_live_details_reconnect_cleans_groups(ocpp_http_server, db_session) -> None:
    await ChargerService(db_session).register_boot(
        charge_point_id="CP_LIVE_RE", vendor="V", model="M"
    )
    redis = await get_redis()

    async with AsyncClient(base_url=ocpp_http_server, timeout=5.0) as client:
        for _ in range(5):
            async with client.stream("GET", "/timed-live-details") as response:
                assert response.status_code == 200
                await _read_json_line(response.aiter_lines(), timeout=3.0)
            await asyncio.sleep(0.05)

    await asyncio.sleep(0.3)
    try:
        groups = await redis.xinfo_groups("csms.events")
    except Exception:
        groups = []
    live_groups = []
    for group in groups:
        name = group.get("name") if isinstance(group, dict) else None
        if name is None and isinstance(group, dict):
            name = group.get(b"name")
        text = name.decode() if isinstance(name, bytes) else str(name or "")
        if text.startswith("live-status-sse-"):
            live_groups.append(text)
    assert live_groups == []


@pytest.mark.asyncio
async def test_timed_live_details_redis_down_returns_503(ocpp_http_server, monkeypatch) -> None:
    async def boom() -> None:
        raise ConnectionError("redis down")

    monkeypatch.setattr("api.live_status.assert_redis_ready", boom)
    async with AsyncClient(base_url=ocpp_http_server, timeout=5.0) as client:
        response = await client.get("/timed-live-details")
    assert response.status_code == 503
    assert response.json()["detail"]["response"] == "Event bus unavailable"
