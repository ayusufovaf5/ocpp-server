from __future__ import annotations

import asyncio
import ssl
import urllib.request
from datetime import UTC, datetime
from typing import Any

import pytest

from events.evpoint_http import create_evpoint_ssl_context, post_live_update_sync
from events.evpoint_payload import build_payload, charger_details_from_event, normalize_status
from events.evpoint_push_consumer import EvpointPushConsumer
from events.publisher import get_publisher
from events.types import EventType


def test_build_payload_matches_old_shape() -> None:
    now = datetime(2026, 7, 24, 10, 0, 0, tzinfo=UTC)
    payload = build_payload(
        "CP1",
        {
            "connectors": [
                {
                    "number": 1,
                    "status": "SuspendedEV",
                    "charging_speed_kw": 7.2,
                    "total_energy_delivered_kwh": 1.5,
                    "duration_seconds": 90,
                    "transaction_id": 42,
                    "battery": 55.0,
                }
            ]
        },
        now=now,
    )
    assert payload == {
        "update_time": "2026-07-24T10:00:00Z",
        "chargers": [
            {
                "charger_id": "CP1",
                "connectors": [
                    {
                        "number": 1,
                        "status": "SuspendedEv",
                        "charging_speed_kw": 7.2,
                        "total_energy_delivered_kwh": 1.5,
                        "duration_seconds": 90,
                        "transaction_id": 42,
                        "battery": 55.0,
                    }
                ],
            }
        ],
    }


def test_normalize_status_aliases() -> None:
    assert normalize_status(None) == "Unavailable"
    assert normalize_status("SuspendedEVSE") == "SuspendedEv"
    assert normalize_status("Not specified") == "Unavailable"
    assert normalize_status("Custom") == "Custom"


def test_tls_verification_is_explicitly_enabled() -> None:
    context = create_evpoint_ssl_context()
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True
    assert context.verify_mode != ssl.CERT_NONE


def test_post_live_update_uses_verifying_ssl_context(monkeypatch) -> None:
    seen: dict[str, Any] = {}

    class _Resp:
        status = 200

        def __enter__(self) -> _Resp:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    def fake_urlopen(request, data=None, timeout=None, *, context=None, **kwargs):
        assert context is not None
        seen["verify_mode"] = context.verify_mode
        seen["check_hostname"] = context.check_hostname
        if context.verify_mode != ssl.CERT_REQUIRED or not context.check_hostname:
            raise ssl.SSLError("TLS verification must stay enabled for EvPoint push")
        return _Resp()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    context = create_evpoint_ssl_context()
    broken = ssl.create_default_context()
    broken.check_hostname = False
    broken.verify_mode = ssl.CERT_NONE
    with pytest.raises(ssl.SSLError, match="TLS verification must stay enabled"):
        post_live_update_sync(
            "https://example.invalid/Charging/ocpp-live-update",
            {"update_time": "2026-07-24T10:00:00Z", "chargers": []},
            ssl_context=broken,
            timeout_seconds=1,
        )

    status = post_live_update_sync(
        "https://example.invalid/Charging/ocpp-live-update",
        {"update_time": "2026-07-24T10:00:00Z", "chargers": []},
        ssl_context=context,
        timeout_seconds=1,
    )
    assert status == 200
    assert seen["verify_mode"] == ssl.CERT_REQUIRED
    assert seen["check_hostname"] is True


def test_create_evpoint_ssl_context_never_returns_cert_none() -> None:
    context = create_evpoint_ssl_context()
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True


def test_create_evpoint_ssl_context_ignores_missing_ca_bundle() -> None:
    context = create_evpoint_ssl_context(ca_bundle="/nonexistent/evpoint-ca.pem")
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True


@pytest.mark.asyncio
async def test_session_started_posts_expected_payload(fake_redis, monkeypatch) -> None:
    monkeypatch.setenv("EVPOINT_PUSH_MAX_ATTEMPTS", "1")
    monkeypatch.setenv("EVPOINT_PUSH_BACKOFF_SECONDS", "0")
    from config import get_settings

    get_settings.cache_clear()

    posted: list[tuple[str, dict[str, Any]]] = []

    async def fake_post(url: str, payload: dict[str, Any], **_: Any) -> int:
        posted.append((url, payload))
        return 200

    consumer = EvpointPushConsumer(http_post=fake_post, consumer_name="test-evpoint-ok")
    task = asyncio.create_task(consumer.run())
    try:
        await get_publisher().publish(
            EventType.SESSION_STARTED,
            {
                "charge_point_id": "CP_EVPOINT",
                "connector_id": 1,
                "session_id": 9,
                "ocpp_transaction_id": 1001,
                "id_tag": "TAG",
                "meter_start": 0,
                "resumed": False,
            },
        )
        deadline = asyncio.get_running_loop().time() + 5
        while not posted and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.05)
    finally:
        consumer.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        get_settings.cache_clear()

    assert len(posted) == 1
    url, body = posted[0]
    assert "ocpp-live-update" in url
    assert "update_time" in body
    assert body["update_time"].endswith("Z")
    assert body["chargers"][0]["charger_id"] == "CP_EVPOINT"
    connectors = body["chargers"][0]["connectors"]
    assert len(connectors) == 1
    assert connectors[0]["number"] == 1
    assert connectors[0]["status"] == "Charging"
    assert connectors[0]["transaction_id"] == 1001


@pytest.mark.asyncio
async def test_evpoint_unavailable_does_not_stop_consumer(fake_redis, monkeypatch) -> None:
    monkeypatch.setenv("EVPOINT_PUSH_MAX_ATTEMPTS", "2")
    monkeypatch.setenv("EVPOINT_PUSH_BACKOFF_SECONDS", "0")
    from config import get_settings

    get_settings.cache_clear()

    calls: list[str] = []

    async def flaky_post(url: str, payload: dict[str, Any], **_: Any) -> int:
        charge_id = payload["chargers"][0]["charger_id"]
        calls.append(charge_id)
        if charge_id == "CP_DOWN":
            raise ConnectionError("EvPoint unreachable")
        return 200

    consumer = EvpointPushConsumer(http_post=flaky_post, consumer_name="test-evpoint-flaky")
    task = asyncio.create_task(consumer.run())
    try:
        await get_publisher().publish(
            EventType.SESSION_STARTED,
            {
                "charge_point_id": "CP_DOWN",
                "connector_id": 1,
                "ocpp_transaction_id": 1,
            },
        )
        await get_publisher().publish(
            EventType.SESSION_STARTED,
            {
                "charge_point_id": "CP_UP",
                "connector_id": 2,
                "ocpp_transaction_id": 2,
            },
        )
        deadline = asyncio.get_running_loop().time() + 5
        while "CP_UP" not in calls and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.05)
    finally:
        consumer.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        get_settings.cache_clear()

    assert "CP_DOWN" in calls
    assert "CP_UP" in calls
    assert calls.count("CP_DOWN") == 2
    assert calls.count("CP_UP") == 1


def test_charger_details_from_session_started() -> None:
    details = charger_details_from_event(
        EventType.SESSION_STARTED,
        {"connector_id": 1, "ocpp_transaction_id": 7},
    )
    assert details["connectors"][0]["status"] == "Charging"
    assert details["connectors"][0]["transaction_id"] == 7
