import json

import pytest

from ocpp16 import protocol
from services.status import (
    aggregate_station_status,
    normalize_connector_status,
    normalize_meter_sample,
)


def test_parse_call_frame() -> None:
    raw = json.dumps([2, "abc", "Heartbeat", {}])
    message_type, unique_id, action, payload = protocol.parse_frame(raw)
    assert message_type == protocol.MessageType.CALL
    assert unique_id == "abc"
    assert action == "Heartbeat"
    assert payload == {}


def test_parse_invalid_frame() -> None:
    with pytest.raises(ValueError):
        protocol.parse_frame(json.dumps({"not": "a list"}))


def test_call_result_and_error_shapes() -> None:
    assert json.loads(protocol.call_result("1", {"ok": True})) == [3, "1", {"ok": True}]
    err = json.loads(protocol.call_error("1", "NotImplemented", "nope"))
    assert err[0] == 4
    assert err[2] == "NotImplemented"


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("Available", "Available"),
        ("Finishing", "Available"),
        ("SuspendedEVSE", "SuspendedEV"),
        ("Charging", "Charging"),
    ],
)
def test_normalize_connector_status(status: str, expected: str) -> None:
    assert normalize_connector_status(status) == expected


@pytest.mark.parametrize(
    ("statuses", "expected"),
    [
        ([], None),
        (["Available"], "Available"),
        (["Unavailable"], "Unavailable"),
        (["Available", "Unavailable"], "Available"),
        (["Available", "Preparing"], "Preparing"),
        (["Charging", "Available"], "Charging"),
        (["Preparing", "Faulted"], "Faulted"),
        (["SuspendedEV", "Preparing"], "SuspendedEV"),
    ],
)
def test_aggregate_station_status(
    statuses: list[str], expected: str | None
) -> None:
    assert aggregate_station_status(statuses) == expected


@pytest.mark.parametrize(
    ("value", "unit", "expected_value", "expected_unit"),
    [
        (1.5, "kWh", 1500.0, "Wh"),
        (2.0, "kW", 2000.0, "W"),
        (100.0, "Wh", 100.0, "Wh"),
        (50.0, None, 50.0, None),
    ],
)
def test_normalize_meter_sample(
    value: float,
    unit: str | None,
    expected_value: float,
    expected_unit: str | None,
) -> None:
    assert normalize_meter_sample(value, unit) == (expected_value, expected_unit)
