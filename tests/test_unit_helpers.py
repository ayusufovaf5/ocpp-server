import json

import pytest

from ocpp16 import protocol
from services.status import (
    ENERGY_IMPORT_MEASURANDS,
    SOC_MEASURANDS,
    aggregate_station_status,
    normalize_connector_status,
    normalize_meter_sample,
    pick_latest_meter,
    power_watts_to_kw,
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
def test_aggregate_station_status(statuses: list[str], expected: str | None) -> None:
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


def test_power_watts_to_kw() -> None:
    assert power_watts_to_kw(7200.0, "W") == 7.2
    assert power_watts_to_kw(7.2, "kW") == 7.2


def test_pick_latest_meter() -> None:
    class Row:
        def __init__(self, measurand: str, value: float) -> None:
            self.measurand = measurand
            self.value = value

    meters = [
        Row("SoC", 55.0),
        Row("Energy.Active.Import.Register", 1000.0),
    ]
    assert pick_latest_meter(meters, SOC_MEASURANDS).value == 55.0
    assert pick_latest_meter(meters, ENERGY_IMPORT_MEASURANDS).value == 1000.0
    assert pick_latest_meter(meters, frozenset({"Power.Active.Import"})) is None
