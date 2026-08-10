_IDLE_STATUSES = frozenset({"Available", "Unavailable"})

_BUSY_PRIORITY = {
    "Faulted": 100,
    "Charging": 80,
    "SuspendedEV": 70,
    "Preparing": 60,
    "Reserved": 50,
}

ENERGY_IMPORT_MEASURANDS = frozenset(
    {
        "Energy.Active.Import.Register",
        "Energy.Active.Import.Interval",
    }
)
SOC_MEASURANDS = frozenset({"SoC"})
POWER_IMPORT_MEASURANDS = frozenset({"Power.Active.Import"})


def normalize_connector_status(status: str) -> str:
    mapping = {
        "SuspendedEVSE": "SuspendedEV",
        "Finishing": "Available",
    }
    return mapping.get(status, status)


def aggregate_station_status(connector_statuses: list[str]) -> str | None:
    if not connector_statuses:
        return None

    if all(status in _IDLE_STATUSES for status in connector_statuses):
        if any(status == "Available" for status in connector_statuses):
            return "Available"
        return "Unavailable"

    busy = [status for status in connector_statuses if status not in _IDLE_STATUSES]
    return max(
        busy,
        key=lambda status: (_BUSY_PRIORITY.get(status, 40), status),
    )


def normalize_meter_sample(value: float, unit: str | None) -> tuple[float, str | None]:
    if unit == "kWh":
        return value * 1000.0, "Wh"
    if unit == "kW":
        return value * 1000.0, "W"
    return value, unit


def power_watts_to_kw(value: float, unit: str | None) -> float:
    if unit == "kW":
        return round(value, 3)
    return round(value / 1000.0, 3)


def pick_latest_meter(
    meters: list,
    measurands: frozenset[str],
):
    for meter in meters:
        if (meter.measurand or "") in measurands:
            return meter
    return None
