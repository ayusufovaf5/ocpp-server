_IDLE_STATUSES = frozenset({"Available", "Unavailable"})

_BUSY_PRIORITY = {
    "Faulted": 100,
    "Charging": 80,
    "SuspendedEV": 70,
    "Preparing": 60,
    "Reserved": 50,
}


def normalize_connector_status(status: str) -> str:
    mapping = {
        "SuspendedEVSE": "SuspendedEV",
        "Finishing": "Available",
    }
    return mapping.get(status, status)


def aggregate_station_status(connector_statuses: list[str]) -> str | None:
    """Aggregate per-connector statuses into one station status (ADR 005).

    Returns None when there are no connector rows (caller should use legacy).
    """
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
