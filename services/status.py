def normalize_connector_status(status: str) -> str:
    mapping = {
        "SuspendedEVSE": "SuspendedEV",
        "Finishing": "Available",
    }
    return mapping.get(status, status)


def normalize_meter_sample(value: float, unit: str | None) -> tuple[float, str | None]:
    if unit == "kWh":
        return value * 1000.0, "Wh"
    if unit == "kW":
        return value * 1000.0, "W"
    return value, unit
