from datetime import UTC, datetime, timedelta


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc_now_iso() -> str:
    return utc_now().strftime("%Y-%m-%dT%H:%M:%S") + "Z"


def parse_ocpp_time(value: str | None) -> datetime:
    if not value:
        return utc_now()
    text = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return utc_now()
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def seconds_ago(seconds: int) -> datetime:
    return utc_now() - timedelta(seconds=seconds)
