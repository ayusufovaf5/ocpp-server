from __future__ import annotations

import structlog

from config import get_settings

logger = structlog.get_logger(__name__)


def parse_ocpp_allowlist(raw: str) -> set[str]:
    return {part.strip() for part in raw.split(",") if part.strip()}


def ocpp_allowlist_is_open(raw: str) -> bool:
    return "*" in parse_ocpp_allowlist(raw)


def is_charge_point_allowed(charge_point_id: str, allowlist_raw: str) -> bool:
    allowed = parse_ocpp_allowlist(allowlist_raw)
    if "*" in allowed:
        return True
    return charge_point_id in allowed


def log_dev_auth_warnings() -> None:
    settings = get_settings()
    if ocpp_allowlist_is_open(settings.ocpp_charge_point_allowlist):
        logger.warning(
            "auth.dev_mode",
            component="ocpp16",
            detail="OCPP_CHARGE_POINT_ALLOWLIST=* accepts any charge_point_id",
        )
