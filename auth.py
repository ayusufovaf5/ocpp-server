from __future__ import annotations

import secrets
from dataclasses import dataclass

import structlog
from fastapi import Header, HTTPException, status

from config import DEV_API_KEY, get_settings

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class Principal:
    subject: str
    auth_method: str


def is_dev_api_key(api_key: str) -> bool:
    return secrets.compare_digest(api_key, DEV_API_KEY)


def parse_ocpp_allowlist(raw: str) -> set[str]:
    return {part.strip() for part in raw.split(",") if part.strip()}


def ocpp_allowlist_is_open(raw: str) -> bool:
    return "*" in parse_ocpp_allowlist(raw)


def is_charge_point_allowed(charge_point_id: str, allowlist_raw: str) -> bool:
    allowed = parse_ocpp_allowlist(allowlist_raw)
    if "*" in allowed:
        return True
    return charge_point_id in allowed


def is_dev_auth_mode() -> bool:
    settings = get_settings()
    return is_dev_api_key(settings.api_key) or ocpp_allowlist_is_open(
        settings.ocpp_charge_point_allowlist
    )


def log_dev_auth_warnings() -> None:
    settings = get_settings()
    if is_dev_api_key(settings.api_key):
        logger.warning(
            "auth.dev_mode",
            component="rest",
            detail="Default API_KEY in use; not safe outside local development",
        )
    if ocpp_allowlist_is_open(settings.ocpp_charge_point_allowlist):
        logger.warning(
            "auth.dev_mode",
            component="ocpp16",
            detail="OCPP_CHARGE_POINT_ALLOWLIST=* accepts any charge_point_id",
        )


async def get_current_principal(
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> Principal:
    settings = get_settings()
    expected = settings.api_key
    if x_api_key is None or not secrets.compare_digest(x_api_key, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    return Principal(subject="api-client", auth_method="api_key")
