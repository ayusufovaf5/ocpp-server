from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from auth import Principal, get_current_principal
from db import get_db_session
from services.errors import ChargerCallError, ChargerOfflineError, ChargerTimeoutError
from services.remote_control_service import RemoteControlService

router = APIRouter()


class ResetRequest(BaseModel):
    reset_type: Literal["hard", "soft"]


class ChangeAvailabilityRequest(BaseModel):
    is_available: bool
    connector_id: int = Field(default=0)


class UnlockConnectorRequest(BaseModel):
    connector_id: int


def _remote_http_result(coro_result: Any) -> dict[str, Any]:
    return {"status": "success", "response": coro_result}


def _map_remote_errors(exc: Exception) -> dict[str, Any] | None:
    if isinstance(exc, ChargerTimeoutError):
        return {
            "status": "success",
            "response": {
                "status": "Timeout",
                "message": "Charge point did not respond in time",
            },
        }
    if isinstance(exc, ChargerCallError):
        return {
            "status": "success",
            "response": {"status": "Error", "message": str(exc)},
        }
    return None


@router.post("/reset/{charger_id}")
async def reset_charger(
    charger_id: str,
    body: ResetRequest,
    db: AsyncSession = Depends(get_db_session),
    _principal: Principal = Depends(get_current_principal),
) -> dict[str, Any]:
    try:
        response = await RemoteControlService(db).reset(charger_id, body.reset_type)
    except ChargerOfflineError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"status": "error", "response": "Charger not found"},
        ) from None
    except (ChargerTimeoutError, ChargerCallError) as exc:
        mapped = _map_remote_errors(exc)
        assert mapped is not None
        return mapped
    return _remote_http_result(response)


@router.post("/change-availability/{charger_id}")
async def change_availability(
    charger_id: str,
    body: ChangeAvailabilityRequest,
    db: AsyncSession = Depends(get_db_session),
    _principal: Principal = Depends(get_current_principal),
) -> dict[str, Any]:
    try:
        response = await RemoteControlService(db).change_availability(
            charger_id,
            is_available=body.is_available,
            connector_id=body.connector_id,
        )
    except ChargerOfflineError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"status": "error", "response": "Charger not found"},
        ) from None
    except (ChargerTimeoutError, ChargerCallError) as exc:
        mapped = _map_remote_errors(exc)
        assert mapped is not None
        return mapped
    return _remote_http_result(response)


@router.post("/unlock-connector/{charger_id}")
async def unlock_connector(
    charger_id: str,
    body: UnlockConnectorRequest,
    db: AsyncSession = Depends(get_db_session),
    _principal: Principal = Depends(get_current_principal),
) -> dict[str, Any]:
    try:
        response = await RemoteControlService(db).unlock_connector(
            charger_id,
            body.connector_id,
        )
    except ChargerOfflineError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"status": "error", "response": "Charger not found"},
        ) from None
    except (ChargerTimeoutError, ChargerCallError) as exc:
        mapped = _map_remote_errors(exc)
        assert mapped is not None
        return mapped
    return _remote_http_result(response)
