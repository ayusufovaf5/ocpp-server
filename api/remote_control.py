from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from auth import Principal, get_current_principal
from db import get_db_session
from services.errors import (
    AmbiguousActiveSessionError,
    ChargerCallError,
    ChargerOfflineError,
    ChargerTimeoutError,
    NoActiveSessionError,
)
from services.remote_control_service import RemoteControlService

router = APIRouter()


class ResetRequest(BaseModel):
    reset_type: Literal["hard", "soft"]


class ChangeAvailabilityRequest(BaseModel):
    is_available: bool
    connector_id: int = Field(default=0)


class UnlockConnectorRequest(BaseModel):
    connector_id: int


class UpdateFirmwareRequest(BaseModel):
    location: str


class RemoteStartRequest(BaseModel):
    connector_id: int
    id_tag: str
    transaction_id: int


class RemoteStopRequest(BaseModel):
    transaction_id: int = 0
    connector_id: int | None = None


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


async def _run_remote(factory) -> dict[str, Any]:
    try:
        response = await factory()
    except ChargerOfflineError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"status": "error", "response": "Charger not found"},
        ) from None
    except NoActiveSessionError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"status": "error", "response": "No active session"},
        ) from None
    except AmbiguousActiveSessionError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "status": "error",
                "response": (f"Multiple active sessions ({exc.count}); pass connector_id"),
            },
        ) from None
    except (ChargerTimeoutError, ChargerCallError) as exc:
        mapped = _map_remote_errors(exc)
        assert mapped is not None
        return mapped
    return _remote_http_result(response)


@router.post("/reset/{charger_id}")
async def reset_charger(
    charger_id: str,
    body: ResetRequest,
    db: AsyncSession = Depends(get_db_session),
    _principal: Principal = Depends(get_current_principal),
) -> dict[str, Any]:
    return await _run_remote(lambda: RemoteControlService(db).reset(charger_id, body.reset_type))


@router.post("/change-availability/{charger_id}")
async def change_availability(
    charger_id: str,
    body: ChangeAvailabilityRequest,
    db: AsyncSession = Depends(get_db_session),
    _principal: Principal = Depends(get_current_principal),
) -> dict[str, Any]:
    return await _run_remote(
        lambda: RemoteControlService(db).change_availability(
            charger_id,
            is_available=body.is_available,
            connector_id=body.connector_id,
        )
    )


@router.post("/unlock-connector/{charger_id}")
async def unlock_connector(
    charger_id: str,
    body: UnlockConnectorRequest,
    db: AsyncSession = Depends(get_db_session),
    _principal: Principal = Depends(get_current_principal),
) -> dict[str, Any]:
    return await _run_remote(
        lambda: RemoteControlService(db).unlock_connector(charger_id, body.connector_id)
    )


@router.get("/{charger_id}/configuration")
async def get_configuration(
    charger_id: str,
    key: list[str] = Query(default=[]),
    db: AsyncSession = Depends(get_db_session),
    _principal: Principal = Depends(get_current_principal),
) -> dict[str, Any]:
    try:
        configuration = await RemoteControlService(db).get_configuration(
            charger_id, keys=key or None
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
    return {"status": "success", "configuration": configuration}


@router.post("/change-configuration/{charger_id}")
async def change_configuration(
    charger_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db_session),
    _principal: Principal = Depends(get_current_principal),
) -> dict[str, Any]:
    body = await request.json()
    if not isinstance(body, dict) or not body:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"status": "error", "response": "No configuration data provided"},
        )
    return await _run_remote(
        lambda: RemoteControlService(db).change_configuration(charger_id, body)
    )


@router.get("/trigger-message/{charger_id}/{message_type}")
async def trigger_message(
    charger_id: str,
    message_type: str,
    db: AsyncSession = Depends(get_db_session),
    _principal: Principal = Depends(get_current_principal),
) -> dict[str, Any]:
    return await _run_remote(
        lambda: RemoteControlService(db).trigger_message(charger_id, message_type)
    )


@router.post("/update-firmware/{charger_id}")
async def update_firmware(
    charger_id: str,
    body: UpdateFirmwareRequest,
    db: AsyncSession = Depends(get_db_session),
    _principal: Principal = Depends(get_current_principal),
) -> dict[str, Any]:
    return await _run_remote(
        lambda: RemoteControlService(db).update_firmware(charger_id, body.location)
    )


@router.post("/start/{charger_id}")
async def remote_start(
    charger_id: str,
    body: RemoteStartRequest,
    db: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    return await _run_remote(
        lambda: RemoteControlService(db).remote_start(
            charger_id,
            connector_id=body.connector_id,
            id_tag=body.id_tag,
            transaction_id=body.transaction_id,
        )
    )


@router.post("/stop/{charger_id}")
async def remote_stop(
    charger_id: str,
    body: RemoteStopRequest,
    db: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    return await _run_remote(
        lambda: RemoteControlService(db).remote_stop(
            charger_id,
            transaction_id=body.transaction_id,
            connector_id=body.connector_id,
        )
    )
