from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from db import get_db_session
from config import get_settings
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


class GetDiagnosticsRequest(BaseModel):
    location: str | None = None
    retries: int | None = None
    retry_interval: int | None = None
    start_time: str | None = None
    stop_time: str | None = None


class RemoteStartRequest(BaseModel):
    connector_id: int
    id_tag: str
    transaction_id: int


class RemoteStopRequest(BaseModel):
    transaction_id: int = 0
    connector_id: int | None = None


class SetChargingProfileRequest(BaseModel):
    connector_id: int
    transaction_id: int
    limit: float
    charging_rate_unit: Literal["W", "A"] = "W"
    number_phases: int | None = 3
    stack_level: int = 0
    charging_profile_id: int | None = None


class GetCompositeScheduleRequest(BaseModel):
    connector_id: int
    duration: int
    charging_rate_unit: Literal["W", "A"] | None = None


class ClearChargingProfileRequest(BaseModel):
    id: int | None = None
    connector_id: int | None = None
    charging_profile_purpose: Literal[
        "ChargePointMaxProfile", "TxDefaultProfile", "TxProfile"
    ] | None = None
    stack_level: int | None = None


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
) -> dict[str, Any]:
    return await _run_remote(lambda: RemoteControlService(db).reset(charger_id, body.reset_type))


@router.post("/change-availability/{charger_id}")
async def change_availability(
    charger_id: str,
    body: ChangeAvailabilityRequest,
    db: AsyncSession = Depends(get_db_session),
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
) -> dict[str, Any]:
    return await _run_remote(
        lambda: RemoteControlService(db).unlock_connector(charger_id, body.connector_id)
    )


@router.get("/{charger_id}/configuration")
async def get_configuration(
    charger_id: str,
    key: list[str] = Query(default=[]),
    db: AsyncSession = Depends(get_db_session),
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
) -> dict[str, Any]:
    return await _run_remote(
        lambda: RemoteControlService(db).trigger_message(charger_id, message_type)
    )


@router.post("/update-firmware/{charger_id}")
async def update_firmware(
    charger_id: str,
    body: UpdateFirmwareRequest,
    db: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    return await _run_remote(
        lambda: RemoteControlService(db).update_firmware(charger_id, body.location)
    )


@router.post("/get-diagnostics/{charger_id}")
async def get_diagnostics(
    charger_id: str,
    body: GetDiagnosticsRequest | None = None,
    db: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    req = body or GetDiagnosticsRequest()
    location = (req.location or "").strip() or get_settings().diagnostics_ftp_location.strip()
    if not location:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "status": "error",
                "response": "DIAGNOSTICS_FTP_LOCATION is not configured",
            },
        )
    return await _run_remote(
        lambda: RemoteControlService(db).get_diagnostics(
            charger_id,
            location=location,
            retries=req.retries,
            retry_interval=req.retry_interval,
            start_time=req.start_time,
            stop_time=req.stop_time,
        )
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


@router.post("/set-charging-profile/{charger_id}")
async def set_charging_profile(
    charger_id: str,
    body: SetChargingProfileRequest,
    db: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    return await _run_remote(
        lambda: RemoteControlService(db).set_charging_profile(
            charger_id,
            connector_id=body.connector_id,
            transaction_id=body.transaction_id,
            limit=body.limit,
            charging_rate_unit=body.charging_rate_unit,
            number_phases=body.number_phases,
            stack_level=body.stack_level,
            charging_profile_id=body.charging_profile_id,
        )
    )


@router.post("/composite-schedule/{charger_id}")
async def get_composite_schedule(
    charger_id: str,
    body: GetCompositeScheduleRequest,
    db: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    return await _run_remote(
        lambda: RemoteControlService(db).get_composite_schedule(
            charger_id,
            connector_id=body.connector_id,
            duration=body.duration,
            charging_rate_unit=body.charging_rate_unit,
        )
    )


@router.post("/clear-charging-profile/{charger_id}")
async def clear_charging_profile(
    charger_id: str,
    body: ClearChargingProfileRequest | None = None,
    db: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    req = body or ClearChargingProfileRequest()
    return await _run_remote(
        lambda: RemoteControlService(db).clear_charging_profile(
            charger_id,
            id=req.id,
            connector_id=req.connector_id,
            charging_profile_purpose=req.charging_profile_purpose,
            stack_level=req.stack_level,
        )
    )
