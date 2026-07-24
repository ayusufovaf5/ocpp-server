from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from auth import Principal, get_current_principal
from db import get_db_session
from services.errors import ChargerCallError, ChargerOfflineError, ChargerTimeoutError
from services.remote_control_service import RemoteControlService

router = APIRouter()


class ResetRequest(BaseModel):
    reset_type: Literal["hard", "soft"]


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
    except ChargerTimeoutError:
        return {
            "status": "success",
            "response": {
                "status": "Timeout",
                "message": "Charge point did not respond in time",
            },
        }
    except ChargerCallError as exc:
        return {
            "status": "success",
            "response": {"status": "Error", "message": str(exc)},
        }
    return {"status": "success", "response": response}
