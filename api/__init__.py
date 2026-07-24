from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from auth import Principal, get_current_principal
from config import get_settings
from db import get_db_session
from services.charger_service import ChargerService

router = APIRouter()


class ConnectorStatusResponse(BaseModel):
    connector_id: int
    status: str
    updated_at: datetime


class ChargerStatusResponse(BaseModel):
    charge_point_id: str
    status: str
    legacy_status: str
    charge_point_status: str | None
    connectors: list[ConnectorStatusResponse]


@router.get("/health")
async def health(db: AsyncSession = Depends(get_db_session)) -> JSONResponse:
    try:
        await db.execute(text("SELECT 1"))
        db_ok = True
    except Exception:
        db_ok = False

    payload = {
        "status": "ok" if db_ok else "degraded",
        "database": "up" if db_ok else "down",
    }
    code = status.HTTP_200_OK if db_ok else status.HTTP_503_SERVICE_UNAVAILABLE
    return JSONResponse(content=payload, status_code=code)


@router.get("/version")
async def version(
    _principal: Principal = Depends(get_current_principal),
) -> dict[str, str]:
    settings = get_settings()
    return {
        "name": settings.app_name,
        "version": settings.app_version,
    }


@router.get(
    "/chargers/{charge_point_id}/status",
    response_model=ChargerStatusResponse,
)
async def charger_status(
    charge_point_id: str,
    db: AsyncSession = Depends(get_db_session),
    _principal: Principal = Depends(get_current_principal),
) -> ChargerStatusResponse:
    view = await ChargerService(db).get_status_view(charge_point_id)
    if view is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Charger not found")
    return ChargerStatusResponse(
        charge_point_id=view.charge_point_id,
        status=view.status,
        legacy_status=view.legacy_status,
        charge_point_status=view.charge_point_status,
        connectors=[
            ConnectorStatusResponse(
                connector_id=c.connector_id,
                status=c.status,
                updated_at=c.updated_at,
            )
            for c in view.connectors
        ],
    )
