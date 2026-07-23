import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

import db as db_module
from config import get_settings
from logging_config import configure_logging
from ocpp16.handler import Ocpp16Handler
from tasks.heartbeat_monitor import run_heartbeat_monitor

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)
    monitor = asyncio.create_task(run_heartbeat_monitor())
    logger.info("ocpp16.startup", port=settings.ocpp16_port)
    try:
        yield
    finally:
        monitor.cancel()
        try:
            await monitor
        except asyncio.CancelledError:
            pass
        logger.info("ocpp16.shutdown")


def create_ocpp_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title=f"{settings.app_name}-ocpp16",
        version=settings.app_version,
        lifespan=lifespan,
    )

    @app.websocket("/ocpp/{charge_point_id}")
    async def ocpp_ws(websocket: WebSocket, charge_point_id: str) -> None:
        subprotocol = None
        if "ocpp1.6" in (websocket.headers.get("sec-websocket-protocol") or ""):
            subprotocol = "ocpp1.6"
        await websocket.accept(subprotocol=subprotocol)
        logger.info("ocpp16.connected", charge_point_id=charge_point_id)
        try:
            while True:
                raw = await websocket.receive_text()
                async with db_module.async_session_factory() as db:
                    handler = Ocpp16Handler(charge_point_id, db)
                    response = await handler.handle_raw(raw)
                await websocket.send_text(response)
        except WebSocketDisconnect:
            logger.info("ocpp16.disconnected", charge_point_id=charge_point_id)
        except Exception:
            logger.exception("ocpp16.connection_error", charge_point_id=charge_point_id)
            await websocket.close()

    return app


app = create_ocpp_app()


def main() -> None:
    import uvicorn

    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)
    uvicorn.run(
        "ocpp16.app:app",
        host=settings.api_host,
        port=settings.ocpp16_port,
        reload=False,
    )


if __name__ == "__main__":
    main()
