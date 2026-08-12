import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, WebSocketException, status

import db as db_module
from api.live_status import router as live_status_router
from api.remote_control import router as remote_control_router
from auth import is_charge_point_allowed, log_dev_auth_warnings
from config import get_settings
from events.evpoint_push_consumer import EvpointPushConsumer
from events.logging_consumer import LoggingConsumer
from logging_config import configure_logging
from ocpp16 import protocol
from ocpp16.handler import Ocpp16Handler
from services.charger_service import ChargerService
from services.errors import ChargerOfflineError
from state.connection_registry import get_connection_registry
from tasks.charging_session_timeout_monitor import run_charging_session_timeout_monitor
from tasks.heartbeat_monitor import run_heartbeat_monitor
from tasks.offline_session_monitor import run_offline_session_monitor

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)
    log_dev_auth_warnings()
    heartbeat = asyncio.create_task(run_heartbeat_monitor())
    offline = asyncio.create_task(run_offline_session_monitor())
    session_timeout = asyncio.create_task(run_charging_session_timeout_monitor())
    logging_consumer = LoggingConsumer()
    evpoint_consumer = EvpointPushConsumer()
    consumer_task = asyncio.create_task(logging_consumer.run())
    evpoint_task = asyncio.create_task(evpoint_consumer.run())
    logger.info("ocpp16.startup", port=settings.ocpp16_port)
    try:
        yield
    finally:
        logging_consumer.stop()
        evpoint_consumer.stop()
        for task in (heartbeat, offline, session_timeout, consumer_task, evpoint_task):
            task.cancel()
            try:
                await task
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
    app.include_router(remote_control_router)
    app.include_router(live_status_router)

    @app.websocket("/ocpp/{charge_point_id}")
    async def ocpp_ws(websocket: WebSocket, charge_point_id: str) -> None:
        settings = get_settings()
        if not is_charge_point_allowed(charge_point_id, settings.ocpp_charge_point_allowlist):
            logger.warning(
                "ocpp.auth_rejected",
                charge_point_id=charge_point_id,
                reason="not_in_allowlist",
            )
            raise WebSocketException(
                code=status.WS_1008_POLICY_VIOLATION,
                reason="Unauthorized charge_point_id",
            )

        subprotocol = None
        if "ocpp1.6" in (websocket.headers.get("sec-websocket-protocol") or ""):
            subprotocol = "ocpp1.6"
        await websocket.accept(subprotocol=subprotocol)
        registry = get_connection_registry()
        previous = registry.register(charge_point_id, websocket)
        if previous is not None:
            protocol.fail_pending_for_charge_point(
                charge_point_id,
                ChargerOfflineError(charge_point_id),
            )
            try:
                await previous.close()
            except Exception:
                logger.warning(
                    "ocpp16.previous_ws_close_failed",
                    charge_point_id=charge_point_id,
                )
        async with db_module.async_session_factory() as db:
            await ChargerService(db).ensure_connected(charge_point_id)
        logger.info("ocpp16.connected", charge_point_id=charge_point_id)
        inbound_tasks: set[asyncio.Task[None]] = set()

        async def _handle_inbound_call(raw: str) -> None:
            try:
                async with db_module.async_session_factory() as db:
                    handler = Ocpp16Handler(charge_point_id, db)
                    response = await handler.handle_raw(raw)
                await websocket.send_text(response)
            except Exception:
                logger.exception(
                    "ocpp16.inbound_call_failed",
                    charge_point_id=charge_point_id,
                )

        try:
            while True:
                raw = await websocket.receive_text()
                try:
                    message_type, unique_id, _action, payload = protocol.parse_frame(raw)
                except (ValueError, TypeError):
                    await websocket.send_text(
                        protocol.call_error("0", "FormationViolation", "Invalid OCPP-J frame")
                    )
                    continue

                if message_type in (
                    protocol.MessageType.CALLRESULT,
                    protocol.MessageType.CALLERROR,
                ):
                    protocol.resolve_outbound_response(unique_id, message_type, payload)
                    continue

                # Handle inbound CALLs in the background so outbound CallResults
                # (e.g. SetChargingProfile) can be received while MeterValues runs.
                task = asyncio.create_task(_handle_inbound_call(raw))
                inbound_tasks.add(task)
                task.add_done_callback(inbound_tasks.discard)
        except WebSocketDisconnect:
            for task in list(inbound_tasks):
                task.cancel()
            protocol.fail_pending_for_charge_point(
                charge_point_id,
                ChargerOfflineError(charge_point_id),
            )
            registry.unregister(charge_point_id, websocket)
            async with db_module.async_session_factory() as db:
                await ChargerService(db).mark_disconnected(charge_point_id)
            logger.info("ocpp16.disconnected", charge_point_id=charge_point_id)
        except Exception:
            for task in list(inbound_tasks):
                task.cancel()
            logger.exception("ocpp16.connection_error", charge_point_id=charge_point_id)
            protocol.fail_pending_for_charge_point(
                charge_point_id,
                ChargerOfflineError(charge_point_id),
            )
            registry.unregister(charge_point_id, websocket)
            try:
                async with db_module.async_session_factory() as db:
                    await ChargerService(db).mark_disconnected(charge_point_id)
            except Exception:
                logger.exception(
                    "ocpp16.mark_disconnected_failed",
                    charge_point_id=charge_point_id,
                )
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
