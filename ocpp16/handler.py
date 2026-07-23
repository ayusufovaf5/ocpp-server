from typing import Any

import structlog
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from config import get_settings
from ocpp16 import protocol
from ocpp16.messages import (
    AuthorizeConf,
    AuthorizeReq,
    BootNotificationConf,
    BootNotificationReq,
    HeartbeatConf,
    HeartbeatReq,
    IdTagInfo,
    MeterValuesConf,
    MeterValuesReq,
    StartTransactionConf,
    StartTransactionReq,
    StatusNotificationConf,
    StatusNotificationReq,
    StopTransactionConf,
    StopTransactionReq,
    dump_ocpp,
    utc_now_iso,
)
from services.charger_service import ChargerService
from services.errors import (
    MissingOcppTransactionIdError,
    UnknownChargerError,
    UnsupportedActionError,
)
from services.session_service import SessionService

logger = structlog.get_logger(__name__)


class Ocpp16Handler:
    def __init__(self, charge_point_id: str, db: AsyncSession) -> None:
        self.charge_point_id = charge_point_id
        self._chargers = ChargerService(db)
        self._sessions = SessionService(db)
        self._settings = get_settings()

    async def handle_raw(self, raw: str | bytes) -> str:
        try:
            message_type, unique_id, action, payload = protocol.parse_frame(raw)
        except (ValueError, TypeError):
            return protocol.call_error("0", "FormationViolation", "Invalid OCPP-J frame")

        if message_type != protocol.MessageType.CALL:
            return protocol.call_error(
                unique_id,
                "MessageTypeNotSupported",
                "Only CALL frames from charge points are handled",
            )

        try:
            result = await self.dispatch(action or "", payload or {})
            return protocol.call_result(unique_id, result)
        except ValidationError:
            logger.warning(
                "ocpp.validation_error",
                charge_point_id=self.charge_point_id,
                action=action,
            )
            return protocol.call_error(unique_id, "FormationViolation", "Invalid payload")
        except UnsupportedActionError:
            return protocol.call_error(
                unique_id, "NotImplemented", f"Action not supported: {action}"
            )
        except UnknownChargerError:
            return protocol.call_error(
                unique_id,
                "InternalError",
                "Charge point is not registered; send BootNotification first",
            )
        except MissingOcppTransactionIdError as exc:
            logger.error(
                "ocpp.missing_transaction_id",
                charge_point_id=self.charge_point_id,
                session_id=exc.session_id,
            )
            return protocol.call_error(
                unique_id,
                "InternalError",
                "StartTransaction did not assign ocpp_transaction_id",
            )
        except Exception:
            logger.exception(
                "ocpp.handler_error",
                charge_point_id=self.charge_point_id,
                action=action,
            )
            return protocol.call_error(unique_id, "InternalError", "Internal server error")

    async def dispatch(self, action: str, payload: dict[str, Any]) -> dict[str, Any]:
        handlers = {
            "BootNotification": self._boot_notification,
            "Heartbeat": self._heartbeat,
            "StatusNotification": self._status_notification,
            "Authorize": self._authorize,
            "StartTransaction": self._start_transaction,
            "MeterValues": self._meter_values,
            "StopTransaction": self._stop_transaction,
        }
        handler = handlers.get(action)
        if handler is None:
            raise UnsupportedActionError(action)
        return await handler(payload)

    async def _boot_notification(self, payload: dict[str, Any]) -> dict[str, Any]:
        req = BootNotificationReq.model_validate(payload)
        await self._chargers.register_boot(
            charge_point_id=self.charge_point_id,
            vendor=req.charge_point_vendor,
            model=req.charge_point_model,
            firmware_version=req.firmware_version,
        )
        return dump_ocpp(
            BootNotificationConf(
                status="Accepted",
                currentTime=utc_now_iso(),
                interval=self._settings.ocpp_heartbeat_interval,
            )
        )

    async def _heartbeat(self, payload: dict[str, Any]) -> dict[str, Any]:
        HeartbeatReq.model_validate(payload or {})
        await self._chargers.heartbeat(self.charge_point_id)
        return dump_ocpp(HeartbeatConf(currentTime=utc_now_iso()))

    async def _status_notification(self, payload: dict[str, Any]) -> dict[str, Any]:
        req = StatusNotificationReq.model_validate(payload)
        if req.connector_id != 0:
            await self._chargers.update_status(
                self.charge_point_id,
                req.status,
                connector_id=req.connector_id,
            )
        else:
            await self._chargers.heartbeat(
                self.charge_point_id,
                connector_id=req.connector_id,
                status=req.status,
            )
        return dump_ocpp(StatusNotificationConf())

    async def _authorize(self, payload: dict[str, Any]) -> dict[str, Any]:
        AuthorizeReq.model_validate(payload)
        return dump_ocpp(AuthorizeConf(idTagInfo=IdTagInfo(status="Accepted")))

    async def _start_transaction(self, payload: dict[str, Any]) -> dict[str, Any]:
        req = StartTransactionReq.model_validate(payload)
        charging = await self._sessions.start_transaction(
            charge_point_id=self.charge_point_id,
            connector_id=req.connector_id,
            id_tag=req.id_tag,
            meter_start=req.meter_start,
            timestamp=req.timestamp,
        )
        if charging.ocpp_transaction_id is None:
            raise MissingOcppTransactionIdError(session_id=charging.id)
        return dump_ocpp(
            StartTransactionConf(
                transactionId=charging.ocpp_transaction_id,
                idTagInfo=IdTagInfo(status="Accepted"),
            )
        )

    async def _meter_values(self, payload: dict[str, Any]) -> dict[str, Any]:
        req = MeterValuesReq.model_validate(payload)
        await self._sessions.record_meter_values(
            charge_point_id=self.charge_point_id,
            connector_id=req.connector_id,
            transaction_id=req.transaction_id,
            meter_value=[mv.model_dump(by_alias=True) for mv in req.meter_value],
        )
        return dump_ocpp(MeterValuesConf())

    async def _stop_transaction(self, payload: dict[str, Any]) -> dict[str, Any]:
        req = StopTransactionReq.model_validate(payload)
        transaction_data = None
        if req.transaction_data:
            transaction_data = [
                mv.model_dump(by_alias=True) for mv in req.transaction_data
            ]
        await self._sessions.stop_transaction(
            charge_point_id=self.charge_point_id,
            transaction_id=req.transaction_id,
            meter_stop=req.meter_stop,
            timestamp=req.timestamp,
            transaction_data=transaction_data,
        )
        return dump_ocpp(StopTransactionConf())
