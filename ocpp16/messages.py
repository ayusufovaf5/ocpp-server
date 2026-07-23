from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from db.time import utc_now_iso

__all__ = [
    "AuthorizeConf",
    "AuthorizeReq",
    "BootNotificationConf",
    "BootNotificationReq",
    "HeartbeatConf",
    "HeartbeatReq",
    "IdTagInfo",
    "MeterValuesConf",
    "MeterValuesReq",
    "OcppMeterValue",
    "SampledValue",
    "StartTransactionConf",
    "StartTransactionReq",
    "StatusNotificationConf",
    "StatusNotificationReq",
    "StopTransactionConf",
    "StopTransactionReq",
    "dump_ocpp",
    "utc_now_iso",
]


class CamelModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")


class BootNotificationReq(CamelModel):
    charge_point_vendor: str = Field(alias="chargePointVendor", max_length=255)
    charge_point_model: str = Field(alias="chargePointModel", max_length=255)
    charge_point_serial_number: str | None = Field(default=None, alias="chargePointSerialNumber")
    charge_box_serial_number: str | None = Field(default=None, alias="chargeBoxSerialNumber")
    firmware_version: str | None = Field(default=None, alias="firmwareVersion")
    iccid: str | None = None
    imsi: str | None = None
    meter_type: str | None = Field(default=None, alias="meterType")
    meter_serial_number: str | None = Field(default=None, alias="meterSerialNumber")


class BootNotificationConf(CamelModel):
    status: Literal["Accepted", "Pending", "Rejected"]
    current_time: str = Field(alias="currentTime")
    interval: int


class HeartbeatReq(CamelModel):
    pass


class HeartbeatConf(CamelModel):
    current_time: str = Field(alias="currentTime")


class StatusNotificationReq(CamelModel):
    connector_id: int = Field(alias="connectorId")
    error_code: str = Field(alias="errorCode")
    status: str
    info: str | None = None
    timestamp: str | None = None
    vendor_id: str | None = Field(default=None, alias="vendorId")
    vendor_error_code: str | None = Field(default=None, alias="vendorErrorCode")


class StatusNotificationConf(CamelModel):
    pass


class AuthorizeReq(CamelModel):
    id_tag: str = Field(alias="idTag", max_length=255)


class IdTagInfo(CamelModel):
    status: str
    expiry_date: str | None = Field(default=None, alias="expiryDate")
    parent_id_tag: str | None = Field(default=None, alias="parentIdTag")


class AuthorizeConf(CamelModel):
    id_tag_info: IdTagInfo = Field(alias="idTagInfo")


class StartTransactionReq(CamelModel):
    connector_id: int = Field(alias="connectorId")
    id_tag: str = Field(alias="idTag", max_length=255)
    meter_start: int = Field(alias="meterStart")
    timestamp: str
    reservation_id: int | None = Field(default=None, alias="reservationId")


class StartTransactionConf(CamelModel):
    transaction_id: int = Field(alias="transactionId")
    id_tag_info: IdTagInfo = Field(alias="idTagInfo")


class SampledValue(CamelModel):
    value: str
    context: str | None = None
    format: str | None = None
    measurand: str | None = None
    phase: str | None = None
    location: str | None = None
    unit: str | None = None


class OcppMeterValue(CamelModel):
    timestamp: str
    sampled_value: list[SampledValue] = Field(alias="sampledValue")


class MeterValuesReq(CamelModel):
    connector_id: int = Field(alias="connectorId")
    meter_value: list[OcppMeterValue] = Field(alias="meterValue")
    transaction_id: int | None = Field(default=None, alias="transactionId")


class MeterValuesConf(CamelModel):
    pass


class StopTransactionReq(CamelModel):
    meter_stop: int = Field(alias="meterStop")
    timestamp: str
    transaction_id: int = Field(alias="transactionId")
    reason: str | None = None
    id_tag: str | None = Field(default=None, alias="idTag", max_length=255)
    transaction_data: list[OcppMeterValue] | None = Field(default=None, alias="transactionData")


class StopTransactionConf(CamelModel):
    id_tag_info: IdTagInfo | None = Field(default=None, alias="idTagInfo")


def dump_ocpp(model: BaseModel) -> dict[str, Any]:
    return model.model_dump(by_alias=True, exclude_none=True)
