from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db import Base


class Charger(Base):
    __tablename__ = "chargers"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    charge_point_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    vendor: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    model: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    firmware_version: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[str] = mapped_column(String(64), nullable=False, default="Unknown")
    connector_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    last_heartbeat: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    disconnected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    sessions: Mapped[list["ChargingSession"]] = relationship(back_populates="charger")
    connector_statuses: Mapped[list["ConnectorStatus"]] = relationship(
        back_populates="charger"
    )


class ConnectorStatus(Base):
    __tablename__ = "connector_status"
    __table_args__ = (
        UniqueConstraint(
            "charger_id",
            "connector_id",
            name="uq_connector_status_charger_connector",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    charger_id: Mapped[int] = mapped_column(ForeignKey("chargers.id"), nullable=False)
    connector_id: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(64), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    charger: Mapped[Charger] = relationship(back_populates="connector_statuses")


class ChargingSession(Base):
    __tablename__ = "sessions"
    __table_args__ = (
        Index(
            "uq_sessions_active_charger_connector",
            "charger_id",
            "connector_id",
            unique=True,
            postgresql_where=text("status = 'Active'"),
            sqlite_where=text("status = 'Active'"),
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    charger_id: Mapped[int] = mapped_column(ForeignKey("chargers.id"), nullable=False)
    connector_id: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    id_tag: Mapped[str] = mapped_column(String(255), nullable=False)
    ocpp_transaction_id: Mapped[int | None] = mapped_column(Integer, unique=True, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    stopped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    meter_start: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    meter_stop: Mapped[int | None] = mapped_column(Integer, nullable=True)
    meter_stop_estimated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    end_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(64), nullable=False, default="Active")

    charger: Mapped[Charger] = relationship(back_populates="sessions")
    meter_values: Mapped[list["MeterValue"]] = relationship(back_populates="session")

    @property
    def effective_end_reason(self) -> str:
        return self.end_reason or "station_stop"


class MeterValue(Base):
    __tablename__ = "meter_values"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    session_id: Mapped[int] = mapped_column(ForeignKey("sessions.id"), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    unit: Mapped[str | None] = mapped_column(String(32), nullable=True)
    measurand: Mapped[str | None] = mapped_column(String(64), nullable=True)

    session: Mapped[ChargingSession] = relationship(back_populates="meter_values")
