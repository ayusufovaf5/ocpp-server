from functools import lru_cache

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

DEV_API_KEY = "dev-api-key-change-me"
DEV_OCPP_ALLOWLIST = "*"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    api_host: str = Field(default="0.0.0.0", alias="API_HOST")
    api_port: int = Field(default=8000, alias="API_PORT")
    ocpp16_port: int = Field(default=9000, alias="OCPP16_PORT")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_format: str = Field(default="json", alias="LOG_FORMAT")

    pg_host: str = Field(default="localhost", alias="PG_HOST")
    pg_port: int = Field(default=5433, alias="PG_PORT")
    pg_user: str = Field(default="opencpo", alias="PG_USER")
    pg_password: str = Field(default="ocpp123", alias="PG_PASSWORD")
    pg_database: str = Field(default="opencpo", alias="PG_DATABASE")
    database_url: str | None = Field(default=None, alias="DATABASE_URL")

    redis_url: str = Field(default="redis://localhost:6379/0", alias="REDIS_URL")
    redis_state_ttl_seconds: int = Field(default=86400, alias="REDIS_STATE_TTL_SECONDS")

    api_key: str = Field(default=DEV_API_KEY, alias="API_KEY")
    ocpp_charge_point_allowlist: str = Field(
        default=DEV_OCPP_ALLOWLIST,
        alias="OCPP_CHARGE_POINT_ALLOWLIST",
    )

    ocpp_heartbeat_interval: int = Field(default=60, alias="OCPP_HEARTBEAT_INTERVAL")
    heartbeat_timeout_seconds: int = Field(default=120, alias="HEARTBEAT_TIMEOUT_SECONDS")
    heartbeat_check_interval_seconds: int = Field(
        default=30,
        alias="HEARTBEAT_CHECK_INTERVAL_SECONDS",
    )
    offline_session_grace_period_seconds: int = Field(
        default=300,
        alias="OFFLINE_SESSION_GRACE_PERIOD_SECONDS",
    )
    outbound_call_timeout_seconds: float = Field(
        default=15,
        alias="OUTBOUND_CALL_TIMEOUT_SECONDS",
    )

    stale_status_remap_window_seconds: int = Field(
        default=60,
        alias="STALE_STATUS_REMAP_WINDOW_SECONDS",
    )
    evpoint_live_tx_grace_seconds: int = Field(
        default=15,
        alias="EVPOINT_LIVE_TX_GRACE_SECONDS",
    )

    evpoint_live_update_url: str = Field(
        default="http://localhost:5055/Charging/ocpp-live-update",
        alias="EVPOINT_LIVE_UPDATE_URL",
    )
    evpoint_ca_bundle: str | None = Field(default=None, alias="EVPOINT_CA_BUNDLE")
    evpoint_ssl_verify: bool = Field(default=True, alias="EVPOINT_SSL_VERIFY")
    evpoint_push_max_attempts: int = Field(default=3, alias="EVPOINT_PUSH_MAX_ATTEMPTS")
    evpoint_push_backoff_seconds: float = Field(
        default=0.5,
        alias="EVPOINT_PUSH_BACKOFF_SECONDS",
    )
    evpoint_push_timeout_seconds: float = Field(
        default=5.0,
        alias="EVPOINT_PUSH_TIMEOUT_SECONDS",
    )

    app_name: str = "opencpo-core"
    app_version: str = "0.2.0"

    @computed_field
    @property
    def sqlalchemy_database_url(self) -> str:
        if self.database_url:
            return self.database_url
        return (
            f"postgresql+asyncpg://{self.pg_user}:{self.pg_password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_database}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
