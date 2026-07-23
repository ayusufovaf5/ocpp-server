from functools import lru_cache

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    pg_port: int = Field(default=5432, alias="PG_PORT")
    pg_user: str = Field(default="opencpo", alias="PG_USER")
    pg_password: str = Field(default="ocpp123", alias="PG_PASSWORD")
    pg_database: str = Field(default="opencpo", alias="PG_DATABASE")
    database_url: str | None = Field(default=None, alias="DATABASE_URL")

    ocpp_heartbeat_interval: int = Field(default=60, alias="OCPP_HEARTBEAT_INTERVAL")
    heartbeat_timeout_seconds: int = Field(default=120, alias="HEARTBEAT_TIMEOUT_SECONDS")
    heartbeat_check_interval_seconds: int = Field(
        default=30,
        alias="HEARTBEAT_CHECK_INTERVAL_SECONDS",
    )

    app_name: str = "opencpo-core"
    app_version: str = "0.2.0"

    @computed_field  # type: ignore[prop-decorator]
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
