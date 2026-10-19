from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from functools import lru_cache

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env",
                                      env_file_encoding="utf8",
                                      case_sensitive=False,
                                      extra="ignore",)
    redis_url: str = Field(default="redis://localhost:6379", alias="REDIS_URL")
    redis_stream_name: str = Field(default="telemetry.points", alias="REDIS_STREAM_NAME")
    redis_consumer_group: str = Field(default="loader", alias="REDIS_CONSUMER_GROUP")
    redis_consumer_name: str = Field(default="loader-1", alias="REDIS_CONSUMER_NAME")
    clickhouse_host: str = Field(default="clickhouse", alias="CLICKHOUSE_HOST")
    clickhouse_port: int = Field(default=8123, alias="CLICKHOUSE_PORT")
    clickhouse_user: str = Field(default="default", alias="CLICKHOUSE_USER")
    clickhouse_password: str = Field(default="", alias="CLICKHOUSE_PASSWORD")
    clickhouse_database: str = Field(default="telemetry", alias="CLICKHOUSE_DATABASE")
    # схема A: размер пачки = COUNT XREAD; polling = BLOCK ms
    batch_stream_len: int = Field(default=500, alias="BATCH_STREAM_LEN")
    stream_polling_interval_ms: int = Field(
        default=500,
        alias="STREAM_POLLING_INTERVAL_MS",
    )
    http_host: str = Field(default="0.0.0.0", alias="HTTP_HOST")
    http_port: int = Field(default=8002, alias="HTTP_PORT")

@lru_cache
def get_settings() -> Settings:
    return Settings()