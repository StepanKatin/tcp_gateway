from pydantic_settings import SettingsConfigDict, BaseSettings
from functools import lru_cache
from pydantic import Field

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", 
                                      env_file_encoding="utf8",
                                      case_sensitive=False,
                                      extra="ignore",)

    redis_url: str = Field(default="redis://localhost:6379", alias="REDIS_URL")
    tcp_server: str = Field(default="http://localhost:8000", alias="TCP_SERVER_URL")
    tcp_timeout: int = Field(default=60 ,alias="TCP_TIMEOUT")
    tcp_host: str = "0.0.0.0"
    tcp_port: int = 8000
    redis_stream_name: str = Field(default="telemetry.points", alias="REDIS_STREAM_NAME")
    redis_stream_max_len: int = Field(default=100_000, alias="REDIS_STREAM_MAXLEN")
    # redis-py 8 asyncio pool default = 100; под нагрузку нужно больше
    redis_max_connections: int = Field(default=512, alias="REDIS_MAX_CONNECTIONS")
    http_host: str = Field(default="0.0.0.0", alias="HTTP_HOST")
    http_port: int = Field(default=8001, alias="HTTP_PORT")

@lru_cache
def get_settings():
    return Settings()
