import logging

import pytest

from core.core import Settings


@pytest.fixture
def settings() -> Settings:
    return Settings(
        REDIS_URL="redis://localhost:6379/0",
        CLICKHOUSE_HOST="localhost",
        CLICKHOUSE_PORT=8123,
        CLICKHOUSE_USER="default",
        CLICKHOUSE_PASSWORD="",
        CLICKHOUSE_DATABASE="telemetry",
        BATCH_STREAM_LEN=500,
        STREAM_POLLING_INTERVAL_MS=500,
        HTTP_HOST="0.0.0.0",
        HTTP_PORT=8002,
    )


@pytest.fixture
def logger() -> logging.Logger:
    return logging.getLogger("loader_test")
