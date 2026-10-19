"""Интеграция против живого docker compose (Redis Stream + loader + ClickHouse).

Запуск из loader/:
  pytest -m integration -q
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator

import clickhouse_connect
import pytest
from faststream.redis import RedisBroker
from redis.asyncio import Redis

STREAM = os.getenv("REDIS_STREAM_NAME", "telemetry.points")
REDIS_URL = os.getenv("REDIS_URL_HOST", "redis://localhost:6379/0")
CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "admin")
CH_DB = os.getenv("CLICKHOUSE_DATABASE", "telemetry")


def _ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB,
    )


async def _stack_ready() -> None:
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        await redis.ping()
    finally:
        await redis.aclose()

    client = _ch_client()
    try:
        client.query("SELECT 1")
        tables = {
            row[0]
            for row in client.query(
                "SELECT name FROM system.tables WHERE database = {db:String}",
                parameters={"db": CH_DB},
            ).result_rows
        }
    finally:
        client.close()

    required = {"telemetry_raw", "telemetry_clean", "telemetry_clean_mv"}
    missing = required - tables
    if missing:
        raise RuntimeError(f"ClickHouse schema incomplete, missing: {sorted(missing)}")


@pytest.fixture
async def require_stack() -> AsyncIterator[None]:
    try:
        await _stack_ready()
    except Exception as exc:
        pytest.skip(f"compose stack not ready: {exc}")
    yield


async def _wait_ch_count(imei: str, table: str, *, timeout: float = 15.0) -> int:
    client = _ch_client()
    deadline = asyncio.get_running_loop().time() + timeout
    try:
        while True:
            rows = client.query(
                f"SELECT count() FROM {CH_DB}.{table} WHERE imei = {{imei:String}}",
                parameters={"imei": imei},
            ).result_rows
            count = int(rows[0][0])
            if count > 0:
                return count
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(
                    f"no rows for imei={imei!r} in {table} within {timeout}s"
                )
            await asyncio.sleep(0.25)
    finally:
        client.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_stream_message_lands_in_clickhouse(require_stack):
    """XADD через FastStream RedisBroker → loader → raw + clean (MV)."""
    imei = f"itest-{uuid.uuid4().hex[:12]}"
    payload = {
        "imei": imei,
        "datetime": "12-00-00",
        "speed_can": 42.0,
        "speed_engine": 1500.0,
        "dut_lvl": 33.5,
        "crc16": "1",
    }

    broker = RedisBroker(url=REDIS_URL)
    await broker.start()
    try:
        await broker.publish(message=payload, stream=STREAM)
    finally:
        await broker.stop()

    raw_count = await _wait_ch_count(imei, "telemetry_raw")
    clean_count = await _wait_ch_count(imei, "telemetry_clean")

    assert raw_count == 1
    assert clean_count == 1
