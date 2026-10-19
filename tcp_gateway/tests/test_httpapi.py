from unittest.mock import AsyncMock

import pytest
from aiohttp.test_utils import TestClient, TestServer

from app.httpapi.app import create_http_app


@pytest.fixture
async def http_client():
    redis = AsyncMock()
    redis.ping = AsyncMock(return_value=True)
    app = create_http_app(redis)
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()
    try:
        yield client, redis
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_health_ok(http_client):
    client, _redis = http_client
    resp = await client.get("/health")
    assert resp.status == 200
    assert await resp.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_ready_ok(http_client):
    client, redis = http_client
    resp = await client.get("/ready")
    assert resp.status == 200
    assert await resp.json() == {"status": "ready"}
    redis.ping.assert_awaited()


@pytest.mark.asyncio
async def test_ready_not_ready_when_redis_fails(http_client):
    client, redis = http_client
    redis.ping = AsyncMock(side_effect=ConnectionError("redis down"))

    resp = await client.get("/ready")
    body = await resp.json()

    assert resp.status == 503
    assert body["status"] == "not_ready"
    assert "redis down" in body["error"]


@pytest.mark.asyncio
async def test_metrics_endpoint(http_client):
    client, _redis = http_client
    resp = await client.get("/metrics")
    text = await resp.text()

    assert resp.status == 200
    assert "text/plain" in resp.headers["Content-Type"]
    # стандартные process_* метрики prometheus_client всегда есть
    assert "python_info" in text or "process_" in text
