import asyncio
import logging
from functools import partial

import pytest_asyncio

from app.connection.context import ConnectionContext
from app.core.core import get_settings
from app.main import handle_client


class FakePointPublisher:
    def __init__(self) -> None:
        self.lists: dict[str, list[dict]] = {}

    async def publish(self, message: dict) -> None:
        imei = message.get("imei")
        if not imei:
            raise ValueError("imei is required")
        self.lists.setdefault(str(imei), []).append(message)


class _NopMetric:
    def __init__(self, parent: "FakeMetrics | None" = None, kind: str = "") -> None:
        self._parent = parent
        self._kind = kind

    def inc(self, *_args, **_kwargs) -> None:
        if self._parent is not None and self._kind == "connections_active":
            self._parent.connections_active_value += 1

    def dec(self, *_args, **_kwargs) -> None:
        if self._parent is not None and self._kind == "connections_active":
            self._parent.connections_active_value -= 1

    def observe(self, value: float, *_args, **_kwargs) -> None:
        if self._parent is not None:
            self._parent.observed_durations.append(value)

    def labels(self, **_kwargs) -> "_NopMetric":
        return self


class FakeMetrics:
    """Счётчики для ассертов; без prometheus REGISTRY."""

    def __init__(self) -> None:
        self.ok = 0
        self.bad = 0
        self.redis_error = 0
        self.connections_active_value = 0
        self.observed_durations: list[float] = []
        self.connections_active = _NopMetric(self, "connections_active")
        self.packet_duration_seconds = _NopMetric(self, "duration")
        self.packets_total = _NopMetric()

    def packet_ok(self) -> None:
        self.ok += 1

    def packet_bad(self) -> None:
        self.bad += 1

    def packet_redis_error(self) -> None:
        self.redis_error += 1


@pytest_asyncio.fixture
async def fake_publisher():
    pub = FakePointPublisher()
    yield pub
    pub.lists.clear()


@pytest_asyncio.fixture
async def fake_metrics():
    yield FakeMetrics()


@pytest_asyncio.fixture
async def tcp_server(fake_publisher, fake_metrics):
    test_logger = logging.getLogger("test_logger")
    settings = get_settings()
    ctx = ConnectionContext(peer=("127.0.0.1", 0))

    test_handler = partial(
        handle_client,
        logger=test_logger,
        settings=settings,
        publisher=fake_publisher,
        ctx=ctx,
        metrics=fake_metrics,
    )

    server = await asyncio.start_server(test_handler, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()

    yield host, port

    server.close()
    await server.wait_closed()
