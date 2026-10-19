from collections.abc import AsyncIterator
from logging import INFO, Formatter, Logger, StreamHandler, getLogger

from dishka import Provider, Scope, provide
from faststream.redis import RedisBroker
from redis.asyncio import Redis

from connection.context import ConnectionContext
from core.core import Settings, get_settings
from metrics.metrics import GatewayMetrics
from publisher.publisher import PointPublisher


class GatewayProvider(Provider):
    """Deps for app lifetime."""

    @provide(scope=Scope.APP)
    def settings(self) -> Settings:
        return get_settings()

    @provide(scope=Scope.APP)
    def logger(self) -> Logger:
        logger = getLogger("tcp_gateway")
        logger.setLevel(INFO)
        if not logger.handlers:
            handler = StreamHandler()
            handler.setFormatter(
                Formatter("%(asctime)s - %(levelname)s - %(message)s")
            )
            logger.addHandler(handler)
        return logger

    @provide(scope=Scope.APP)
    async def redis_client(self, settings: Settings) -> AsyncIterator[Redis]:
        """Клиент для /ready и стартового ping."""
        client = Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            max_connections=settings.redis_max_connections,
        )
        try:
            yield client
        finally:
            await client.aclose()

    @provide(scope=Scope.APP)
    async def redis_broker(self, settings: Settings) -> AsyncIterator[RedisBroker]:
        """Брокер для publish в Redis Stream."""
        broker = RedisBroker(url=settings.redis_url)
        await broker.start()
        try:
            yield broker
        finally:
            await broker.stop()

    @provide(scope=Scope.APP)
    def point_publisher(
        self,
        broker: RedisBroker,
        settings: Settings,
    ) -> PointPublisher:
        return PointPublisher(broker, settings)

    @provide(scope=Scope.REQUEST)
    def connection_context(self) -> ConnectionContext:
        return ConnectionContext(peer=None)

    @provide(scope=Scope.APP)
    def metrics(self) -> GatewayMetrics:
        return GatewayMetrics()
