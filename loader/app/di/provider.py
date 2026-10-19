from logging import INFO, Formatter, Logger, StreamHandler, getLogger

from clickhouse_connect.driver.client import Client
from dishka import Provider, Scope, provide
from faststream.redis import RedisBroker
from faststream.redis.prometheus import RedisPrometheusMiddleware
from prometheus_client import CollectorRegistry

from core.core import Settings, get_settings
from domain.ports import Repository
from repository.adapter import ClickHouseRepo, create_clickhouse_client


class LoaderContainer(Provider):
    """Deps for app lifetime."""

    @provide(scope=Scope.APP)
    def settings(self) -> Settings:
        return get_settings()

    @provide(scope=Scope.APP)
    def logger(self) -> Logger:
        logger = getLogger("loader")
        logger.setLevel(INFO)
        if not logger.handlers:
            handler = StreamHandler()
            handler.setFormatter(
                Formatter("%(asctime)s - %(levelname)s - %(message)s")
            )
            logger.addHandler(handler)
        return logger

    @provide(scope=Scope.APP)
    def metrics_registry(self) -> CollectorRegistry:
        return CollectorRegistry()

    @provide(scope=Scope.APP)
    def redis_broker(
        self,
        settings: Settings,
        registry: CollectorRegistry,
    ) -> RedisBroker:
        # start/stop делает AsgiFastStream / app.run
        return RedisBroker(
            url=settings.redis_url,
            middlewares=(RedisPrometheusMiddleware(registry=registry),),
        )

    @provide(scope=Scope.APP)
    def clickhouse_client(self, settings: Settings) -> Client:
        # один HTTP-клиент на процесс; для batch-insert пул не обязателен
        return create_clickhouse_client(settings)

    @provide(scope=Scope.APP)
    def repository(
        self,
        client: Client,
        settings: Settings,
        logger: Logger,
    ) -> Repository:
        return ClickHouseRepo(client, settings, logger)
