from logging import Logger

from consumer.consumer import register_consumers
from core.core import Settings
from di.container import create_di_container
from domain.ports import Repository
from faststream.asgi import AsgiFastStream
from faststream.redis import RedisBroker
from prometheus_client import CollectorRegistry, make_asgi_app
from metrics.metrics import create_inserted_rows_counter


async def main() -> None:
    container = create_di_container()
    try:
        settings = await container.get(Settings)
        logger = await container.get(Logger)
        broker = await container.get(RedisBroker)
        repository = await container.get(Repository)
        registry = await container.get(CollectorRegistry)
        inserted_rows = create_inserted_rows_counter(registry)

        register_consumers(broker, settings, repository, inserted_rows)

        app = AsgiFastStream(
            broker,
            asgi_routes=[("/metrics", make_asgi_app(registry))],
        )

        @app.after_startup
        async def _on_startup() -> None:
            logger.info(
                "Loader started (scheme A): stream=%s group=%s "
                "max_records=%s poll_ms=%s metrics=%s:%s",
                settings.redis_stream_name,
                settings.redis_consumer_group,
                settings.batch_stream_len,
                settings.stream_polling_interval_ms,
                settings.http_host,
                settings.http_port,
            )

        await app.run(
            run_extra_options={
                "host": settings.http_host,
                "port": settings.http_port,
            },
        )
    finally:
        await container.close()


if __name__ == "__main__":
    try:
        import asyncio

        asyncio.run(main())
    except KeyboardInterrupt:
        print("Loader stopped by user")
