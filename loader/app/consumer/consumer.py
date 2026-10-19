from core.core import Settings
from domain.ports import Repository
from faststream.redis import RedisBroker, StreamSub
from prometheus_client import Counter


def register_consumers(
    broker: RedisBroker,
    settings: Settings,
    repository: Repository,
    inserted_rows: Counter,
) -> None:
    @broker.subscriber(
        stream=StreamSub(
            settings.redis_stream_name,
            group=settings.redis_consumer_group,
            consumer=settings.redis_consumer_name,
            batch=True,
            max_records=settings.batch_stream_len,
            polling_interval=settings.stream_polling_interval_ms,
        )
    )
    async def catch_points(messages: list[dict]) -> None:
        # FS отдал 1..max_records за один XREAD — сразу в CH
        await repository.insert_many(messages)
        inserted_rows.inc(len(messages))
