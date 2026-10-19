import asyncio

from faststream.redis import RedisBroker

from core.core import Settings


class PointPublisher:
    def __init__(self, broker: RedisBroker, settings: Settings) -> None:
        self._broker = broker
        self._stream = settings.redis_stream_name
        self._max_len = settings.redis_stream_max_len
        # backpressure: не открываем больше конкурентных XADD, чем размер pool
        self._sem = asyncio.Semaphore(max(1, settings.redis_max_connections))

    async def publish(self, message: dict) -> None:
        imei = message.get("imei")
        if not imei:
            raise ValueError("imei is required")

        async with self._sem:
            await self._broker.publish(
                message=message,
                stream=self._stream,
                maxlen=self._max_len,
            )
