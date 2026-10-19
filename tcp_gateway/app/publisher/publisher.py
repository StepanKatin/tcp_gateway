from core.core import Settings
from faststream.redis import RedisBroker

class PointPublisher:
    def __init__(self, broker: RedisBroker, settings: Settings) -> None:
        self._broker = broker
        self._stream = settings.redis_stream_name
        self._max_len = settings.redis_stream_max_len

    async def publish(self, message: dict) -> None:
        imei = message.get("imei")
        if not imei:
            raise ValueError("imei is required")

        await self._broker.publish(
            message=message,
            stream=self._stream,
            maxlen=self._max_len,
        )


    
    
