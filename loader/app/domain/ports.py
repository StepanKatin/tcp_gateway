from abc import ABC, abstractmethod


class Repository(ABC):
    @abstractmethod
    async def insert_many(self, messages: list[dict]) -> None:
        """Один INSERT пачки, которую уже собрал FastStream (XREAD COUNT)."""
