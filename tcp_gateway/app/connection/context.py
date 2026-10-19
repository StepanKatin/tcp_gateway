from dataclasses import dataclass, field
from time import monotonic

@dataclass
class ConnectionContext:
    peer: tuple | str | None
    started_at: float = field(default_factory=monotonic)
    imei: str | None = None
    packets_ok: int = 0