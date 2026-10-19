from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram


class GatewayMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        reg = registry if registry is not None else REGISTRY
        self.packets_total = Counter(
            "tcp_gateway_packets_total",
            "Processed TCP packets",
            labelnames=("result",),
            registry=reg,
        )
        self.connections_active = Gauge(
            "tcp_gateway_connections_active",
            "Active TCP connections",
            registry=reg,
        )
        self.packet_duration_seconds = Histogram(
            "tcp_gateway_packet_duration_seconds",
            "Time from packet read to ack",
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
            registry=reg,
        )
    def packet_ok(self) -> None:
        self.packets_total.labels(result="ok").inc()
    def packet_bad(self) -> None:
        self.packets_total.labels(result="bad_packet").inc()
    def packet_redis_error(self) -> None:
        self.packets_total.labels(result="redis_error").inc()