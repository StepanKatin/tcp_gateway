from prometheus_client import CollectorRegistry, Counter


def create_inserted_rows_counter(registry: CollectorRegistry) -> Counter:
    return Counter(
        "loader_inserted_rows_total",
        "Rows successfully inserted into ClickHouse",
        registry=registry,
    )