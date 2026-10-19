import asyncio
from unittest.mock import patch

import pytest
from prometheus_client import CollectorRegistry, generate_latest

from app.metrics.metrics import GatewayMetrics
from parser.parser import crc16_modbus


def test_gateway_metrics_counters_and_histogram():
    registry = CollectorRegistry()
    metrics = GatewayMetrics(registry=registry)

    metrics.connections_active.inc()
    metrics.packet_ok()
    metrics.packet_bad()
    metrics.packet_redis_error()
    metrics.packet_duration_seconds.observe(0.01)

    payload = generate_latest(registry).decode()

    assert 'tcp_gateway_packets_total{result="ok"} 1.0' in payload
    assert 'tcp_gateway_packets_total{result="bad_packet"} 1.0' in payload
    assert 'tcp_gateway_packets_total{result="redis_error"} 1.0' in payload
    assert "tcp_gateway_connections_active 1.0" in payload
    assert "tcp_gateway_packet_duration_seconds_count 1.0" in payload


@pytest.mark.asyncio
async def test_handle_client_records_ok_metrics(tcp_server, fake_metrics):
    host, port = tcp_server
    body = b"imei:22222;datetime:12-00-00;speed_can:0;speed_engine:0;dut_lvl:0"
    payload = body + f";crc16:{crc16_modbus(body)}".encode()

    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(payload)
        await writer.drain()
        ack = await reader.read(1024)
    finally:
        writer.close()
        await writer.wait_closed()

    assert ack == b"'ack':'ok';"
    assert fake_metrics.ok == 1
    assert fake_metrics.bad == 0
    assert fake_metrics.redis_error == 0
    # finally на сервере может догнать чуть позже закрытия клиента
    for _ in range(50):
        if fake_metrics.connections_active_value == 0:
            break
        await asyncio.sleep(0.01)
    assert fake_metrics.connections_active_value == 0
    assert len(fake_metrics.observed_durations) == 1
    assert fake_metrics.observed_durations[0] >= 0


@pytest.mark.asyncio
async def test_handle_client_records_bad_packet_metrics(tcp_server, fake_metrics):
    host, port = tcp_server
    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(b"garbage-packet\n")
        await writer.drain()
        ack = await reader.read(1024)
    finally:
        writer.close()
        await writer.wait_closed()

    assert ack == b"'ack':'error_bad_packet';"
    assert fake_metrics.bad == 1
    assert fake_metrics.ok == 0


@pytest.mark.asyncio
async def test_handle_client_records_redis_error_metrics(
    tcp_server, fake_metrics, fake_publisher
):
    host, port = tcp_server

    async def boom(_message: dict) -> None:
        raise RuntimeError("redis unavailable")

    fake_publisher.publish = boom  # type: ignore[method-assign]

    fake_message = {"imei": "33333", "speed": "1"}
    with patch("app.main.parse_request", return_value=fake_message):
        reader, writer = await asyncio.open_connection(host, port)
        try:
            writer.write(b"anything\n")
            await writer.drain()
            ack = await reader.read(1024)
        finally:
            writer.close()
            await writer.wait_closed()

    assert ack == b"'ack':'internal_error';"
    assert fake_metrics.redis_error == 1
    assert fake_metrics.ok == 0
