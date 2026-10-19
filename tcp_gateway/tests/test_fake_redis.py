import asyncio

import pytest
from unittest.mock import patch


@pytest.mark.asyncio
async def test_server_writes_to_redis(tcp_server, fake_publisher):
    # --- ARRANGE ---
    host, port = tcp_server
    reader, writer = await asyncio.open_connection(host, port)

    fake_message = {"imei": "129464373", "speed": "77"}
    test_payload = b"any_raw_bytes_for_test\n"

    with patch("app.main.parse_request", return_value=fake_message):
        try:
            # --- ACT ---
            writer.write(test_payload)
            await writer.drain()
            ack = await reader.read(1024)
        finally:
            writer.close()
            await writer.wait_closed()

    # --- ASSERT ---
    assert ack == b"'ack':'ok';"
    assert "129464373" in fake_publisher.lists
    assert fake_publisher.lists["129464373"][0]["imei"] == "129464373"


@pytest.mark.asyncio
async def test_server_concurrent_redis_writes(tcp_server, fake_publisher):
    # --- ARRANGE ---
    host, port = tcp_server
    client_count = 50

    async def run_client(client_id: int):
        r, w = await asyncio.open_connection(host, port)
        try:
            w.write(f"raw_{client_id}\n".encode())
            await w.drain()
            await r.read(1024)
        finally:
            w.close()
            await w.wait_closed()

    def mock_parser_side_effect(data: bytes) -> dict:
        raw_str = data.decode("utf-8", errors="ignore").strip()
        client_id = raw_str.split("_")[1]
        return {"imei": f"imei_{client_id}"}

    # --- ACT ---
    with patch("app.main.parse_request", side_effect=mock_parser_side_effect):
        tasks = [run_client(i) for i in range(client_count)]
        await asyncio.gather(*tasks)

    # --- ASSERT ---
    assert len(fake_publisher.lists) == client_count
    for i in range(client_count):
        assert f"imei_{i}" in fake_publisher.lists
        assert len(fake_publisher.lists[f"imei_{i}"]) == 1
