import asyncio
import pytest
# Импортируем вашу реальную функцию, чтобы посчитать CRC прямо в тесте
from parser.parser import crc16_modbus  

@pytest.mark.asyncio
async def test_echo_server_success(tcp_server):
    # --- ARRANGE ---
    host, port = tcp_server
    reader, writer = await asyncio.open_connection(host, port)
    
    # Полезная нагрузка
    payload_body = b'imei:11111;datetime:12-00-00;speed_can:0;speed_engine:0;dut_lvl:0'
    # Считаем реальный CRC от тела
    real_crc = crc16_modbus(payload_body)
    # Собираем финальный пакет
    test_payload = payload_body + f';crc16:{real_crc}'.encode()

    try:
        # --- ACT ---
        writer.write(test_payload)
        await writer.drain()
        response = await reader.read(1024)
    finally:
        writer.close()
        await writer.wait_closed()

    # --- ASSERT ---
    assert response == b"'ack':'ok';"


@pytest.mark.asyncio
async def test_server_handles_multiple_clients_concurrently(tcp_server):
    # --- ARRANGE ---
    host, port = tcp_server
    client_count = 10
    
    async def run_client(client_id: int):
        r, w = await asyncio.open_connection(host, port)
        try:
            # У каждого клиента свой уникальный IMEI, значит и CRC будет разный
            payload_body = f'imei:{client_id};datetime:12-00-00;speed_can:0;speed_engine:0;dut_lvl:0'.encode()
            real_crc = crc16_modbus(payload_body)
            test_payload = payload_body + f';crc16:{real_crc}'.encode()
            
            w.write(test_payload)
            await w.drain()
            
            await asyncio.sleep(0.01) 
            res = await r.read(1024)
            return res == b"'ack':'ok';"
        finally:
            w.close()
            await w.wait_closed()

    # --- ACT ---
    # Никаких моков! Всё работает на реальных функциях
    tasks = [run_client(i) for i in range(client_count)]
    results = await asyncio.gather(*tasks)

    # --- ASSERT ---
    assert all(results) is True
    assert len(results) == client_count
