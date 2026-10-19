#!/usr/bin/env python3
"""
Smoke-клиент для tcp_gateway.

Примеры (из tcp_gateway/, второй терминал):
  python smoke_tcp.py
  python smoke_tcp.py --mode bad_crc
  python smoke_tcp.py --mode garbage
  python smoke_tcp.py --imei 11111 --count 3

После успешного ok проверь Redis Stream (из корня репо):
  docker compose exec redis redis-cli XLEN telemetry.points
  docker compose exec redis redis-cli XRANGE telemetry.points - + COUNT 3
"""

from __future__ import annotations

import argparse
import asyncio


def crc16_modbus(data: bytes) -> int:
    crc = 0xFFFF
    poly = 0xA001
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ poly
            else:
                crc >>= 1
    return crc


def build_payload(*, imei: str, mode: str) -> bytes:
    body = (
        f"imei:{imei};"
        f"datetime:12-00-00;"
        f"speed_can:0;"
        f"speed_engine:0;"
        f"dut_lvl:0"
    ).encode()

    if mode == "ok":
        return body + f";crc16:{crc16_modbus(body)}".encode()
    if mode == "bad_crc":
        return body + b";crc16:1"
    if mode == "garbage":
        return b"not-a-valid-packet\n"
    raise ValueError(f"unknown mode: {mode}")


async def send_once(host: str, port: int, payload: bytes, timeout: float) -> bytes:
    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(payload)
        await writer.drain()
        return await asyncio.wait_for(reader.read(1024), timeout=timeout)
    finally:
        writer.close()
        await writer.wait_closed()


async def run(args: argparse.Namespace) -> int:
    payload = build_payload(imei=args.imei, mode=args.mode)
    print(f"→ {args.host}:{args.port} mode={args.mode} imei={args.imei} count={args.count}")
    print(f"  payload: {payload!r}")

    exit_code = 0
    for i in range(1, args.count + 1):
        try:
            ack = await send_once(args.host, args.port, payload, args.timeout)
        except Exception as exc:
            print(f"[{i}/{args.count}] ERROR: {exc}")
            return 1

        print(f"[{i}/{args.count}] ack: {ack!r}")

        if args.mode == "ok" and ack != b"'ack':'ok';":
            exit_code = 1
        if args.mode in {"bad_crc", "garbage"} and ack != b"'ack':'error_bad_packet';":
            # gateway может просто рвать соединение без ack — тоже зафиксируем
            if ack:
                exit_code = 1

    if args.mode == "ok" and exit_code == 0:
        print(
            "\nПроверь Redis Stream (из корня репо):\n"
            "  docker compose exec redis redis-cli XLEN telemetry.points\n"
            "  docker compose exec redis redis-cli XRANGE telemetry.points - + COUNT 3"
        )
    return exit_code


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke TCP client for tcp_gateway")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--imei", default="11111")
    parser.add_argument(
        "--mode",
        choices=("ok", "bad_crc", "garbage"),
        default="ok",
        help="ok = валидный CRC; bad_crc / garbage = ожидаем error_bad_packet",
    )
    parser.add_argument("--count", type=int, default=1, help="сколько раз отправить")
    parser.add_argument("--timeout", type=float, default=5.0)
    args = parser.parse_args()
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
