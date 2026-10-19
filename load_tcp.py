#!/usr/bin/env python3
"""
Нагрузочный TCP-клиент для tcp_gateway (stdlib only).

Запуск из корня репо при поднятом compose:
  python load_tcp.py --rps 100 --duration 20
  python load_tcp.py --ladder 100,500,1000,2500,5000 --duration 15 --concurrency 250

Смотри Grafana: TCP IoT Pipeline, и:
  curl -s localhost:8001/metrics | grep tcp_gateway_packets
  curl -s localhost:8002/metrics | grep loader_inserted
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time
from dataclasses import dataclass, field


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


def build_ok_payload(imei: str) -> bytes:
    body = (
        f"imei:{imei};"
        f"datetime:12-00-00;"
        f"speed_can:77;"
        f"speed_engine:1500;"
        f"dut_lvl:1024"
    ).encode()
    return body + f";crc16:{crc16_modbus(body)}".encode()


@dataclass
class Stats:
    ok: int = 0
    bad_ack: int = 0
    errors: int = 0
    latencies_ms: list[float] = field(default_factory=list)

    def add_ok(self, latency_ms: float) -> None:
        self.ok += 1
        self.latencies_ms.append(latency_ms)

    def add_bad_ack(self) -> None:
        self.bad_ack += 1

    def add_error(self) -> None:
        self.errors += 1


async def send_one(
    host: str,
    port: int,
    payload: bytes,
    timeout: float,
    stats: Stats,
    sem: asyncio.Semaphore,
) -> None:
    async with sem:
        t0 = time.perf_counter()
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=timeout,
            )
            try:
                writer.write(payload)
                await writer.drain()
                ack = await asyncio.wait_for(reader.read(1024), timeout=timeout)
            finally:
                writer.close()
                await writer.wait_closed()
            latency_ms = (time.perf_counter() - t0) * 1000
            if ack == b"'ack':'ok';":
                stats.add_ok(latency_ms)
            else:
                stats.add_bad_ack()
        except Exception:
            stats.add_error()


async def run_rps(
    *,
    host: str,
    port: int,
    rps: int,
    duration: float,
    concurrency: int,
    timeout: float,
    imei_prefix: str,
) -> Stats:
    stats = Stats()
    sem = asyncio.Semaphore(concurrency)
    payload_cache: dict[str, bytes] = {}
    interval = 1.0 / rps if rps > 0 else 0.0
    deadline = time.perf_counter() + duration
    seq = 0
    pending: set[asyncio.Task[None]] = set()

    def _on_done(task: asyncio.Task[None]) -> None:
        pending.discard(task)

    next_at = time.perf_counter()
    while time.perf_counter() < deadline:
        seq += 1
        imei = f"{imei_prefix}{seq % 10_000:05d}"
        payload = payload_cache.get(imei)
        if payload is None:
            payload = build_ok_payload(imei)
            payload_cache[imei] = payload

        task = asyncio.create_task(
            send_one(host, port, payload, timeout, stats, sem)
        )
        pending.add(task)
        task.add_done_callback(_on_done)

        next_at += interval
        sleep_for = next_at - time.perf_counter()
        if sleep_for > 0:
            await asyncio.sleep(sleep_for)
        else:
            # отстаём от целевого RPS — не спим
            next_at = time.perf_counter()

    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
    return stats


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    idx = min(len(ordered) - 1, max(0, int(round((p / 100) * (len(ordered) - 1)))))
    return ordered[idx]


def print_stats(label: str, stats: Stats, duration: float) -> None:
    total = stats.ok + stats.bad_ack + stats.errors
    achieved = stats.ok / duration if duration > 0 else 0.0
    p50 = statistics.median(stats.latencies_ms) if stats.latencies_ms else 0.0
    p95 = _percentile(stats.latencies_ms, 95)
    print(
        f"[{label}] total≈{total} ok={stats.ok} bad_ack={stats.bad_ack} "
        f"err={stats.errors} | achieved≈{achieved:.0f} ok/s | "
        f"lat_ms p50={p50:.1f} p95={p95:.1f}"
    )


async def run(args: argparse.Namespace) -> int:
    if args.ladder:
        steps = [int(x.strip()) for x in args.ladder.split(",") if x.strip()]
    else:
        steps = [args.rps]

    print(
        f"target={args.host}:{args.port} concurrency={args.concurrency} "
        f"timeout={args.timeout}s steps={steps} duration={args.duration}s"
    )

    exit_code = 0
    for rps in steps:
        label = f"{rps}rps"
        t0 = time.perf_counter()
        stats = await run_rps(
            host=args.host,
            port=args.port,
            rps=rps,
            duration=args.duration,
            concurrency=args.concurrency,
            timeout=args.timeout,
            imei_prefix=args.imei_prefix,
        )
        elapsed = time.perf_counter() - t0
        print_stats(label, stats, elapsed)
        if stats.ok == 0 or stats.errors > max(1, int(stats.ok * 0.05)):
            exit_code = 1
        if args.pause > 0 and rps != steps[-1]:
            await asyncio.sleep(args.pause)

    return exit_code


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load TCP client for tcp_gateway (root of repo)"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--rps", type=int, default=100, help="целевой RPS одной ступени")
    parser.add_argument(
        "--ladder",
        default="",
        help="лестница RPS через запятую, напр. 100,500,1000,2500,5000",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=20.0,
        help="длительность каждой ступени, сек",
    )
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--imei-prefix", default="9")
    parser.add_argument("--pause", type=float, default=2.0, help="пауза между ступенями")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
