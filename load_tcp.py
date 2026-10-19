#!/usr/bin/env python3
"""
Нагрузочный TCP-клиент: 4000 уникальных IMEI (терминалы телематики).

Модель нагрузки:
  фиксированный пул терминалов; на каждой ступени они шлют всё чаще.
  target_rps = num_imei / interval_sec
  → 100 RPS  ≈ раз в 40 с
  → 200 RPS  ≈ раз в 20 с
  → …
  → 4000 RPS ≈ раз в 1 с

Запуск из корня репо при поднятом compose:
  python load_tcp.py
  python load_tcp.py --step-rps 100 --max-rps 4000 --duration 30
  python load_tcp.py --ladder 100,500,1000,4000 --duration 20

Смотри Grafana: TCP IoT Pipeline, и:
  curl -s localhost:8001/metrics | grep tcp_gateway_packets
  curl -s localhost:8002/metrics | grep loader_inserted
"""

from __future__ import annotations

import argparse
import asyncio
import random
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone


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


def build_payload(
    imei: str,
    *,
    speed_can: int | None = None,
    speed_engine: int | None = None,
    dut_lvl: int | None = None,
) -> bytes:
    now = datetime.now(timezone.utc).strftime("%H-%M-%S")
    speed_can = 50 + (speed_can if speed_can is not None else random.randint(0, 80))
    speed_engine = (
        800 + (speed_engine if speed_engine is not None else random.randint(0, 2200))
    )
    dut_lvl = dut_lvl if dut_lvl is not None else random.randint(0, 4095)
    body = (
        f"imei:{imei};"
        f"datetime:{now};"
        f"speed_can:{speed_can};"
        f"speed_engine:{speed_engine};"
        f"dut_lvl:{dut_lvl}"
    ).encode()
    return body + f";crc16:{crc16_modbus(body)}".encode()


def make_imei_pool(count: int, prefix: str) -> list[str]:
    """Ровно `count` уникальных IMEI."""
    width = max(4, len(str(count - 1)))
    return [f"{prefix}{i:0{width}d}" for i in range(count)]


@dataclass
class Stats:
    ok: int = 0
    bad_ack: int = 0
    errors: int = 0
    latencies_ms: list[float] = field(default_factory=list)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def add_ok(self, latency_ms: float) -> None:
        async with self._lock:
            self.ok += 1
            # ограничиваем память на длинных прогонах
            if len(self.latencies_ms) < 50_000:
                self.latencies_ms.append(latency_ms)

    async def add_bad_ack(self) -> None:
        async with self._lock:
            self.bad_ack += 1

    async def add_error(self) -> None:
        async with self._lock:
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
                await stats.add_ok(latency_ms)
            else:
                await stats.add_bad_ack()
        except Exception:
            await stats.add_error()


async def terminal_loop(
    *,
    imei: str,
    host: str,
    port: int,
    timeout: float,
    stats: Stats,
    sem: asyncio.Semaphore,
    interval_holder: list[float],
    stop_at: float,
    phase_offset: float,
) -> None:
    """
    Один терминал: шлёт пакет, затем спит interval_holder[0] секунд.
    interval_holder — shared list, чтобы менять частоту на ступенях без пересоздания задач.
    """
    # размазываем первый тик по интервалу, иначе все 4000 ударят в одну секунду
    await asyncio.sleep(phase_offset)
    while time.perf_counter() < stop_at:
        payload = build_payload(imei)
        await send_one(host, port, payload, timeout, stats, sem)
        interval = interval_holder[0]
        remaining = stop_at - time.perf_counter()
        if remaining <= 0:
            break
        await asyncio.sleep(min(interval, remaining))


async def run_stage(
    *,
    host: str,
    port: int,
    imeis: list[str],
    target_rps: int,
    duration: float,
    concurrency: int,
    timeout: float,
) -> Stats:
    """
    Все IMEI активны; интервал = len(imeis) / target_rps.
    При 4000 IMEI и 100 RPS → каждый шлёт раз в 40 с; при 4000 RPS — раз в 1 с.
    """
    stats = Stats()
    sem = asyncio.Semaphore(concurrency)
    n = len(imeis)
    interval = n / target_rps if target_rps > 0 else float("inf")
    interval_holder = [interval]
    stop_at = time.perf_counter() + duration

    tasks = [
        asyncio.create_task(
            terminal_loop(
                imei=imei,
                host=host,
                port=port,
                timeout=timeout,
                stats=stats,
                sem=sem,
                interval_holder=interval_holder,
                stop_at=stop_at,
                phase_offset=(i / n) * interval if n and interval < float("inf") else 0.0,
            )
        )
        for i, imei in enumerate(imeis)
    ]
    await asyncio.gather(*tasks, return_exceptions=True)
    return stats


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    idx = min(len(ordered) - 1, max(0, int(round((p / 100) * (len(ordered) - 1)))))
    return ordered[idx]


def print_stats(
    label: str,
    stats: Stats,
    duration: float,
    *,
    target_rps: int,
    num_imei: int,
) -> None:
    total = stats.ok + stats.bad_ack + stats.errors
    achieved = stats.ok / duration if duration > 0 else 0.0
    interval = num_imei / target_rps if target_rps > 0 else 0.0
    p50 = statistics.median(stats.latencies_ms) if stats.latencies_ms else 0.0
    p95 = _percentile(stats.latencies_ms, 95)
    print(
        f"[{label}] interval≈{interval:.2f}s/imei | "
        f"total≈{total} ok={stats.ok} bad_ack={stats.bad_ack} err={stats.errors} | "
        f"target={target_rps} achieved≈{achieved:.0f} ok/s | "
        f"lat_ms p50={p50:.1f} p95={p95:.1f}"
    )


def build_ladder(args: argparse.Namespace) -> list[int]:
    if args.ladder:
        return [int(x.strip()) for x in args.ladder.split(",") if x.strip()]
    return list(range(args.step_rps, args.max_rps + 1, args.step_rps))


async def run(args: argparse.Namespace) -> int:
    imeis = make_imei_pool(args.num_imei, args.imei_prefix)
    steps = build_ladder(args)

    print(
        f"target={args.host}:{args.port} imei={len(imeis)} "
        f"concurrency={args.concurrency} timeout={args.timeout}s "
        f"steps={steps[0]}..{steps[-1]} ({len(steps)} шт) duration={args.duration}s/step"
    )
    print(
        "модель: все терминалы всегда онлайн; на ступени N RPS "
        f"каждый шлёт раз в ≈{len(imeis)}/N секунд"
    )

    exit_code = 0
    for rps in steps:
        label = f"{rps}rps"
        t0 = time.perf_counter()
        stats = await run_stage(
            host=args.host,
            port=args.port,
            imeis=imeis,
            target_rps=rps,
            duration=args.duration,
            concurrency=args.concurrency,
            timeout=args.timeout,
        )
        elapsed = time.perf_counter() - t0
        print_stats(
            label, stats, elapsed, target_rps=rps, num_imei=len(imeis)
        )
        if stats.ok == 0 or stats.errors > max(1, int(stats.ok * 0.05)):
            exit_code = 1
        if args.pause > 0 and rps != steps[-1]:
            await asyncio.sleep(args.pause)

    return exit_code


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Load TCP: 4000 IMEI, частота растёт (раз в минуту → раз в секунду)"
        )
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument(
        "--num-imei",
        type=int,
        default=4000,
        help="число уникальных терминалов (IMEI)",
    )
    parser.add_argument(
        "--step-rps",
        type=int,
        default=100,
        help="шаг лестницы RPS (по умолчанию 100,200,...,max)",
    )
    parser.add_argument(
        "--max-rps",
        type=int,
        default=4000,
        help="верхняя ступень RPS",
    )
    parser.add_argument(
        "--ladder",
        default="",
        help="явная лестница RPS через запятую (перебивает --step-rps/--max-rps)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=30.0,
        help="длительность каждой ступени, сек",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=500,
        help="макс. одновременных TCP-соединений",
    )
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--imei-prefix", default="86")
    parser.add_argument(
        "--pause",
        type=float,
        default=2.0,
        help="пауза между ступенями, сек",
    )
    args = parser.parse_args()
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
