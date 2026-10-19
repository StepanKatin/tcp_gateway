import asyncio
import time
from aiohttp import web
from httpapi.app import create_http_app
from parser.parser import parse_request
from core.core import Settings
from publisher.publisher import PointPublisher
from functools import partial
from logging import Logger
from redis.asyncio import Redis
from connection.context import ConnectionContext
from di.container import create_di_container
from metrics.metrics import GatewayMetrics



async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    logger: Logger,
    settings: Settings,
    publisher: PointPublisher,
    ctx: ConnectionContext,
    metrics: GatewayMetrics,
):
    addr = ctx.peer or writer.get_extra_info("peername")
    try:
        metrics.connections_active.inc()
        while True:
            try:
                async with asyncio.timeout(settings.tcp_timeout):
                    data = await reader.read(1024)
            except TimeoutError:
                logger.info(f"Client {addr} closed TCP connection")
                break

            if not data:
                break

            t0 = time.perf_counter()

            try:
                message = parse_request(data)
            except Exception as parse_error:
                logger.error(
                    f"Парсинг не удался: {parse_error}, CRC16 или формат неверны. "
                    "Отключаем клиента."
                )
                metrics.packet_bad()
                writer.write(b"'ack':'error_bad_packet';")
                await writer.drain()
                break

            try:
                await publisher.publish(message)
                ctx.packets_ok += 1
                if ctx.imei is None:
                    ctx.imei = message.get("imei")
            except Exception as redis_error:
                logger.error(f"Ошибка записи в Redis: {redis_error}")
                metrics.packet_redis_error()
                writer.write(b"'ack':'internal_error';")
                await writer.drain()
                continue

            writer.write(b"'ack':'ok';")
            logger.debug(f"{message.get('imei')} successfully handled")
            await writer.drain()

            metrics.packet_ok()
            metrics.packet_duration_seconds.observe(time.perf_counter() - t0)

    except asyncio.CancelledError:
        pass
    finally:
        metrics.connections_active.dec()
        logger.info("TCP connection is closed")
        writer.close()
        await writer.wait_closed()

async def client_accepted(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    app_container,
):
    async with app_container() as request_container:
        logger = await request_container.get(Logger)
        settings = await request_container.get(Settings)
        publisher = await request_container.get(PointPublisher)
        ctx = await request_container.get(ConnectionContext)
        metrics = await request_container.get(GatewayMetrics)

        ctx.peer = writer.get_extra_info("peername")

        await handle_client(
            reader,
            writer,
            logger=logger,
            settings=settings,
            publisher=publisher,
            ctx=ctx,
            metrics=metrics,
        )

async def main():
    di_container = create_di_container()
    runner = None
    try:
        logger = await di_container.get(Logger)
        settings = await di_container.get(Settings)

        redis = await di_container.get(Redis)
        await redis.ping()
        logger.info("Redis ping ok")

        # --- HTTP : health / ready / metrics ---
        http_app = create_http_app(redis)
        runner = web.AppRunner(http_app)
        await runner.setup()
        site = web.TCPSite(runner, settings.http_host, settings.http_port)
        await site.start()
        logger.info(
            f"HTTP started on {settings.http_host}:{settings.http_port}"
        )

        # --- TCP терминалы ---
        handler = partial(client_accepted, app_container=di_container)
        server = await asyncio.start_server(
            handler,
            settings.tcp_host,
            settings.tcp_port,
        )
        addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
        logger.info(f"TCP server started on {addrs}")

        async with server:
            await server.serve_forever()
    finally:
        if runner is not None:
            await runner.cleanup()
        await di_container.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Server stopped by user")