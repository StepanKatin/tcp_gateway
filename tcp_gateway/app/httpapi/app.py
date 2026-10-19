from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis.asyncio import Redis


async def health(_: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def ready(request: web.Request) -> web.Response:
    redis: Redis = request.app["redis"]
    try:
        await redis.ping()
    except Exception as exc:
        return web.json_response(
            {"status": "not_ready", "error": str(exc)},
            status=503,
        )
    return web.json_response({"status": "ready"})


async def metrics_handler(_: web.Request) -> web.Response:
    payload = generate_latest()
    return web.Response(body=payload, headers={"Content-Type": CONTENT_TYPE_LATEST})


def create_http_app(redis: Redis) -> web.Application:
    app = web.Application()
    app["redis"] = redis
    app.router.add_get("/health", health)
    app.router.add_get("/ready", ready)
    app.router.add_get("/metrics", metrics_handler)
    return app