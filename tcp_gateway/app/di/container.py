from dishka import AsyncContainer, make_async_container
from di.provider import GatewayProvider

def create_di_container() -> AsyncContainer:
    return make_async_container(GatewayProvider())
