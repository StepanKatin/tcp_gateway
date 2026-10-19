from dishka import AsyncContainer, make_async_container
from di.provider import LoaderContainer

def create_di_container() -> AsyncContainer:
    return make_async_container(LoaderContainer())