import asyncio
from logging import Logger

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from core.core import Settings
from domain.ports import Repository
from repository.models import PointModel

COLUMNS = [
    "imei",
    "datetime_src",
    "speed_can",
    "speed_engine",
    "dut_lvl",
    "crc16",
]


class ClickHouseRepo(Repository):
    def __init__(
        self,
        client: Client,
        settings: Settings,
        logger: Logger,
    ) -> None:
        self._client = client
        self._logger = logger
        self._table = f"{settings.clickhouse_database}.telemetry_raw"

    async def insert_many(self, messages: list[dict]) -> None:
        if not messages:
            return

        rows: list[list] = []
        for data in messages:
            point = PointModel.model_validate(data)
            rows.append(
                [
                    point.imei,
                    point.datetime_src,
                    float(point.speed_can),
                    float(point.speed_engine),
                    float(point.dut_lvl),
                    str(point.crc16),
                ]
            )

        try:
            await asyncio.to_thread(
                self._client.insert,
                self._table,
                rows,
                column_names=COLUMNS,
            )
            self._logger.info("Inserted %s rows into %s", len(rows), self._table)
        except Exception:
            self._logger.exception(
                "ClickHouse insert failed (%s rows)", len(rows)
            )
            raise


def create_clickhouse_client(settings: Settings) -> Client:
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
    )
