import pytest

from repository.adapter import COLUMNS, ClickHouseRepo


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def insert(self, table, rows, column_names=None):
        self.calls.append((table, rows, column_names))


@pytest.mark.asyncio
async def test_insert_many_empty_noop(settings, logger):
    client = FakeClient()
    repo = ClickHouseRepo(client, settings, logger)
    await repo.insert_many([])
    assert client.calls == []


@pytest.mark.asyncio
async def test_insert_many_maps_batch_and_inserts(settings, logger):
    client = FakeClient()
    repo = ClickHouseRepo(client, settings, logger)

    await repo.insert_many(
        [
            {
                "imei": "11111",
                "datetime": "12-00-00",
                "speed_can": "1",
                "speed_engine": "2",
                "dut_lvl": "3",
                "crc16": "4",
            },
            {
                "imei": "22222",
                "datetime": "13-00-00",
                "speed_can": "10",
                "speed_engine": "20",
                "dut_lvl": "30",
                "crc16": "40",
            },
        ]
    )

    assert len(client.calls) == 1
    table, rows, column_names = client.calls[0]
    assert table == "telemetry.telemetry_raw"
    assert column_names == COLUMNS
    assert rows == [
        ["11111", "12-00-00", 1.0, 2.0, 3.0, "4"],
        ["22222", "13-00-00", 10.0, 20.0, 30.0, "40"],
    ]


@pytest.mark.asyncio
async def test_insert_many_raises_on_client_error(settings, logger):
    class BoomClient:
        def insert(self, *args, **kwargs):
            raise RuntimeError("ch down")

    repo = ClickHouseRepo(BoomClient(), settings, logger)
    with pytest.raises(RuntimeError, match="ch down"):
        await repo.insert_many(
            [
                {
                    "imei": "1",
                    "datetime": "12-00-00",
                    "speed_can": "0",
                    "speed_engine": "0",
                    "dut_lvl": "0",
                    "crc16": "1",
                }
            ]
        )
