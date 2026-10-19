import pytest
from pydantic import ValidationError

from repository.models import PointModel


def test_point_model_alias_datetime():
    point = PointModel.model_validate(
        {
            "imei": "11111",
            "datetime": "12-00-00",
            "speed_can": "1.5",
            "speed_engine": "1000",
            "dut_lvl": "10",
            "crc16": "32442",
        }
    )
    assert point.imei == "11111"
    assert point.datetime_src == "12-00-00"
    assert point.speed_can == 1.5
    assert point.speed_engine == 1000.0
    assert point.dut_lvl == 10.0
    assert point.crc16 == "32442"


def test_point_model_requires_imei():
    with pytest.raises(ValidationError):
        PointModel.model_validate(
            {
                "datetime": "12-00-00",
                "speed_can": "0",
                "speed_engine": "0",
                "dut_lvl": "0",
                "crc16": "1",
            }
        )
