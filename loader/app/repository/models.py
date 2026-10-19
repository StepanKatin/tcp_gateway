from pydantic import BaseModel, ConfigDict, Field


class PointModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    imei: str
    datetime_src: str = Field(alias="datetime")
    speed_can: float
    speed_engine: float
    dut_lvl: float
    crc16: str
