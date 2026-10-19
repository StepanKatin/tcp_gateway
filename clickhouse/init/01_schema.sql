CREATE DATABASE IF NOT EXISTS telemetry;

CREATE TABLE IF NOT EXISTS telemetry.telemetry_raw
(
    event_time   DateTime64(3) DEFAULT now64(3),
    imei         String,
    datetime_src String,
    speed_can    Float64,
    speed_engine Float64,
    dut_lvl      Float64,
    crc16        String,
    ingested_at  DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (imei, event_time);

CREATE TABLE IF NOT EXISTS telemetry.telemetry_clean
(
    event_time   DateTime64(3),
    imei         String,
    speed_can    Nullable(Float64),
    speed_engine Nullable(Float64),
    dut_lvl      Nullable(Float64),
    ingested_at  DateTime64(3)
)
ENGINE = MergeTree
ORDER BY (imei, event_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry.telemetry_clean_mv
TO telemetry.telemetry_clean
AS
SELECT
    event_time,
    imei,
    if(speed_can BETWEEN 0 AND 250, speed_can, NULL) AS speed_can,
    if(speed_engine BETWEEN 0 AND 8000, speed_engine, NULL) AS speed_engine,
    if(dut_lvl >= 0, dut_lvl, NULL) AS dut_lvl,
    ingested_at
FROM telemetry.telemetry_raw;