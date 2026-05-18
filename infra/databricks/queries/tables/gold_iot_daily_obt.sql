CREATE TABLE IF NOT EXISTS gold.iot_daily_obt (
    `sensor_id` STRING NOT NULL,
    `location` STRING,
    `date` DATE NOT NULL,
    `min_temperature` DOUBLE,
    `max_temperature` DOUBLE,
    `avg_temperature` DOUBLE,
    `min_humidity` DOUBLE,
    `max_humidity` DOUBLE,
    `avg_humidity` DOUBLE,
    `min_ph` DOUBLE,
    `max_ph` DOUBLE,
    `avg_ph` DOUBLE,
    `batch_id` STRING NOT NULL,
    `ingest_timestamp` TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (date)
DBPROPERTIES (
    'delta.enableIcebergCompatV2' = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.columnMapping.mode' = 'name',
    'delta.tuneFileSizesForRewrites' = 'true',
    'delta.dataSkippingNumIndexedCols' = '10',
    'sync_frequency' = 'D-1_nocturno'
);

COMMENT ON TABLE gold.iot_daily_obt IS 'OBT: Agregación diaria de IoT (min/max/avg temp/humidity/pH). D-1 sync. FinOps -50% compute.';
