CREATE TABLE IF NOT EXISTS gold.weather_daily_obt (
    `city` STRING NOT NULL,
    `date` DATE NOT NULL,
    `min_temperature` DOUBLE,
    `max_temperature` DOUBLE,
    `avg_temperature` DOUBLE,
    `min_humidity` DOUBLE,
    `max_humidity` DOUBLE,
    `avg_humidity` DOUBLE,
    `batch_id` STRING NOT NULL,
    `ingest_timestamp` TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (date)
TBLPROPERTIES (
    'delta.enableIcebergCompatV2' = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.columnMapping.mode' = 'name',
    'delta.tuneFileSizesForRewrites' = 'true',
    'sync_frequency' = 'D-1_nocturno'
);

COMMENT ON TABLE gold.weather_daily_obt IS 'OBT: Agregación diaria de weather (min/max/avg temp/humidity). D-1 sync nocturna. FinOps -50% storage.';
