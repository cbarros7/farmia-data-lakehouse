CREATE TABLE IF NOT EXISTS silver.iot_sensors (
    `sensor_id` STRING NOT NULL,
    `location` STRING,
    `temperature` DOUBLE,
    `humidity` DOUBLE,
    `ph` DOUBLE,
    `reading_timestamp` TIMESTAMP,
    `batch_id` STRING NOT NULL,
    `ingest_timestamp` TIMESTAMP NOT NULL,
    `event_timestamp` TIMESTAMP NOT NULL,
    `schema_version_id` STRING NOT NULL,
    `pipeline_git_hash` STRING NOT NULL,
    `source_system` STRING NOT NULL,
    `file_path` STRING NOT NULL,
    `execution_user_id` STRING NOT NULL,
    `environment_id` STRING NOT NULL,
    `cluster_id` STRING NOT NULL,
    `processing_library_version` STRING NOT NULL,
    `operation_type` STRING NOT NULL,
    `retention_ttl` INT NOT NULL
)
USING DELTA
PARTITIONED BY (sensor_id, reading_timestamp)
TBLPROPERTIES (
    'delta.enableIcebergCompatV2' = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.columnMapping.mode' = 'name',
    'retention_ttl_days' = '14',
    'idempotence_mode' = 'APPEND'
);

COMMENT ON TABLE silver.iot_sensors IS 'Granular IoT telemetry (temp, humidity, pH). TTL: 14 días. Pre-agg obligatorio antes de VACUUM.';
