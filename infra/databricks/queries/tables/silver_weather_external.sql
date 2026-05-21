CREATE TABLE IF NOT EXISTS silver.weather_external (
    `city` STRING NOT NULL,
    `temperature` DOUBLE,
    `humidity` DOUBLE,
    `forecast_timestamp` TIMESTAMP NOT NULL,
    `forecast_date` DATE NOT NULL,
    `_batch_id` BIGINT NOT NULL,
    `_ingest_timestamp` TIMESTAMP NOT NULL,
    `_event_timestamp` TIMESTAMP NOT NULL,
    `_schema_version_id` STRING NOT NULL,
    `_pipeline_git_hash` STRING NOT NULL,
    `_source_system` STRING NOT NULL,
    `_file_path` STRING NOT NULL,
    `_execution_user_id` STRING NOT NULL,
    `_environment_id` STRING NOT NULL,
    `_cluster_id` STRING NOT NULL,
    `_processing_library_version` STRING NOT NULL,
    `_operation_type` STRING NOT NULL,
    `_retention_ttl` STRING NOT NULL
)
USING DELTA
PARTITIONED BY (city, forecast_date)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'retention_ttl_days' = '14',
    'idempotence_mode' = 'APPEND'
);

COMMENT ON TABLE silver.weather_external IS 'Telemetría meteorológica consolidada. TTL: 14 días post-agg. Iceberg compatible.';
