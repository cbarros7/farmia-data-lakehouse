CREATE TABLE IF NOT EXISTS silver.inventory_erp (
    `product_id` STRING NOT NULL,
    `warehouse_id` STRING NOT NULL,
    `quantity` LONG,
    `movement_date` TIMESTAMP,
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
PARTITIONED BY (movement_date)
TBLPROPERTIES (
    'delta.enableIcebergCompatV2' = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.columnMapping.mode' = 'name',
    'idempotence_mode' = 'MERGE_INTO'
);

COMMENT ON TABLE silver.inventory_erp IS 'Fact table: Movimientos stock ERP. Histórico ilimitado. Star Schema para Trino.';
