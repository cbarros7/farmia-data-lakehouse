CREATE TABLE IF NOT EXISTS silver.inventory_erp (
    `product_id` STRING NOT NULL,
    `warehouse_id` STRING NOT NULL,
    `quantity_available` INT NOT NULL,
    `quantity_reserved` INT,
    `last_updated` DATE NOT NULL,
    `last_updated_timestamp` TIMESTAMP NOT NULL,
    `operation_id` STRING NOT NULL,
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
PARTITIONED BY (last_updated)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'idempotence_mode' = 'MERGE_INTO'
);

COMMENT ON TABLE silver.inventory_erp IS 'Fact table: Movimientos stock ERP. Merge by (product_id, warehouse_id). Particionado por last_updated (DATE).';
