CREATE TABLE IF NOT EXISTS silver.sales_online (
    `transaction_id` STRING NOT NULL,
    `customer_id` STRING NOT NULL,
    `product_id` STRING NOT NULL,
    `amount` DOUBLE NOT NULL,
    `quantity` INT NOT NULL,
    `currency` STRING NOT NULL,
    `transaction_timestamp` TIMESTAMP NOT NULL,
    `transaction_date` DATE NOT NULL,
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
PARTITIONED BY (transaction_date)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'idempotence_mode' = 'MERGE_INTO'
);

COMMENT ON TABLE silver.sales_online IS 'Fact table: Transacciones e-commerce. Histórico ilimitado. Star Schema.';
