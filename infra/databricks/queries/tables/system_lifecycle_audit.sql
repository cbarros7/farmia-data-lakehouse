CREATE TABLE IF NOT EXISTS system.lifecycle_audit (
    `audit_id` STRING NOT NULL,
    `operation_type` STRING NOT NULL,
    `table_name` STRING NOT NULL,
    `rows_affected` LONG,
    `execution_time_ms` LONG,
    `status` STRING NOT NULL,
    `message` STRING,
    `execution_timestamp` TIMESTAMP NOT NULL,
    `batch_id` STRING NOT NULL
)
USING DELTA
DBPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.autoCompact.enabled' = 'true',
    'delta.autoCompact.minNumFiles' = '5'
);

COMMENT ON TABLE system.lifecycle_audit IS 'System table: Auditoría de ciclo de vida (VACUUM, pre-aggregation completados). Trazabilidad FinOps.';
