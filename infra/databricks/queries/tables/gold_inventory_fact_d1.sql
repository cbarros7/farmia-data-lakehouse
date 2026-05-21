CREATE TABLE IF NOT EXISTS gold.inventory_fact_d1 (
    `product_id` STRING NOT NULL,
    `warehouse_id` STRING NOT NULL,
    `quantity` LONG,
    `movement_date` DATE NOT NULL,
    `batch_id` STRING NOT NULL,
    `ingest_timestamp` TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (movement_date)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'sync_frequency' = 'D-1_nocturno'
);

COMMENT ON TABLE gold.inventory_fact_d1 IS 'Fact table: Movimientos inventory consolidados D-1. Star Schema.';
