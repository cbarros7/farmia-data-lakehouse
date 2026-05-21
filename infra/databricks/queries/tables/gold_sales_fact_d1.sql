CREATE TABLE IF NOT EXISTS gold.sales_fact_d1 (
    `transaction_id` STRING NOT NULL,
    `customer_id` STRING,
    `amount` DOUBLE,
    `currency` STRING,
    `transaction_date` DATE NOT NULL,
    `batch_id` STRING NOT NULL,
    `ingest_timestamp` TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (transaction_date)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'sync_frequency' = 'D-1_nocturno'
);

COMMENT ON TABLE gold.sales_fact_d1 IS 'Fact table: Venta de transacciones consolidadas D-1. Star Schema.';
