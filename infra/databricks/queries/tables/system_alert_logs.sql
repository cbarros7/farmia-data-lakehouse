CREATE TABLE IF NOT EXISTS system.alert_logs (
    `alert_id` STRING NOT NULL,
    `error_type` STRING NOT NULL,
    `error_message` STRING,
    `error_fingerprint` STRING NOT NULL,
    `first_occurrence` TIMESTAMP NOT NULL,
    `last_occurrence` TIMESTAMP NOT NULL,
    `occurrence_count` LONG NOT NULL,
    `silenced_until` TIMESTAMP,
    `domain` STRING,
    `batch_id` STRING NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.autoCompact.enabled' = 'true',
    'delta.autoCompact.minNumFiles' = '10'
);

COMMENT ON TABLE system.alert_logs IS 'System table: Tracking de excepciones para exception masking (ADD §3.4). Fingerprinting + silencio 4h.';
