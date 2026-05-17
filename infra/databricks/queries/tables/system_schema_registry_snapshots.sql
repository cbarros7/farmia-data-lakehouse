CREATE TABLE IF NOT EXISTS system.schema_registry_snapshots (
    `domain` STRING NOT NULL,
    `schema_version` STRING NOT NULL,
    `schema_json` STRING NOT NULL,
    `created_timestamp` TIMESTAMP NOT NULL,
    `pipeline_git_hash` STRING NOT NULL,
    `status` STRING NOT NULL
)
USING DELTA
PARTITIONED BY (domain)
TBLPROPERTIES (
    'delta.enableIcebergCompatV2' = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg'
);

COMMENT ON TABLE system.schema_registry_snapshots IS 'System table: Versionamiento de contratos YAML (ADD §3.2). Retro-compatibilidad en evolución.';
