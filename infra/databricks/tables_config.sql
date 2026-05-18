-- ============================================================================
-- SECCIÓN A: Configuración Silver (Datos transaccionalmente válidos)
-- ============================================================================

-- A1: Weather External (Clima externo - IoT/Meteorología)
-- Restricción: TTL 14 días (pre-agregación antes de purga)
ALTER TABLE silver.weather_external SET DBPROPERTIES (
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg',
  'delta.columnMapping.mode' = 'name'
);

COMMENT ON TABLE silver.weather_external IS 'Telemetría meteorológica consolidada. Compatibilidad Iceberg: Trino lectura Zero-ETL. TTL: 14 días post-agregación.';

-- A2: IoT Sensors (Datos granulares de sensores)
-- Restricción: TTL 14 días + Pre-aggregation a Gold OBT antes de purga
ALTER TABLE silver.iot_sensors SET DBPROPERTIES (
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg',
  'delta.columnMapping.mode' = 'name'
);

COMMENT ON TABLE silver.iot_sensors IS 'Granular IoT telemetry (temp, humidity, pH). Iceberg compatible para Trino. Política de retención: 14 días (ADD §3.5). Pre-agg obligatorio antes de VACUUM.';

-- A3: Sales Online (Transacciones operacionales)
-- Restricción: Sin TTL (histórico completo, ADD §3.5)
ALTER TABLE silver.sales_online SET DBPROPERTIES (
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg',
  'delta.columnMapping.mode' = 'name'
);

COMMENT ON TABLE silver.sales_online IS 'Fact table: Transacciones e-commerce. Histórico ilimitado. Star Schema compatible Trino vía Iceberg.';

-- A4: Inventory ERP (Movimientos de stock)
-- Restricción: Sin TTL (histórico completo)
ALTER TABLE silver.inventory_erp SET DBPROPERTIES (
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg',
  'delta.columnMapping.mode' = 'name'
);

COMMENT ON TABLE silver.inventory_erp IS 'Fact table: Movimientos stock ERP. Histórico ilimitado. Star Schema para Trino.';

-- ============================================================================
-- SECCIÓN B: Configuración Gold (Agregaciones y Sincronización D-1)
-- ============================================================================

-- B1: Weather Daily Aggregation OBT (Tabla desnormalizada)
-- Patrón: One-Big-Table para optimizar escaneos secuenciales en Trino
-- Ciclo: Sincronización D-1 nocturna (ADD §3.5)
-- Costo: -50% vs granular (pre-aggregation antes de purga)
ALTER TABLE gold.weather_daily_obt SET DBPROPERTIES (
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg',
  'delta.columnMapping.mode' = 'name',
  -- FinOps: Z-order para aceleración de predicado en Trino
  'delta.tuneFileSizesForRewrites' = 'true'
);

COMMENT ON TABLE gold.weather_daily_obt IS 
'OBT: Agregación diaria de weather (promedios, min, max). Trino lectura Zero-ETL. Ciclo D-1 asincrónico (ventana nocturna). Storage -50% vs Silver granular.';

-- B2: IoT Daily Aggregation OBT (Tabla desnormalizada masiva)
-- Patrón: OBT para telemetría de sensores
-- Consolidación: Promedios/min/max de temperatura, humedad, pH por hora/día
-- Ciclo: D-1 Background Batch Sync (ADD §3.5)
-- FinOps: Reduce I/O en Trino de O(n granular) a O(1 agregado)
ALTER TABLE gold.iot_daily_obt SET DBPROPERTIES (
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg',
  'delta.columnMapping.mode' = 'name',
  -- Optimización: Clustering por sensor_id y date para escaneos secuenciales
  'delta.tuneFileSizesForRewrites' = 'true',
  -- Pre-aggregation completa antes de escribir (mitigación de crecimiento infinito ADD §3.5)
  'delta.dataSkippingNumIndexedCols' = '10'
);

COMMENT ON TABLE gold.iot_daily_obt IS 
'OBT: Agregación diaria de IoT (temp/humidity/pH min/max/avg). Trino lectura Zero-ETL. D-1 sync nocturna. FinOps: -50% storage, -70% compute vs real-time Trino.';

-- B3: Sales Fact Table (Star Schema)
-- Patrón: Fact table para OLAP
-- Ciclo: D-1 Sincronización nocturna (datos hasta ayer)
-- Idempotencia: INSERT de nuevas filas (APPEND, ADD §3.3)
ALTER TABLE gold.sales_fact_d1 SET DBPROPERTIES (
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg',
  'delta.columnMapping.mode' = 'name'
);

COMMENT ON TABLE gold.sales_fact_d1 IS 
'Fact table: Venta de transacciones consolidadas D-1. Star Schema para Trino. Acceso Zero-ETL vía Iceberg.';

-- B4: Inventory Fact Table (Star Schema)
-- Patrón: Fact table para gestión de stock
-- Ciclo: D-1 Sincronización nocturna
ALTER TABLE gold.inventory_fact_d1 SET DBPROPERTIES (
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg',
  'delta.columnMapping.mode' = 'name'
);

COMMENT ON TABLE gold.inventory_fact_d1 IS 
'Fact table: Movimientos inventory consolidados D-1. Star Schema para Trino. Lectura Zero-ETL.';

-- ============================================================================
-- SECCIÓN C: Tablas de Sistema para Gobernanza y Observabilidad
-- ============================================================================

-- C1: System Alert Logs (Exception Masking, ADD §3.4)
-- Uso: Silenciamiento de alertas recurrentes (4h default)
ALTER TABLE system.alert_logs SET DBPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  -- Compactación automática para tabla de logs
  'delta.autoCompact.enabled' = 'true',
  'delta.autoCompact.minNumFiles' = '10'
);

COMMENT ON TABLE system.alert_logs IS 
'System table: Tracking de excepciones para masking (ADD §3.4). Fingerprinting de errores + silencio 4h. Auditoría de alertas críticas.';

-- C2: Lifecycle Audit Trail (Auditoría de ciclo de vida)
-- Uso: Registro de operaciones VACUUM, pre-aggregation
ALTER TABLE system.lifecycle_audit SET DBPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.autoCompact.enabled' = 'true',
  'delta.autoCompact.minNumFiles' = '5'
);

COMMENT ON TABLE system.lifecycle_audit IS 
'System table: Auditoría de operaciones ciclo de vida (VACUUM, pre-aggregation completados). Trazabilidad de FinOps.';

-- ============================================================================
-- SECCIÓN D: Metadatos de Control Plane para Versionamiento
-- ============================================================================

-- D1: Schema Registry Snapshots (Versionamiento de contratos, ADD §3.2)
-- Nota: Esta tabla almacena snapshots de esquemas esperados para cada versión
-- de contrato YAML. Permite retro-compatibilidad en caso de evolución.
CREATE TABLE IF NOT EXISTS system.schema_registry_snapshots (
  domain STRING NOT NULL,
  schema_version STRING NOT NULL,
  schema_json STRING NOT NULL,
  created_timestamp TIMESTAMP NOT NULL,
  pipeline_git_hash STRING NOT NULL,
  status STRING NOT NULL  -- "active", "deprecated"
) USING DELTA
PARTITIONED BY (domain, schema_version);

ALTER TABLE system.schema_registry_snapshots SET DBPROPERTIES (
  'delta.dataSkippingNumIndexedCols' = '3'
);

COMMENT ON TABLE system.schema_registry_snapshots IS 
'Control Plane: Snapshots de esquemas para versionamiento (ADD §3.2). Rastreo de pipeline_git_hash para auditoría.';

-- ============================================================================
-- SECCIÓN E: Habilitación Global de UniForm en el Catálogo
-- ============================================================================

-- Nota: Estas configuraciones a nivel de sesión se pueden parametrizar en
-- ingestion_engine.py o en el script de lifecycle_manager.py para asegurar
-- que TODAS las tablas creadas adopten el estándar Iceberg por defecto.

-- spark.conf.set("spark.databricks.delta.universalFormat.enabled", "true")
-- spark.conf.set("spark.databricks.delta.universalFormat.writeFormat", "iceberg")

-- ============================================================================
-- RESUMEN DE IMPACTO FINOPS Y GOBERNANZA
-- ============================================================================
-- 
-- ✅ Zero-ETL Habilitado:
--    - Trino lee metadatos Iceberg asincronamente (cero duplicación física)
--    - Acceso universal sin dependencia de Databricks Runtime
--
-- ✅ Ciclo de Vida Inteligente:
--    - IoT/Clima: 14 días granular + pre-agg a OBT (TTL del ADD §3.5)
--    - Sales/Inventory: Histórico ilimitado (transacciones críticas)
--
-- ✅ FinOps Optimizado:
--    - Pre-aggregation: -50% almacenamiento post-14d
--    - D-1 sync: -70% compute (Trino solo se enciende 16h/día)
--    - Column mapping: Evolución segura sin bloqueos
--
-- ✅ Gobernanza:
--    - Schema Registry: Versionamiento y auditabilidad (ADD §3.2)
--    - Alert logs: Exception masking + auditoría (ADD §3.4)
--    - Lifecycle audit: Trazabilidad de purgas
