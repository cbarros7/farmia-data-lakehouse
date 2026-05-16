"""Escudo Pydantic: Validación estricta de contratos de ingesta.

Fail-Fast: Valida YAML antes de provisioning.
"""

from pydantic import BaseModel, Field, validator, root_validator
from typing import List, Optional, Dict, Any, Literal
from enum import Enum


class DomainType(str, Enum):
    """Enumeración de dominios arquetípicos permisibles."""
    SALES_ONLINE = "sales_online"
    INVENTORY_ERP = "inventory_erp"
    IOT_SENSORS = "iot_sensors"
    WEATHER_EXTERNAL = "weather_external"


class SourceType(str, Enum):
    """Tipos de fuente soportados por el motor de ingesta."""
    AUTO_LOADER = "auto_loader"
    STREAMING = "streaming"
    BATCH = "batch"


class SourceFormat(str, Enum):
    """Formatos de datos soportados."""
    JSON = "json"
    PARQUET = "parquet"
    AVRO = "avro"
    CSV = "csv"


class SinkMode(str, Enum):
    """Estrategias de escritura: idempotencia determinística según dominio."""
    APPEND = "append"
    MERGE_INTO = "merge_into"
    OVERWRITE = "overwrite"


class PipelineInfo(BaseModel):
    """Metadatos de identidad del pipeline (Quanta arquitectónico)."""
    domain: str = Field(..., description="Dominio funcional (sales, inventory, iot, weather)")
    subdomain: str = Field(..., description="Subdominio específico (ej. online, external)")
    version: str = Field(..., description="Versión semántica del contrato (ej. 1.0.0)")
    owner: str = Field(..., description="Equipo propietario del pipeline")
    description: str = Field(..., description="Descripción del propósito del pipeline")


class SourceConfig(BaseModel):
    """Configuración de fuente de datos: Auto Loader con opciones dinámicas."""
    type: SourceType = Field(SourceType.AUTO_LOADER, description="Tipo de fuente")
    format: SourceFormat = Field(..., description="Formato de datos (json, parquet, avro)")
    path: str = Field(..., description="Ruta ADLS Gen2 (ej. abfss://landing@farmia.dfs.core.windows.net/weather/)")
    options: Dict[str, str] = Field(
        default_factory=dict,
        description="Opciones CloudFiles (cloudFiles.useIncrementalListing, cloudFiles.maxBytesPerTrigger, etc.)"
    )

    @validator("path")
    def validate_adls_path(cls, v: str) -> str:
        """Validar que path sea una ruta ADLS Gen2 válida."""
        if not v.startswith("abfss://"):
            raise ValueError(f"Path must be ADLS Gen2 format (abfss://...), got: {v}")
        return v


class FieldDefinition(BaseModel):
    """Definición de campo en esquema (sin inferencia, explicit es mejor)."""
    name: str = Field(..., description="Nombre del campo")
    type: str = Field(..., description="Tipo Spark SQL (StringType, IntegerType, TimestampType, etc.)")
    nullable: bool = Field(True, description="¿Campo nullable?")


class BusinessRule(BaseModel):
    """Regla de negocio dinámica: condición + acción (YAML-driven, no hardcodeada)."""
    name: str = Field(..., description="Identificador único de regla")
    condition: str = Field(..., description="Expresión Spark SQL (ej. 'temperature < -50 OR temperature > 60')")
    action: Literal["QUARANTINE", "WARN", "ENRICH"] = Field(..., description="Acción si condición es verdadera")
    reason: Optional[str] = Field(None, description="Motivo de la acción (para auditoría)")


class SchemaValidation(BaseModel):
    """Validación estricta de esquema: rechaza evolución no autorizada."""
    schema_evolution_mode: Literal["none", "additive", "all"] = Field(
        "none",
        description="cloudFiles.schemaEvolutionMode: 'none'=rechaza cambios, 'additive'=permite campos nuevos"
    )
    rescued_data_column: str = Field(
        "_rescued_data",
        description="Nombre de columna que captura datos corruptos (NULL si todas columnas válidas)"
    )
    fields: Optional[List[FieldDefinition]] = Field(
        None,
        description="Definición explícita de campos esperados (si None, inferencia deshabilitada)"
    )
    business_rules: Optional[List[BusinessRule]] = Field(
        None,
        description="Reglas de negocio dinámicas a aplicar post-parsing (YAML-driven)"
    )


class CircuitBreaker(BaseModel):
    """Circuit Breaker: degradación automática si tasa de errores excede umbral."""
    max_error_percentage: float = Field(
        ...,
        ge=0,
        le=100,
        description="Porcentaje de errores que dispara degradación (ej. 5%)"
    )
    action: Literal["degrade_mode", "fail_fast", "alert_only"] = Field(
        "degrade_mode",
        description="Acción si error% > umbral: degrade (DLQ silent), fail_fast (aborta job), alert_only (solo alerta)"
    )
    alert_threshold_minutes: int = Field(
        10,
        ge=1,
        description="Minutos entre alertas recurrentes en estado OPEN"
    )
    degradation_duration_minutes: int = Field(
        30,
        ge=5,
        description="Duración de degradación antes de intentar recuperación (HALF_OPEN)"
    )
    auto_recover: bool = Field(True, description="¿Auto-recuperar tras degradation_duration_minutes?")


class RetryPolicy(BaseModel):
    """Política de reintentos con exponential backoff: resilencia anti-frágil."""
    strategy: Literal["exponential_backoff", "fixed", "linear"] = Field(
        "exponential_backoff",
        description="Estrategia de reintento"
    )
    max_retries: int = Field(
        5,
        ge=1,
        le=20,
        description="Máximo número de reintentos (no reintentar infinitamente)"
    )
    initial_interval_seconds: float = Field(
        2.0,
        ge=0.1,
        description="Intervalo inicial en segundos (ej. 2s para primer reintento)"
    )
    max_interval_seconds: float = Field(
        60.0,
        ge=1,
        description="Intervalo máximo (cap para exponential backoff)"
    )
    multiplier: float = Field(
        2.0,
        ge=1.0,
        description="Multiplicador exponencial (ej. 2.0 = duplica intervalo cada intento)"
    )
    jitter_enabled: bool = Field(
        True,
        description="¿Añadir jitter para evitar thundering herd? (recomendado True)"
    )

    @validator("max_interval_seconds")
    def validate_max_greater_than_initial(cls, v: float, values: Dict[str, Any]) -> float:
        """Asegurar que max_interval >= initial_interval."""
        if "initial_interval_seconds" in values and v < values["initial_interval_seconds"]:
            raise ValueError(
                f"max_interval_seconds ({v}) must be >= initial_interval_seconds ({values['initial_interval_seconds']})"
            )
        return v


class LateData(BaseModel):
    """Gestión de datos tardíos: watermarking con tolerancia explícita."""
    enabled: bool = Field(True, description="¿Habilitar captura de late data?")
    max_delay_minutes: int = Field(
        ...,
        ge=5,
        description="Máximo retraso permitido (ej. 30min)"
    )
    table_name: str = Field(
        ...,
        description="Tabla Delta para almacenar late data (ej. bronze.weather_late_data)"
    )
    storage_days: int = Field(
        7,
        ge=1,
        description="Días de retención de late data antes de purga"
    )


class ExceptionMasking(BaseModel):
    """Exception Masking: prevenir alert fatigue mediante silenciamiento dinámico."""
    enabled: bool = Field(False, description="¿Habilitar silenciamiento de alertas repetidas?")
    silence_expires_hours: int = Field(
        4,
        ge=1,
        description="Horas de silencio tras primera alerta de error específico"
    )
    alert_logs_table: str = Field(
        "system.alert_logs",
        description="Tabla de auditoría de alertas (fingerprint -> last_alerted)"
    )


class Watchdog(BaseModel):
    """Watchdog: monitoreo de ejecución sin fallar (timeout suave)."""
    enabled: bool = Field(True, description="¿Habilitar watchdog?")
    max_execution_minutes: int = Field(
        45,
        ge=10,
        description="Máximo tiempo permitido para batch (ej. 45min)"
    )
    check_interval_seconds: int = Field(
        30,
        ge=5,
        description="Intervalo de chequeo de timeout (ej. 30s)"
    )
    alert_action: Literal["warn_no_fail", "fail", "cancel_job"] = Field(
        "warn_no_fail",
        description="Acción si timeout: warn_no_fail (continúa con alerta), fail (aborta batch), cancel_job (mata job)"
    )


class DLQConfig(BaseModel):
    """Dead Letter Queue: captura, reintento y observabilidad de datos corruptos."""
    circuit_breaker: CircuitBreaker = Field(..., description="Estrategia de degradación")
    retry_policy: RetryPolicy = Field(..., description="Política de reintentos con exponential backoff")
    late_data: Optional[LateData] = Field(None, description="Configuración de late data handling")
    exception_masking: Optional[ExceptionMasking] = Field(None, description="Silenciamiento de alertas repetidas")
    watchdog: Optional[Watchdog] = Field(None, description="Monitoreo de ejecución")


class MetadataInjection(BaseModel):
    """Inyección de metadatos: campos configurables para auditoría."""
    name: str = Field(..., description="Nombre del metadato (ej. _batch_id, _pipeline_git_hash)")
    expression: str = Field(
        ...,
        description="Expresión Spark SQL segura (solo lit, current_timestamp, col, current_date, unix_timestamp)"
    )
    datatype: Optional[str] = Field(None, description="Tipo esperado (ej. BIGINT, STRING, TIMESTAMP)")

    @validator("expression")
    def validate_safe_expression(cls, v: str) -> str:
        """Prevenir SQL injection: solo funciones allowlist permitidas."""
        import re
        allowed = {"lit", "current_timestamp", "col", "current_date", "unix_timestamp"}
        if not any(pattern in v for pattern in [f"{f}(" for f in allowed]):
            raise ValueError(f"Expression not allowed: {v}. Only {allowed} functions permitted")
        if any(char in v for char in [";", "--", "/*", "*/"]):
            raise ValueError(f"Expression contains forbidden characters: {v}")
        return v


class Watermarking(BaseModel):
    """Watermarking: deduplicación de eventos tardíos con límites de RAM."""
    enabled: bool = Field(False, description="¿Habilitar watermarking?")
    event_time_column: str = Field(
        ...,
        description="Nombre columna timestamp de evento (ej. forecast_date, transaction_time)"
    )
    delayed_threshold_minutes: int = Field(
        ...,
        ge=5,
        description="Ventana de tolerancia para datos en tiempo (ej. 30min)"
    )
    allowed_lateness_minutes: int = Field(
        5,
        ge=0,
        description="Minutos adicionales de tolerancia post-watermark (ej. 5min)"
    )
    max_state_bytes: int = Field(
        1_000_000_000,
        ge=100_000_000,
        description="Límite máximo de estado en memoria (ej. 1GB = 1e9 bytes)"
    )
    checkpoint_interval_minutes: int = Field(
        5,
        ge=1,
        description="Intervalo de persistencia de estado a ADLS (ej. 5min)"
    )
    state_location: str = Field(
        ...,
        description="Ubicación ADLS para checkpoints de watermarking (ej. abfss://checkpoints@farmia...)"
    )


class TransformationsConfig(BaseModel):
    """Transformaciones: inyección de metadatos, reglas dinámicas, pre-agregación."""
    pattern: str = Field(
        "metadata_injection",
        description="Patrón de transformación (metadata_injection, pre_aggregation, acl_layer)"
    )
    spark_memory_config: Optional[Dict[str, str]] = Field(
        None,
        description="Configuración Spark (ej. {'spark.executor.memory': '8g'})"
    )
    watermarking: Optional[Watermarking] = Field(
        None,
        description="Configuración de watermarking para manejo de late data"
    )
    metadata_injection: List[MetadataInjection] = Field(
        ...,
        description="Campos de metadatos a inyectar (definidos en YAML)"
    )
    pre_aggregation: Optional[Dict[str, Any]] = Field(
        None,
        description="Configuración pre-agregación (antes de purga): rollup rules, retention (ej. IoT 14 días)"
    )
    acl_layer: Optional[Dict[str, Any]] = Field(
        None,
        description="Anti-Corruption Layer: parametrización de APIs externas (zero-copy masking)"
    )

    @validator("metadata_injection")
    def validate_metadata_not_empty(cls, v: List[MetadataInjection]) -> List[MetadataInjection]:
        """Validar que metadata_injection no esté vacío."""
        if not v or len(v) == 0:
            raise ValueError("metadata_injection debe contener al menos un campo")
        return v


class SinkConfig(BaseModel):
    """Sink: estrategia de escritura idempotente (MERGE vs APPEND según dominio)."""
    format: str = Field("delta", description="Formato de escritura (delta es estándar)")
    mode: SinkMode = Field(
        ...,
        description="Modo de escritura: append (IoT/Weather), merge_into (Sales/Inventory)"
    )
    table_name: str = Field(..., description="Nombre tabla Delta (ej. silver.weather_external)")
    options: Dict[str, str] = Field(
        default_factory=dict,
        description="Opciones Spark SQL (delta.enableIcebergCompatV2, delta.universalFormat.enabledFormats)"
    )
    partition_by: List[str] = Field(
        default_factory=list,
        description="Columnas de particionamiento (ej. [city, forecast_date])"
    )
    merge_key: Optional[List[str]] = Field(
        None,
        description="Columnas de clave para MERGE (obligatorio si mode=merge_into)"
    )
    lifecycle: Optional[Dict[str, Any]] = Field(
        None,
        description="Políticas de ciclo de vida (ttl, sync_window, purge_strategy)"
    )
    optimization: Optional[Dict[str, Any]] = Field(
        None,
        description="Optimizaciones (z_order_by, vacuum_days, compaction_interval)"
    )

    @root_validator
    def validate_domain_sink_mode(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Validar Idempotencia Dinámica: emparejar dominio con estrategia."""
        pipeline = values.get("pipeline_info")
        sink = values.get("sink")
        
        if pipeline and sink:
            domain = pipeline.domain
            mode = sink.mode
            
            high_volume_domains = {DomainType.SALES_ONLINE.value, DomainType.INVENTORY_ERP.value}
            append_only_domains = {DomainType.WEATHER_EXTERNAL.value, DomainType.IOT_SENSORS.value}
            
            if domain in high_volume_domains and mode != SinkMode.MERGE_INTO:
                raise ValueError(f"Dominio {domain} REQUIERE sink.mode = 'merge_into'")
            if domain in append_only_domains and mode != SinkMode.APPEND:
                raise ValueError(f"Dominio {domain} REQUIERE sink.mode = 'append'")
                
        return values

    @root_validator
    def validate_merge_key_for_merge_into(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Si mode=merge_into, merge_key es obligatorio."""
        if values.get("mode") == SinkMode.MERGE_INTO and not values.get("merge_key"):
            raise ValueError(
                "merge_key es obligatorio cuando mode='merge_into'"
            )
        return values


class TableSchema(BaseModel):
    """Definición de esquema para tabla: nombre, capa (bronze/silver/gold), columnas."""
    name: str = Field(..., description="Nombre tabla (ej. bronze.weather_external)")
    layer: Literal["bronze", "silver", "gold", "quarantine", "late_data"] = Field(
        ...,
        description="Capa de datos"
    )
    columns: List[FieldDefinition] = Field(
        ...,
        description="Definición de columnas (ej. [{name: _batch_id, type: BIGINT, nullable: false}])"
    )
    partition_by: Optional[List[str]] = Field(
        None,
        description="Columnas de particionamiento"
    )


class TablesConfig(BaseModel):
    """Configuración de todas las tablas a crear (Bronze, Silver, Quarantine, Late Data, Gold)."""
    tables: List[TableSchema] = Field(
        ...,
        description="Lista de todas las tablas del pipeline con sus esquemas"
    )


class IngestionContract(BaseModel):
    """Contrato de ingesta: validación exhaustiva de configuración YAML.

    Fail-Fast: detecta anomalías PRE-provisioning. SSoT para toda lógica del motor.
    Ejemplo: config = IngestionContract(**yaml.safe_load(open('config.yaml')))
    """
    pipeline_info: PipelineInfo = Field(..., description="Metadatos de identidad del pipeline")
    source: SourceConfig = Field(..., description="Configuración de fuente")
    schema_validation: SchemaValidation = Field(..., description="Validación estricta de esquema")
    dlq: DLQConfig = Field(..., description="Dead Letter Queue: resilencia, observabilidad")
    transformations: TransformationsConfig = Field(..., description="Transformaciones, metadatos")
    sink: SinkConfig = Field(..., description="Estrategia idempotente de escritura")
    tables_config: TablesConfig = Field(..., description="Definición de todas las tablas con esquemas")
