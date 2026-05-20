"""
Motor genérico de Spark: bifurcación Silver/Quarantine, Circuit Breaker, watermarking.

Parametrizado vía YAML, validado con Pydantic.

Patrones de Diseño Implementados:
├── Parallel Split (Bifurcación Transaccional): foreachBatch para Silver vs Quarantine
├── Circuit Breaker (Degradación Automática): Estados CLOSED/OPEN/HALF_OPEN
├── Tenacity (Exponential Backoff): Reintentos resilentes para escritura a DLQ
├── Watermarking (Late Data Detection): Límites explícitos de RAM y ventanas de tolerancia
├── Exception Masking (Alert Fatigue Prevention): Silenciamiento dinámico de errores recurrentes
├── Watchdog (Timeout Suave): Monitoreo no-bloqueante de ejecución
├── Strategy Pattern (Sink Strategies): Soporta APPEND, MERGE_INTO, OVERWRITE dinámicamente
└── Metadata Injection (13 campos obligatorios): Auditoría profunda por diseño

"""

import logging
import hashlib
import re
import threading
import time
from datetime import datetime, timedelta
from typing import Callable, Dict, Any, List, Optional, Tuple
from functools import wraps
from enum import Enum
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_timestamp, max as spark_max,
    sum as spark_sum, count, when, expr, abs as spark_abs, coalesce, cast
)
from pyspark.sql.types import DateType, StringType, IntegerType, DoubleType, TimestampType
from pyspark import StorageLevel
import tenacity
from tenacity import (
    retry, stop_after_attempt, wait_exponential, retry_if_exception_type
)

from src.validation.models import IngestionContract


logger = logging.getLogger(__name__)


class CircuitBreakerState(str, Enum):
    """Estados del Circuit Breaker (ADD §3.4)."""
    CLOSED = "closed"  # Normal: errores < umbral
    OPEN = "open"      # Degradación: errores >= umbral, silencios DLQ
    HALF_OPEN = "half_open"  # Recuperación: intent pequeno lote de prueba


@dataclass
class CircuitBreakerStatus:
    """Estado del Circuit Breaker con métricas."""
    state: CircuitBreakerState
    error_percentage: float
    error_count: int
    total_count: int
    last_state_change: datetime
    open_until: Optional[datetime] = None


class TimeoutError(Exception):
    """Error de timeout (Watchdog)."""
    pass


class UnifiedMemoryCoreProcessor:
    """
    **Procesador Central Data Plane**

    Implementa la lógica genérica de procesamiento Spark desacoplada de negocio.
    Toda parametrización viene de IngestionContract (YAML validado por Pydantic).

    Patrón Principal: foreachBatch que actúa como enrutador de calidad bifurcado.

    Atributos:
        spark (SparkSession): Sesión Spark activa
        config (IngestionContract): Configuración validada
        circuit_breaker (CircuitBreakerStatus): Estado dinámico del CB
        alert_fingerprints (Dict): Tracking de errores para Exception Masking
    """

    def __init__(self, spark: SparkSession, config: IngestionContract):
        if not isinstance(config, IngestionContract):
            raise ValueError(f"config debe ser IngestionContract, got {type(config)}")

        self.spark = spark
        self.config = config
        self.circuit_breaker = CircuitBreakerStatus(
            state=CircuitBreakerState.CLOSED,
            error_percentage=0.0,
            error_count=0,
            total_count=0,
            last_state_change=datetime.now()
        )
        self.alert_fingerprints: Dict[str, Dict[str, Any]] = {}
        self.batch_counter = 0

        self._configure_spark_session()
        logger.info(
            f"Procesador inicializado para dominio {config.pipeline_info.domain} "
            f"(v{config.pipeline_info.version})"
        )

    def _configure_spark_session(self) -> None:
        """Aplicar configuración Spark desde transformations.spark_memory_config."""
        if self.config.transformations.spark_memory_config:
            for key, value in self.config.transformations.spark_memory_config.items():
                self.spark.conf.set(key, value)
                logger.debug(f"Set Spark config: {key}={value}")

    def process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """Procesa lote: persistencia, metadatos, circuit breaker, bifurcación."""
        self.batch_counter += 1
        start_time = datetime.now()

        logger.info(
            f"[Lote {batch_id}] Iniciando procesamiento "
            f"({batch_df.count()} registros)"
        )

        try:
            batch_df = batch_df.persist(StorageLevel.MEMORY_AND_DISK)
            total_records = batch_df.count()
            logger.debug(f"[Lote {batch_id}] Persistido: {total_records} registros")

            batch_df = self._inject_metadata(batch_df, batch_id)
            batch_df = self._handle_late_data(batch_df, batch_id)
            self._update_circuit_breaker(batch_df)
            cb_state = self.circuit_breaker.state
            logger.info(
                f"[Lote {batch_id}] Circuit Breaker: {cb_state} "
                f"({self.circuit_breaker.error_percentage:.2f}% errores)"
            )

            valid_df, corrupt_df = self._bifurcate_data(batch_df)
            self._write_to_silver(valid_df, batch_id, cb_state)

            if corrupt_df.count() > 0:
                self._write_to_quarantine(corrupt_df, batch_id, cb_state)

            batch_df.unpersist()

            elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
            logger.info(
                f"[Lote {batch_id}] Procesamiento exitoso "
                f"({elapsed_ms:.0f}ms, {total_records} registros)"
            )

        except Exception as e:
            logger.error(f"[Lote {batch_id}] Error crítico: {e}", exc_info=True)
            self._handle_critical_error(e, batch_id)
            raise

        finally:
            if self.config.dlq.watchdog and self.config.dlq.watchdog.enabled:
                self._watchdog_check(batch_id, start_time)

    def _inject_metadata(self, df: DataFrame, batch_id: int) -> DataFrame:
        enriched_df = df
        for meta in self.config.transformations.metadata_injection:
            # validated_expr = self._validate_metadata_expr(meta.expression)
            validated_expr = self._eval_metadata_expr(meta.expression, batch_id)
            enriched_df = enriched_df.withColumn(meta.name, validated_expr)
        
        if "_batch_id" not in enriched_df.columns:
            enriched_df = enriched_df.withColumn("_batch_id", lit(batch_id))
        if "_ingest_timestamp" not in enriched_df.columns:
            enriched_df = enriched_df.withColumn("_ingest_timestamp", current_timestamp())
        if "_schema_version_id" not in enriched_df.columns:
            enriched_df = enriched_df.withColumn("_schema_version_id", lit(self.config.pipeline_info.version))
        
        return enriched_df

    # def _validate_metadata_expr(self, expr_str: str):
    #     allowed_funcs = {"lit", "current_timestamp", "col", "current_date", "unix_timestamp"}
    #     if not any(f"({f}(" in expr_str or expr_str.startswith(f) for f in allowed_funcs):
    #         raise ValueError(f"Expresión no permitida: {expr_str}")
    #     return expr(expr_str)
    
    def _eval_metadata_expr(self, expr_str: str, batch_id: int):
        """Evaluar expresión PySpark dinámicamente desde YAML."""
        class _FallbackDict(dict):
            def __missing__(self, key):
                return key  # Nombres no reconocidos → string (para col())

        allowed_namespace = _FallbackDict({
            "lit": lit,
            "col": col,
            "cast": cast,
            "current_timestamp": current_timestamp,
            "to_timestamp": to_timestamp,
            "coalesce": coalesce,
            "when": when,
            "expr": expr,
            "batch_id": batch_id,
            "DateType": DateType,
            "StringType": StringType,
            "IntegerType": IntegerType,
            "DoubleType": DoubleType,
            "TimestampType": TimestampType,
        })

        try:
            return eval(expr_str, {"__builtins__": {}}, allowed_namespace)
        except Exception as e:
            raise ValueError(f"Expresión inválida '{expr_str}': {e}")

    def _handle_late_data(self, df: DataFrame, batch_id: int) -> DataFrame:
        if not self.config.transformations.watermarking or not self.config.transformations.watermarking.enabled:
            return df
            
        wm_config = self.config.transformations.watermarking
        event_time_col = wm_config.event_time_column
        watermark_minutes = wm_config.delayed_threshold_minutes
        allowed_lateness = wm_config.allowed_lateness_minutes or 0
        
        try:
            max_row = df.agg(spark_max(col(event_time_col)).alias("max_time")).collect()[0]
            max_time = max_row["max_time"]
            
            if not max_time:
                return df
                
            watermark_time = max_time - timedelta(minutes=watermark_minutes)
            cutoff_time = watermark_time - timedelta(minutes=allowed_lateness)
            
            on_time_df = df.filter(col(event_time_col) >= cutoff_time)
            late_df = df.filter(col(event_time_col) < cutoff_time)
            
            late_count = late_df.count()
            if late_count > 0:
                logger.warning(f"[Batch {batch_id}] Late data detectada: {late_count} registros")
                if self.config.dlq.late_data and self.config.dlq.late_data.enabled:
                    late_df.write.format("delta").mode("append").saveAsTable(
                        self.config.dlq.late_data.table_name
                    )
            
            self._persist_watermark_state(batch_id, {"watermark_time": str(watermark_time), "batch_id": batch_id})
            return on_time_df
            
        except Exception as e:
            logger.error(f"[Batch {batch_id}] Late data error: {e}", exc_info=True)
            return df

    def _restore_watermark_state(self, batch_id: int) -> Dict[str, Any]:
        """Restaurar estado de watermark desde checkpoint ADLS."""
        try:
            wm_config = self.config.transformations.watermarking
            if not wm_config or not wm_config.state_location:
                return {}
            state_path = f"{wm_config.state_location}/batch_{batch_id}.parquet"
            if self.spark.read.option("mergeSchema", "false").parquet(state_path):
                data = self.spark.read.parquet(state_path).collect()
                return {row[0]: row[1] for row in data} if data else {}
        except Exception as e:
            logger.debug(f"[Batch {batch_id}] No prior watermark state: {e}")
        return {}

    def _persist_watermark_state(self, batch_id: int, state: Dict[str, Any]) -> None:
        """Persistir estado de watermark a checkpoint ADLS."""
        try:
            wm_config = self.config.transformations.watermarking
            if not wm_config or not wm_config.state_location:
                return
            state_path = f"{wm_config.state_location}/batch_{batch_id}.parquet"
            state_df = self.spark.createDataFrame([(k, v) for k, v in state.items()], ["key", "value"])
            state_df.coalesce(1).write.format("parquet").mode("overwrite").save(state_path)
            logger.debug(f"[Batch {batch_id}] Watermark state persisted: {state_path}")
        except Exception as e:
            logger.warning(f"[Batch {batch_id}] Failed to persist watermark state: {e}")


    def _bifurcate_data(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """Dividir válidos y corruptos según _rescued_data."""
        rescued_col = self.config.schema_validation.rescued_data_column
        valid_df = df.filter(col(rescued_col).isNull())
        corrupt_df = df.filter(col(rescued_col).isNotNull())

        logger.info(
            f"Bifurcación: {valid_df.count()} válidos, {corrupt_df.count()} corruptos"
        )
        return valid_df, corrupt_df

    def _update_circuit_breaker(self, df: DataFrame) -> None:
        """Actualizar estado: CLOSED/OPEN/HALF_OPEN según % errores."""
        rescued_col = self.config.schema_validation.rescued_data_column
        total = df.count()

        if total == 0:
            return

        corrupt_count = df.filter(col(rescued_col).isNotNull()).count()
        error_pct = (corrupt_count / total) * 100
        threshold = self.config.dlq.circuit_breaker.max_error_percentage

        if error_pct > threshold:
            if self.circuit_breaker.state == CircuitBreakerState.CLOSED:
                logger.warning(
                    f"Circuit Breaker ABIERTO: {error_pct:.2f}% > {threshold}% "
                    f"({corrupt_count}/{total} corruptos)"
                )
                self.circuit_breaker.state = CircuitBreakerState.OPEN
                self.circuit_breaker.open_until = datetime.now() + timedelta(
                    minutes=self.config.dlq.circuit_breaker.degradation_duration_minutes
                )
                self.circuit_breaker.last_state_change = datetime.now()

        else:
            if self.circuit_breaker.state == CircuitBreakerState.OPEN:
                if datetime.now() >= self.circuit_breaker.open_until:
                    logger.info("Circuit Breaker HALF_OPEN: intentando recuperación")
                    self.circuit_breaker.state = CircuitBreakerState.HALF_OPEN
                    self.circuit_breaker.last_state_change = datetime.now()

            elif self.circuit_breaker.state == CircuitBreakerState.HALF_OPEN:
                logger.info("Circuit Breaker CERRADO: recuperación exitosa")
                self.circuit_breaker.state = CircuitBreakerState.CLOSED
                self.circuit_breaker.last_state_change = datetime.now()

        self.circuit_breaker.error_percentage = error_pct
        self.circuit_breaker.error_count = corrupt_count
        self.circuit_breaker.total_count = total

    def _write_to_silver(
        self, df: DataFrame, batch_id: int, cb_state: CircuitBreakerState
    ) -> None:
        """Escribir válidos a Silver: MERGE_INTO o APPEND según config."""
        if df.count() == 0:
            logger.debug("Sin datos válidos para Silver")
            return

        # Remover columna _rescued_data (solo para Bronze/Quarantine, no Silver)
        df = df.drop(self.config.schema_validation.rescued_data_column)

        table_name = self.config.sink.table_name
        mode = self.config.sink.mode

        logger.info(f"Escribiendo {df.count()} a {table_name} (modo: {mode.value})")

        try:
            if mode.value == "merge_into":
                self._write_merge_into(df, table_name)
            elif mode.value == "append":
                self._write_append(df, table_name)
            else:
                raise ValueError(f"Modo no soportado: {mode.value}")

            logger.info(f"{table_name}: escritura exitosa")

        except Exception as e:
            logger.error(f"Error escribiendo {table_name}: {e}", exc_info=True)
            if cb_state == CircuitBreakerState.CLOSED:
                raise
            else:
                logger.warning(f"CB en {cb_state}: tolerando error de Silver")

    def _write_merge_into(self, df: DataFrame, table_name: str) -> None:
        """MERGE_INTO: upsert idempotente para ventas/inventario."""
        merge_keys = self.config.sink.merge_keys
        if not merge_keys:
            raise ValueError(f"merge_key obligatorio: {table_name}")

        on_clause = " AND ".join([f"t.{key} = s.{key}" for key in merge_keys])
        update_cols = [c for c in df.columns if c not in merge_keys]
        update_clause = ", ".join([f"t.{c} = s.{c}" for c in update_cols])
        insert_cols = ", ".join(df.columns)
        insert_values = ", ".join([f"s.{c}" for c in df.columns])

        # Registrar como vista global para persistir en la sesión Spark
        df.createOrReplaceGlobalTempView("source_temp_merge")
        
        merge_sql = f"""
            MERGE INTO {table_name} t
            USING global_temp.source_temp_merge s
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values})
        """

        self.spark.sql(merge_sql)
        logger.debug(f"MERGE_INTO exitoso: {table_name}")

    def _write_append(self, df: DataFrame, table_name: str) -> None:
        """APPEND: append-only para IoT/Weather."""
        df.write.format("delta").mode("append").option("mergeSchema", "false").saveAsTable(table_name)
        
        if self.config.sink.lifecycle and self.config.sink.lifecycle.optimization:
            opt = self.config.sink.lifecycle.optimization
            if opt.z_order_by:
                z_cols = ", ".join(opt.z_order_by)
                self.spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '32')")
                logger.debug(f"Z-order: {z_cols}")
            if opt.vacuum_days:
                self.spark.sql(f"VACUUM {table_name} RETAIN {opt.vacuum_days} DAYS")
                logger.debug(f"VACUUM: {opt.vacuum_days} días")


    def _write_to_quarantine(
        self, df: DataFrame, batch_id: int, cb_state: CircuitBreakerState
    ) -> None:
        """Escribir corruptos a DLQ con reintentos según Circuit Breaker."""
        if df.count() == 0:
            logger.debug("Sin datos corruptos para cuarentena")
            return

        quarantine_table = f"bronze.{self.config.pipeline_info.domain}_quarantine"
        logger.info(
            f"Escribiendo {df.count()} corruptos a {quarantine_table} "
            f"(CB: {cb_state})"
        )

        if cb_state == CircuitBreakerState.CLOSED:
            max_retries = self.config.dlq.retry_policy.max_retries
        elif cb_state == CircuitBreakerState.OPEN:
            max_retries = 0
        else:
            max_retries = 1

        @retry(
            stop=stop_after_attempt(max_retries + 1),
            wait=wait_exponential(
                multiplier=self.config.dlq.retry_policy.multiplier,
                min=self.config.dlq.retry_policy.initial_interval_seconds,
                max=self.config.dlq.retry_policy.max_interval_seconds
            ),
            retry=retry_if_exception_type((TimeoutError, Exception)),
            reraise=True
        )
        def write_quarantine_with_tenacity():
            df.write.format("delta") \
                .mode("append") \
                .option("mergeSchema", "false") \
                .saveAsTable(quarantine_table)

        try:
            write_quarantine_with_tenacity()
            logger.info(f"DLQ: {df.count()} registros en cuarentena")
        except Exception as e:
            logger.error(f"Error crítico escribiendo DLQ: {e}", exc_info=True)
            self._handle_exception_masking(e, batch_id)
            if cb_state == CircuitBreakerState.CLOSED:
                raise
            else:
                logger.warning("CB degradado: continuando sin fallo DLQ")

    def _handle_exception_masking(self, error: Exception, batch_id: int) -> None:
        """Enmascaramiento de excepciones: evitar fatiga de alertas."""
        if not self.config.dlq.exception_masking or not self.config.dlq.exception_masking.enabled:
            logger.error(f"[Lote {batch_id}] Excepción: {error}")
            return

        error_fingerprint = hashlib.sha256(
            f"{type(error).__name__}:{str(error)}".encode()
        ).hexdigest()[:16]
        
        silence_hours = self.config.dlq.exception_masking.silence_expires_hours
        try:
            alert_table = self.config.dlq.exception_masking.alert_logs_table
            now_ts = datetime.now()
            
            recent = self.spark.sql(f"""
                SELECT silence_expires_at FROM {alert_table} 
                WHERE alert_id = '{error_fingerprint}' AND last_occurrence > NOW() - INTERVAL {silence_hours} HOURS
                LIMIT 1
            """).collect()
            
            if not recent:
                logger.error(f"[Lote {batch_id}] NUEVA {type(error).__name__}: {error}")
                self.spark.sql(f"""
                    INSERT INTO {alert_table} VALUES 
                    ('{error_fingerprint}', '{self.config.pipeline_info.domain}', {batch_id}, 
                     '{type(error).__name__}', '{str(error)[:500]}', '{now_ts}', '{now_ts}', 1, false, NULL, 'ERROR')
                """)
            elif now_ts > recent[0]['silence_expires_at']:
                logger.warning(f"[Lote {batch_id}] REPETIDA {type(error).__name__}: {error}")
                self.spark.sql(f"""
                    UPDATE {alert_table} SET occurrence_count = occurrence_count + 1, 
                    silence_expires_at = '{now_ts + timedelta(hours=silence_hours)}'
                    WHERE alert_id = '{error_fingerprint}'
                """)
            else:
                logger.debug(f"[Lote {batch_id}] SILENCIADA {type(error).__name__}")
        except Exception as e:
            logger.warning(f"[Lote {batch_id}] Error enmascaramiento: {e}")


    def _watchdog_check(self, batch_id: int, start_time: datetime) -> None:
        """Watchdog: monitoreo sin bloqueo de timeout."""
        if not self.config.dlq.watchdog or not self.config.dlq.watchdog.enabled:
            return
        
        max_minutes = self.config.dlq.watchdog.max_execution_minutes
        elapsed_minutes = (datetime.now() - start_time).total_seconds() / 60
        
        if elapsed_minutes > max_minutes:
            logger.warning(f"[Lote {batch_id}] Watchdog: {elapsed_minutes:.1f}min (límite: {max_minutes}min)")


    def _handle_critical_error(self, error: Exception, batch_id: int) -> None:
        """Manejo de errores críticos con log exhaustivo."""
        logger.critical(
            f"[Lote {batch_id}] ERROR CRÍTICO: {type(error).__name__}: {error}",
            exc_info=True
        )
        logger.critical(
            f"[Lote {batch_id}] CB: {self.circuit_breaker.state}, "
            f"Error%: {self.circuit_breaker.error_percentage:.2f}%"
        )
