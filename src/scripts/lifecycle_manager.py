"""
Orquestador de Ciclo de Vida y TTL: Gestión Inteligente de Retención de Datos

Implementa políticas de retención asimétricas según dominio (ADD §3.5):
- IoT + Clima: TTL 14 días + Pre-aggregation obligatoria antes de VACUUM
- Sales + Inventory: Histórico ilimitado (datos transaccionales críticos)

Objetivo FinOps: Reducir almacenamiento -50% mediante pre-aggregation D-1 antes de purga.

Patrones Implementados:
├── Metadata-Driven Lifecycle: Lee contratos YAML para políticas por dominio
├── Validación Pre-VACUUM: Verifica que pre-aggregation se completó en Gold
├── Auditabilidad: Registra todas las operaciones en system.lifecycle_audit
└── Resiliencia: Exception masking + idempotencia de operaciones VACUUM

SSoT: ADD §3.5 (Ciclo de vida del dato, TTL 14 días para IoT/Clima)
"""

import logging
import hashlib
import yaml
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, datediff, to_date, max as spark_max, count
)

logger = logging.getLogger(__name__)


@dataclass
class LifecycleMeta:
    """Metadatos de políticas de ciclo de vida por dominio."""
    domain: str
    silver_table: str
    gold_table: str
    ttl_days: Optional[int]  # None = sin TTL (histórico ilimitado)
    requires_preagg: bool  # True si debe pre-aggreg antes de VACUUM
    last_aggregation_date: Optional[str] = None


class LifecycleManager:
    """
    Gestor de políticas de retención y ciclo de vida del dato.
    
    Responsabilidades:
    1. Leer contratos YAML de dominios
    2. Aplicar TTL asimétricamente (14 días solo IoT/Clima)
    3. Validar pre-aggregation antes de ejecutar VACUUM
    4. Auditar todas las operaciones en system.lifecycle_audit
    5. Ejecutar VACUUM con opciones FinOps
    """

    def __init__(self, spark: SparkSession, control_plane_path: str):
        """
        Inicializar gestor de ciclo de vida.
        
        Args:
            spark: SparkSession activa
            control_plane_path: Ruta ADLS a contratos YAML
                (ej. abfss://control-plane@farmia.dfs.core.windows.net/ingestion/)
        """
        self.spark = spark
        self.control_plane_path = control_plane_path
        self.domains_config: Dict[str, LifecycleMeta] = {}
        self._load_domain_policies()
        logger.info("✓ Lifecycle Manager inicializado")

    def _load_domain_policies(self) -> None:
        """
        Lee el contrato desde: self.control_plane_path + "ingestion_contract.yaml"
        """
        # [Se requerirá parsear el .yaml vía dbutils o ADLS API usando PyYAML]
        # Implementación mínima transitoria si yaml no está inyectado:
        # TODO: Cargar y parsear Yaml real.
        
        logger.info(f"Cargando contrato desde: {self.control_plane_path}ingestion_contract.yaml")
        
        # Mapeo hardcodeado temporal hasta integrar el parser YAML
        ttl_policy = {
            "sales_online": {"ttl_days": None, "requires_preagg": False},
            "inventory_erp": {"ttl_days": None, "requires_preagg": False},
            "iot_sensors": {"ttl_days": 14, "requires_preagg": True},
            "weather_external": {"ttl_days": 14, "requires_preagg": True},
        }

        table_mapping = {
            "sales_online": "gold.sales_fact_d1",
            "inventory_erp": "gold.inventory_fact_d1",
            "iot_sensors": "gold.iot_daily_obt",
            "weather_external": "gold.weather_daily_obt",
        }

        for domain, policy in ttl_policy.items():
            self.domains_config[domain] = LifecycleMeta(
                domain=domain,
                silver_table=f"silver.{domain}",
                gold_table=table_mapping[domain],
                ttl_days=policy["ttl_days"],
                requires_preagg=policy["requires_preagg"]
            )
            logger.info(
                f"Política configurada: {domain} -> "
                f"TTL={policy['ttl_days']} días, "
                f"PreAgg={'Sí' if policy['requires_preagg'] else 'No'}"
            )

    def validate_preaggregation_complete(self, domain: str) -> bool:
        """
        Validar que la pre-aggregation se completó exitosamente (ADD §3.5).
        
        Estrategia:
        1. Leer fecha de último agregamiento en system.lifecycle_audit
        2. Comparar fecha vs. hoy
        3. Si data está fresca (última ejecución en últimas 25h), OK
        4. Si no hay datos o es antiguo, BLOQUEAR VACUUM
        
        Args:
            domain: Dominio a validar (ej. "iot_sensors")
            
        Returns:
            True si pre-aggregation está completo, False si está pendiente
        """
        try:
            # Consultar última ejecución exitosa de pre-aggregation
            audit_df = self.spark.sql(f"""
                SELECT MAX(completed_timestamp) as last_agg_time
                FROM system.lifecycle_audit
                WHERE domain = '{domain}'
                  AND operation = 'pre_aggregation'
                  AND status = 'SUCCESS'
            """).collect()

            if not audit_df or audit_df[0]["last_agg_time"] is None:
                logger.warning(
                    f"Pre-aggregation para {domain} nunca se ejecutó. "
                    f"BLOQUEANDO VACUUM hasta completar agregación."
                )
                return False

            last_agg_time = audit_df[0]["last_agg_time"]
            hours_since_agg = (datetime.now() - last_agg_time).total_seconds() / 3600

            if hours_since_agg <= 25:  # Ventana de 25h (tolerancia)
                logger.info(
                    f"Pre-aggregation para {domain} está fresco "
                    f"(hace {hours_since_agg:.1f}h). OK para VACUUM."
                )
                return True
            else:
                logger.warning(
                    f"Pre-aggregation para {domain} es antiguo "
                    f"(hace {hours_since_agg:.1f}h > 25h). BLOQUEANDO VACUUM."
                )
                return False

        except Exception as e:
            logger.error(f"Error validando pre-aggregation: {e}")
            return False

    def execute_vacuum(
        self,
        domain: str,
        retention_days: Optional[int] = None,
        dry_run: bool = False
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Ejecutar VACUUM con políticas FinOps (ADD §3.5).
        
        Flujo:
        1. Si TTL y requires_preagg: Validar que pre-aggregation está completo
        2. Si validación OK: Ejecutar VACUUM con retention_hours
        3. Registrar auditoría en system.lifecycle_audit
        4. Retornar éxito/fallo + estadísticas
        
        Args:
            domain: Dominio objetivo
            retention_days: Días de retención (None = usar política por defecto)
            dry_run: True para simular sin ejecutar
            
        Returns:
            Tupla (éxito: bool, estadísticas: dict)
        """
        meta = self.domains_config.get(domain)
        if not meta:
            logger.error(f"Dominio {domain} no configurado")
            return False, {"error": f"Dominio {domain} no encontrado"}

        # Determinar TTL real
        ttl = retention_days or meta.ttl_days
        if ttl is None:
            logger.info(f"{domain}: Sin TTL (histórico ilimitado). SKIPPING VACUUM.")
            return True, {"skipped": True, "reason": "No TTL configured"}

        # Validar pre-aggregation si es requerido
        if meta.requires_preagg:
            if not self.validate_preaggregation_complete(domain):
                msg = f"Pre-aggregation incompleto para {domain}. VACUUM bloqueado."
                logger.error(msg)
                self._audit_operation(domain, "vacuum", "BLOCKED", {"reason": msg})
                return False, {"error": msg}

        # Calcular ventana de retención
        retention_hours = ttl * 24
        silver_table = meta.silver_table

        try:
            logger.info(
                f"Ejecutando VACUUM: {silver_table} "
                f"(retención: {ttl} días = {retention_hours}h)"
            )

            if dry_run:
                logger.info(f"[DRY RUN] VACUUM {silver_table} RETAIN {retention_hours} HOURS")
                self._audit_operation(
                    domain, "vacuum_dryrun", "SUCCESS",
                    {"retention_hours": retention_hours}
                )
                return True, {"dryrun": True}
            else:
                # Ejecutar VACUUM
                self.spark.sql(
                    f"VACUUM {silver_table} RETAIN {retention_hours} HOURS"
                )
                logger.info(f"✓ VACUUM completado: {silver_table}")
                self._audit_operation(
                    domain, "vacuum", "SUCCESS",
                    {"retention_hours": retention_hours, "table": silver_table}
                )
                return True, {"vacuumed": True, "table": silver_table}

        except Exception as e:
            logger.error(f"✗ Error ejecutando VACUUM: {e}", exc_info=True)
            self._audit_operation(
                domain, "vacuum", "FAILED", {"error": str(e)}
            )
            return False, {"error": str(e)}

    def _audit_operation(
        self,
        domain: str,
        operation: str,
        status: str,
        metadata: Dict[str, Any]
    ) -> None:
        """
        Registrar operación en system.lifecycle_audit para trazabilidad.
        
        Args:
            domain: Dominio afectado
            operation: Tipo de operación (vacuum, pre_aggregation, etc.)
            status: SUCCESS, FAILED, BLOCKED
            metadata: Información adicional (JSON)
        """
        try:
            audit_row = {
                "domain": domain,
                "operation": operation,
                "status": status,
                "started_timestamp": datetime.now(),
                "completed_timestamp": datetime.now(),
                "metadata": str(metadata),
                "executed_by": "lifecycle_manager",
                "execution_environment": "databricks"
            }

            audit_df = self.spark.createDataFrame([audit_row])
            audit_df.write.format("delta").mode("append").saveAsTable(
                "system.lifecycle_audit"
            )
            logger.info(
                f"Auditoría registrada: {domain} / {operation} / {status}"
            )

        except Exception as e:
            logger.error(f"Error registrando auditoría: {e}")

    def analyze_storage_impact(self) -> DataFrame:
        """
        Análisis FinOps usando Metadatos Delta puro para costo O(1) de I/O.
        """
        try:
            stats_df = self.spark.createDataFrame([], schema="domain STRING, size_bytes BIGINT")
            for domain_key, meta in self.domains_config.items():
                try:
                    # Lectura puramente desde el Transaction Log de Delta (Zero-Compute data scan)
                    detail = self.spark.sql(f"DESCRIBE DETAIL {meta.silver_table}").collect()[0]
                    logger.info(f"[{domain_key}] Tamaño actual: {detail['sizeInBytes'] / (1024**3):.2f} GB")
                except Exception:
                    pass
            return stats_df

        except Exception as e:
            logger.error(f"Error analizando Storage: {e}")
            return None

    def run_batch_lifecycle_maintenance(
        self,
        domains: Optional[List[str]] = None,
        dry_run: bool = False
    ) -> Dict[str, Tuple[bool, Dict]]:
        """
        Ejecutar mantenimiento de ciclo de vida para múltiples dominios.
        
        Secuencia:
        1. Si dominio tiene TTL: Ejecutar VACUUM con retención
        2. Registrar auditoría de cada operación
        3. Compilar reporte de resultados
        
        Args:
            domains: Lista de dominios (None = todos configurados)
            dry_run: Simular sin ejecutar
            
        Returns:
            Dict con resultados por dominio {domain: (éxito, stats)}
        """
        targets = domains or list(self.domains_config.keys())
        results = {}

        logger.info(
            f"Iniciando mantenimiento de ciclo de vida para: {targets}"
        )

        for domain in targets:
            success, stats = self.execute_vacuum(domain, dry_run=dry_run)
            results[domain] = (success, stats)

        logger.info("Reporte de mantenimiento:")
        for domain, (success, stats) in results.items():
            status = "✓" if success else "✗"
            logger.info(f"  {status} {domain}: {stats}")

        return results


def main(
    spark: SparkSession,
    control_plane_path: str = "abfss://control-plane@farmia.dfs.core.windows.net/ingestion/",
    dry_run: bool = False
) -> None:
    """
    Punto de entrada para job de ciclo de vida (Databricks Jobs / Airflow).
    
    Ejecución típica (nightly):
        spark-submit \\
            --conf spark.databricks.delta.vacuum.parallelDelete.enabled=true \\
            src/scripts/lifecycle_manager.py \\
            --control_plane_path abfss://... \\
            --dry_run false
    
    Args:
        spark: SparkSession
        control_plane_path: Ruta al control plane
        dry_run: True para simular sin ejecutar
    """
    logger.info("="*70)
    logger.info("LIFECYCLE MANAGER: Inicio")
    logger.info("="*70)

    try:
        manager = LifecycleManager(spark, control_plane_path)

        # Analizar impacto de almacenamiento PRE-maintenance
        logger.info("\n[1/3] Análisis de impacto en almacenamiento")
        analysis = manager.analyze_storage_impact()

        # Ejecutar VACUUM batch para todos los dominios
        logger.info("\n[2/3] Ejecutando VACUUM con políticas FinOps")
        results = manager.run_batch_lifecycle_maintenance(dry_run=dry_run)

        # Compilar reporte final
        logger.info("\n[3/3] Reporte final")
        success_count = sum(1 for _, (ok, _) in results.items() if ok)
        total_count = len(results)

        logger.info(
            f"Mantenimiento completado: {success_count}/{total_count} dominios exitosos"
        )
        logger.info("="*70)
        logger.info("LIFECYCLE MANAGER: Fin")
        logger.info("="*70)

    except Exception as e:
        logger.error(f"Error crítico en lifecycle manager: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    # Para ejecución local/testing
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("FarmIA-LifecycleManager") \
        .config("spark.databricks.delta.vacuum.parallelDelete.enabled", "true") \
        .getOrCreate()

    main(spark, dry_run=True)
