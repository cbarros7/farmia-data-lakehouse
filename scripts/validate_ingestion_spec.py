#!/usr/bin/env python3
"""Validación fail-fast para el YAML del plano de control de ingesta."""
from __future__ import annotations

import re
import sys
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


class SourceFormat(str, Enum):
    avro = "avro"
    parquet = "parquet"
    json = "json"


class WriteStrategy(str, Enum):
    merge_into = "merge_into"
    append = "append"


class StorageLevel(str, Enum):
    MEMORY_AND_DISK = "MEMORY_AND_DISK"


class SchemaEvolution(str, Enum):
    none = "none"


class Quantum(BaseModel):
    id: str = Field(..., min_length=1)
    source_format: SourceFormat
    write_strategy: WriteStrategy
    retention_ttl_days: Optional[int] = None

    @model_validator(mode="after")
    def ttl_matches_add(self) -> "Quantum":
        high_volume = self.id in {"iot_sensors", "weather_external"}
        if high_volume:
            if self.retention_ttl_days != 14:
                raise ValueError("El TTL granular de IoT y weather debe ser de 14 días.")
        else:
            if self.retention_ttl_days is not None:
                raise ValueError("retention_ttl_days aplica únicamente a los quanta de IoT y weather.")
        if self.id in {"sales_online", "inventory_erp"} and self.write_strategy != WriteStrategy.merge_into:
            raise ValueError("sales/inventory requieren MERGE (merge_into).")
        if self.id in {"iot_sensors", "weather_external"} and self.write_strategy != WriteStrategy.append:
            raise ValueError("IoT/weather requieren APPEND ligero.")
        return self


class Runtime(BaseModel):
    spark_storage_level: StorageLevel
    schema_evolution_mode: SchemaEvolution
    event_hubs_network_mbps_budget: int = Field(..., ge=1, le=100)
    watermark_late_data_minutes: int = Field(default=30, ge=1, le=120)
    dlq_degradation_threshold_percent: int = Field(default=5, ge=1, le=50)
    watchdog_processing_alert_minutes: int = Field(default=45, ge=1, le=240)
    alert_masking_hours: int = Field(default=4, ge=1, le=24)


class ControlPlane(BaseModel):
    yaml_storage: str
    incremental_loader: str

    @field_validator("yaml_storage")
    @classmethod
    def adls(cls, v: str) -> str:
        if v.lower() != "adls_gen2":
            raise ValueError("El YAML del plano de control debe apuntar a ADLS Gen2.")
        return v

    @field_validator("incremental_loader")
    @classmethod
    def auto_loader(cls, v: str) -> str:
        if "auto_loader" not in v.lower():
            raise ValueError("La ingesta incremental debe usar Databricks Auto Loader.")
        return v


class IngestionContract(BaseModel):
    schema_version: str
    architectural_quantums: List[Quantum]
    runtime: Runtime
    control_plane: ControlPlane

    @field_validator("architectural_quantums")
    @classmethod
    def four_domains(cls, v: List[Quantum]) -> List[Quantum]:
        expected = {"sales_online", "inventory_erp", "iot_sensors", "weather_external"}
        got = {q.id for q in v}
        if got != expected:
            raise ValueError(f"los quanta deben coincidir con {expected}, se obtuvo {got}")
        return v


class SchemaField(BaseModel):
    name: str = Field(..., min_length=1)
    type: str = Field(..., min_length=1)
    nullable: Optional[bool] = True


class MetadataInjection(BaseModel):
    name: str
    expression: str
    datatype: str


class Transformations(BaseModel):
    pattern: str
    metadata_injection: List[MetadataInjection]


class SinkConfig(BaseModel):
    format: str
    mode: str = Field(..., min_length=1)
    table_name: str = Field(..., min_length=1)
    merge_keys: Optional[List[str]] = None
    partition_by: Optional[List[str]] = None

    @model_validator(mode="after")
    def validate_merge_keys_required(self) -> "SinkConfig":
        """Asegurar que merge_keys esté presente cuando el modo sea merge_into."""
        if self.mode == "merge_into":
            if not self.merge_keys or len(self.merge_keys) == 0:
                raise ValueError(
                    f"Tabla '{self.table_name}': mode='merge_into' requiere una lista merge_keys no vacía"
                )
        if self.mode == "append" and self.merge_keys and len(self.merge_keys) > 0:
            raise ValueError(
                f"Tabla '{self.table_name}': mode='append' no debe tener merge_keys definidos"
            )
        return self


class SchemaValidation(BaseModel):
    schema_evolution_mode: str
    rescued_data_column: str
    fields: List[SchemaField] = Field(..., min_items=1)


class DomainContract(BaseModel):
    """Contrato YAML de dominio individual (ej. sales_domain.yaml, iot_domain.yaml)."""
    pipeline_info: Dict[str, Any]
    checkpoint_location: str
    source: Dict[str, Any]
    schema_validation: SchemaValidation
    transformations: Transformations
    sink: SinkConfig
    dlq: Dict[str, Any]

    @field_validator("checkpoint_location", "sink")
    @classmethod
    def validate_adls_paths(cls, v: Any) -> Any:
        """Validar que las rutas sigan el formato de ADLS Gen2."""
        if isinstance(v, str):
            if not re.match(r"^abfss://[a-z0-9\-]+@[a-z0-9\-]+\.dfs\.core\.windows\.net", v):
                raise ValueError(f"Formato de ruta ADLS no válido: {v}")
        return v

    @model_validator(mode="after")
    def validate_domain_idempotence(self) -> "DomainContract":
        """Aplicar reglas de idempotencia por dominio."""
        domain_name = self.pipeline_info.get("domain", "unknown")

        # Sales e Inventory DEBEN usar merge_into
        if domain_name in {"sales_online", "inventory_erp"}:
            if self.sink.mode != "merge_into":
                raise ValueError(
                    f"El dominio '{domain_name}' requiere sink.mode='merge_into' para idempotencia, "
                    f"se obtuvo mode='{self.sink.mode}'"
                )

        # IoT y Weather DEBEN usar append
        if domain_name in {"iot_sensors", "weather_external"}:
            if self.sink.mode != "append":
                raise ValueError(
                    f"El dominio '{domain_name}' requiere sink.mode='append' para ingesta ligera, "
                    f"se obtuvo mode='{self.sink.mode}'"
                )

        return self


def validate_all_domain_contracts(root: Path) -> List[str]:
    """Validar todos los archivos *_domain.yaml en specs/control_plane/."""
    errors = []
    control_plane_dir = root / "specs" / "control_plane"

    if not control_plane_dir.exists():
        errors.append(f"Directorio del plano de control no encontrado: {control_plane_dir}")
        return errors

    domain_files = sorted(control_plane_dir.glob("*_domain.yaml"))

    if not domain_files:
        errors.append(f"No se encontraron archivos YAML de dominio en {control_plane_dir}")
        return errors

    for domain_file in domain_files:
        try:
            raw = yaml.safe_load(domain_file.read_text(encoding="utf-8"))
            if raw is None:
                errors.append(f"{domain_file.name}: YAML vacío o no válido")
                continue

            DomainContract.model_validate(raw)
        except Exception as e:
            errors.append(f"{domain_file.name}: {str(e)}")

    return errors


def main() -> int:
    try:
        _script_path = Path(__file__).resolve()
    except NameError:
        import inspect
        _script_path = Path(inspect.currentframe().f_code.co_filename).resolve()

    root = _script_path.parents[1]

    # 1. Validar ingestion_contract.yaml
    spec_path = root / "specs" / "control_plane" / "ingestion_contract.yaml"
    if not spec_path.is_file() or spec_path.stat().st_size == 0:
        logger.error(f"Especificación faltante o vacía en {spec_path}")
        return 2

    try:
        raw = yaml.safe_load(spec_path.read_text(encoding="utf-8"))
        IngestionContract.model_validate(raw)
        logger.info(f"Validado {spec_path.relative_to(root)}")
    except Exception as e:
        logger.error(f"{spec_path.relative_to(root)}: {str(e)}")
        return 2

    # 2. Validar todos los archivos *_domain.yaml
    domain_errors = validate_all_domain_contracts(root)
    if domain_errors:
        for error in domain_errors:
            logger.error(error)
        return 2

    domain_files_count = len(list((root / "specs" / "control_plane").glob("*_domain.yaml")))
    logger.info(f"Validados {domain_files_count} contrato(s) de dominio")

    logger.info("Todos los contratos fueron validados exitosamente")
    return 0


if __name__ == "__main__":
    main()
