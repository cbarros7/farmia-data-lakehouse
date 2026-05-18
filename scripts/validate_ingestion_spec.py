#!/usr/bin/env python3
"""Fail-fast validation for ingestion control-plane YAML."""
from __future__ import annotations

import sys
from enum import Enum
from pathlib import Path
from typing import List, Optional

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
                raise ValueError("IoT and weather granular TTL must be 14 days.")
        else:
            if self.retention_ttl_days is not None:
                raise ValueError("retention_ttl_days applies only to IoT and weather quanta.")
        if self.id in {"sales_online", "inventory_erp"} and self.write_strategy != WriteStrategy.merge_into:
            raise ValueError("sales/inventory require MERGE (merge_into).")
        if self.id in {"iot_sensors", "weather_external"} and self.write_strategy != WriteStrategy.append:
            raise ValueError("IoT/weather require lightweight APPEND.")
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
            raise ValueError("control-plane YAML must target ADLS Gen2.")
        return v

    @field_validator("incremental_loader")
    @classmethod
    def auto_loader(cls, v: str) -> str:
        if "auto_loader" not in v.lower():
            raise ValueError("incremental ingestion must use Databricks Auto Loader.")
        return v


class FinOpsGold(BaseModel):
    trino_sync: str
    preaggregation_layer: str

    @field_validator("trino_sync")
    @classmethod
    def d1(cls, v: str) -> str:
        if "d_minus_1" not in v.lower() and "d-1" not in v.lower():
            raise ValueError("Trino must follow D-1 background batch synchronization.")
        return v

    @field_validator("preaggregation_layer")
    @classmethod
    def silver(cls, v: str) -> str:
        if v.lower() != "silver":
            raise ValueError("heavy work must stay in Silver, not Gold/Trino.")
        return v


class IngestionContract(BaseModel):
    schema_version: str
    architectural_quantums: List[Quantum]
    runtime: Runtime
    control_plane: ControlPlane
    finops_gold: FinOpsGold

    @field_validator("architectural_quantums")
    @classmethod
    def four_domains(cls, v: List[Quantum]) -> List[Quantum]:
        expected = {"sales_online", "inventory_erp", "iot_sensors", "weather_external"}
        got = {q.id for q in v}
        if got != expected:
            raise ValueError(f"quanta must match {expected}, got {got}")
        return v


def main() -> int:
    try:
        _script_path = Path(__file__).resolve()
    except NameError:
        import inspect
        _script_path = Path(inspect.currentframe().f_code.co_filename).resolve()

    root = _script_path.parents[1]
    
    spec_path = root / "specs" / "control_plane" / "ingestion_contract.yaml"
    if not spec_path.is_file() or spec_path.stat().st_size == 0:
        print(f"FAIL: missing or empty spec at {spec_path}", file=sys.stderr)
        return 2
    raw = yaml.safe_load(spec_path.read_text(encoding="utf-8"))
    IngestionContract.model_validate(raw)
    print(f"OK: validated {spec_path.relative_to(root)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
