"""
Inicializador Idempotente del Data Lakehouse — FarmIA

Responsabilidad: Orquestar la ejecución de DDL desde archivos SQL.
"""

import logging
import yaml
from pathlib import Path
from typing import Dict, List, Optional
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class LakehouseInitializer:
    """Orquestador de inicialización DDL desde archivos SQL."""
    
    # Ruta base de DDL (relativa a raíz del proyecto)
    QUERIES_PATH = Path("infra/databricks/queries")
    SCHEMAS_PATH = QUERIES_PATH / "schemas"
    TABLES_PATH = QUERIES_PATH / "tables"
    
    def __init__(self, spark: SparkSession, project_root: Optional[Path] = None):
        self.spark = spark
        self.project_root = project_root or Path.cwd()
        self.schemas_path = self.project_root / self.SCHEMAS_PATH
        self.tables_path = self.project_root / self.TABLES_PATH
    
    def initialize_all(self) -> None:
        """Punto de entrada único: schemas DDL, luego tablas DDL desde YAML."""
        try:
            logger.info("[START] Inicialización idempotente de Data Lakehouse")
            self._execute_schema_queries()
            self._execute_table_queries_from_yaml()
            logger.info("[SUCCESS] Data Lakehouse inicializado exitosamente")
        except Exception as e:
            logger.error(f"Error durante inicialización: {e}", exc_info=True)
            raise
    
    def _execute_schema_queries(self) -> None:
        """Ejecutar archivos SQL de esquemas."""
        logger.info("\nCreando esquemas...")
        if not self.schemas_path.exists():
            logger.error(f"Ruta de esquemas no existe: {self.schemas_path}")
            return
        for sql_file in sorted(self.schemas_path.glob("*.sql")):
            try:
                sql_content = sql_file.read_text()
                for statement in sql_content.split(";"):
                    statement = statement.strip()
                    if statement:
                        self.spark.sql(statement)
                logger.info(f"{sql_file.name}")
            except Exception as e:
                logger.error(f"{sql_file.name}: {e}")
                raise
    
    def _execute_table_queries_from_yaml(self) -> None:
        """Leer contratos YAML y ejecutar DDL de tablas correspondientes."""
        logger.info("\nCreando tablas (desde YAML)...")
        control_plane_path = self.project_root / "specs" / "control_plane"
        if not control_plane_path.exists():
            logger.error(f"Control plane no existe: {control_plane_path}")
            return
        for yaml_file in sorted(control_plane_path.glob("*_domain.yaml")):
            try:
                self._process_contract_yaml(yaml_file)
            except Exception as e:
                logger.error(f"{yaml_file.name}: {e}")
                raise
    
    def _process_contract_yaml(self, yaml_file: Path) -> None:
        """Procesar un contrato YAML y ejecutar DDL de tablas."""
        with open(yaml_file) as f:
            contract = yaml.safe_load(f)
        domain = contract.get("pipeline_info", {}).get("domain", "")
        if not domain:
            logger.warning(f"{yaml_file.name}: No tiene 'domain' definido")
            return
        sink = contract.get("sink", {})
        table_name = sink.get("table_name", "") or sink.get("table", "")
        if not table_name:
            logger.warning(f"{yaml_file.name}: No tiene 'sink.table_name' definido")
            return
        schema_table = table_name.replace(".", "_")
        sql_file = self.tables_path / f"{schema_table}.sql"
        if not sql_file.exists():
            logger.warning(f"SQL no encontrado: {sql_file}")
            return
        sql_content = sql_file.read_text()
        for statement in sql_content.split(";"):
            statement = statement.strip()
            if statement:
                self.spark.sql(statement)
        logger.info(f"{table_name} (desde {schema_table}.sql)")
