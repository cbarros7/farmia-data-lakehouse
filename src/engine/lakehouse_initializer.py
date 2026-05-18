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
        """
        Inicializar con SparkSession.
        
        Args:
            spark: SparkSession activa
            project_root: Raíz del proyecto (si None, usa directorio actual)
        """
        self.spark = spark
        self.logger = logger
        self.project_root = project_root or Path.cwd()
        self.schemas_path = self.project_root / self.SCHEMAS_PATH
        self.tables_path = self.project_root / self.TABLES_PATH
    
    def initialize_all(self) -> None:
        """
        Punto de entrada único: ejecutar todo desde DDL.
        
        Secuencia:
        1. Crear esquemas desde archivos SQL
        2. Leer contratos YAML
        3. Crear tablas desde archivos SQL según YAML
        """
        try:
            self.logger.info("="*70)
            self.logger.info("INICIALIZACIÓN IDEMPOTENTE DE DATA LAKEHOUSE")
            self.logger.info("="*70)
            
            self._execute_schema_queries()
            self._execute_table_queries_from_yaml()
            
            self.logger.info("="*70)
            self.logger.info("Data Lakehouse inicializado exitosamente")
            self.logger.info("="*70)
            
        except Exception as e:
            self.logger.error(f"Error durante inicialización: {e}", exc_info=True)
            raise
    
    def _execute_schema_queries(self) -> None:
        """Ejecutar archivos SQL de esquemas."""
        self.logger.info("\nCreando esquemas...")
        
        if not self.schemas_path.exists():
            self.logger.error(f"Ruta de esquemas no existe: {self.schemas_path}")
            return
        
        schema_files = sorted(self.schemas_path.glob("*.sql"))
        for sql_file in schema_files:
            try:
                sql_content = sql_file.read_text()
                for statement in sql_content.split(";"):
                    statement = statement.strip()
                    if statement:
                        self.spark.sql(statement)
                # self.spark.sql(sql_content)
                self.logger.info(f"{sql_file.name}")
            except Exception as e:
                self.logger.error(f"{sql_file.name}: {e}")
                raise
    
    def _execute_table_queries_from_yaml(self) -> None:
        """Leer contratos YAML y ejecutar DDL de tablas correspondientes."""
        self.logger.info("\nCreando tablas (desde YAML)...")
        
        control_plane_path = self.project_root / "specs" / "control_plane"
        if not control_plane_path.exists():
            self.logger.error(f"Control plane no existe: {control_plane_path}")
            return
        
        # Leer todos los YAML (excepto ingestion_contract.yaml que es base)
        yaml_files = [f for f in control_plane_path.glob("*_domain.yaml") ]
        #yaml_files = [f for f in control_plane_path.glob("*.yaml") 
        #              if f.name != "ingestion_contract.yaml"]
        
        for yaml_file in sorted(yaml_files):
            try:
                self._process_contract_yaml(yaml_file)
            except Exception as e:
                self.logger.error(f"{yaml_file.name}: {e}")
                raise
    
    def _process_contract_yaml(self, yaml_file: Path) -> None:
        """Procesar un contrato YAML y ejecutar DDL de tablas."""
        with open(yaml_file) as f:
            contract = yaml.safe_load(f)
        
        # Extraer información del dominio
        pipeline_info = contract.get("pipeline_info", {})
        domain = pipeline_info.get("domain", "")
        
        if not domain:
            self.logger.warning(f"{yaml_file.name}: No tiene 'domain' definido")
            return
        
        # Extraer tabla del sink (ej: "silver.weather_external" -> buscar silver_weather_external.sql)
        sink = contract.get("sink", {})
        table_name = sink.get("table_name", "") or sink.get("table", "")
        
        if not table_name:
            self.logger.warning(f"{yaml_file.name}: No tiene 'sink.table_name' definido")
            return
        
        # Convertir tabla "schema.table" -> "schema_table.sql"
        schema_table = table_name.replace(".", "_")
        sql_file = f"{self.tables_path}/{schema_table}.sql"
        
        if not sql_file.exists():
            self.logger.warning(f"SQL no encontrado: {sql_file}")
            return
        
        # Ejecutar DDL
        sql_content = sql_file.read_text()
        for statement in sql_content.split(";"):
            statement = statement.strip()
            if statement:
                self.spark.sql(statement)
        # self.spark.sql(sql_content)
        self.logger.info(f"{table_name} (desde {schema_table}.sql)")
