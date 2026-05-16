"""
Integración: Validación Pydantic + Motor Spark.

Ejemplo de flujo completo en FarmIA.
"""

import argparse
import yaml
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from src.validation.models import IngestionContract
from src.engine.processors import UnifiedMemoryCoreProcessor


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_config_from_yaml(yaml_path: str) -> IngestionContract:
    """Cargar y validar configuración YAML."""
    logger.info(f"Cargando config desde {yaml_path}")
    
    with open(yaml_path) as f:
        yaml_dict = yaml.safe_load(f)
    
    try:
        config = IngestionContract(**yaml_dict)
        logger.info(f"Config validada para dominio: {config.pipeline_info.domain}")
        return config
    except Exception as e:
        logger.error(f"Validación de config falló: {e}")
        raise


def setup_spark_session(app_name: str = "FarmIA-Ingestion") -> SparkSession:
    """Crear SparkSession con configuraciones apropiadas."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.streaming.schemaInference", "false") \
        .getOrCreate()
    
    return spark


def create_mock_schema_for_weather() -> StructType:
    """Esquema de ejemplo para weather domain (mock)."""
    return StructType([
        StructField("city", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("forecast_date", TimestampType(), True),
    ])


def initialize_tables_if_not_exist(spark: SparkSession, config: IngestionContract) -> None:
    """Crear tablas desde configuración YAML sin hardcodear nada."""
    from pyspark.sql.types import StructType, StructField
    from pyspark.sql.types import (
        StringType, IntegerType, LongType, DoubleType, BooleanType, 
        TimestampType, DateType, BinaryType
    )
    
    type_map = {
        "STRING": StringType(),
        "INT": IntegerType(),
        "BIGINT": LongType(),
        "DOUBLE": DoubleType(),
        "BOOLEAN": BooleanType(),
        "TIMESTAMP": TimestampType(),
        "DATE": DateType(),
        "BINARY": BinaryType(),
    }
    
    for table_def in config.tables_config.tables:
        if spark.catalog.tableExists(table_def.name):
            logger.info(f"Tabla ya existe: {table_def.name}")
            continue
        
        fields = [
            StructField(col.name, type_map.get(col.type, StringType()), col.nullable)
            for col in table_def.columns
        ]
        schema = StructType(fields)
        
        df = spark.createDataFrame([], schema)
        writer = df.write.format("delta").mode("overwrite")
        
        if table_def.partition_by:
            writer = writer.partitionBy(*table_def.partition_by)
        
        writer.saveAsTable(table_def.name)
        logger.info(f"Tabla creada: {table_def.name}")



def main(contract_path: str, checkpoint_location: str):
    """Orquestar ingesta con Pydantic + Spark."""
    logger.info("="*70)
    logger.info("PROCESADOR UNIFIED MEMORY CORE")
    logger.info("="*70)
    
    try:
        config = load_config_from_yaml(contract_path)
        logger.info(f"Pipeline: {config.pipeline_info.domain}/{config.pipeline_info.subdomain}")
        
        spark = setup_spark_session(f"FarmIA-{config.pipeline_info.domain}")
        logger.info("SparkSession creada")
        
        initialize_tables_if_not_exist(spark, config)
        logger.info("Tablas inicializadas")
        
        processor = UnifiedMemoryCoreProcessor(spark, config)
        logger.info("Procesador inicializado")
        
        logger.info("Procesando datos simulados...")
        schema = create_mock_schema_for_weather()
        mock_data = [
            ("Madrid", 25.5, 65.0, "2026-05-13 10:00:00"),
            ("Barcelona", 22.1, 70.0, "2026-05-13 10:00:00"),
        ]
        df = spark.createDataFrame(mock_data, schema=schema)
        processor.process_batch(df, batch_id=1)
        
        logger.info("="*70)
        logger.info("PROCESAMIENTO COMPLETO")
        logger.info("="*70)
        
    except Exception as e:
        logger.error(f"Error en pipeline: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FarmIA Unified Memory Core Processor")
    parser.add_argument(
        "--contract_path",
        type=str,
        default="specs/control_plane/weather_domain.yaml",
        help="Ruta al YAML de configuración"
    )
    parser.add_argument(
        "--checkpoint_location",
        type=str,
        default="abfss://checkpoints@farmia.dfs.core.windows.net/weather",
        help="Ubicación de checkpoints"
    )
    
    args = parser.parse_args()
    main(args.contract_path, args.checkpoint_location)
