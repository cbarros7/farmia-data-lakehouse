"""
Motor de Ingesta FarmIA: Pydantic + Auto Loader + Spark Streaming
"""

import argparse
import yaml
import logging
from pathlib import Path
from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, DateType

from src.validation.models import IngestionContract
from src.engine.processors import UnifiedMemoryCoreProcessor
from src.engine.lakehouse_initializer import LakehouseInitializer


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()

# Blob storage
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME") 
SECRET_SCOPE_NAME = os.getenv("SECRET_SCOPE_NAME")
SECRET_KEY_NAME = os.getenv("SECRET_KEY_NAME")

# Databricks busca encriptadamente en Azure Key Vault en tiempo de ejecución
STORAGE_ACCOUNT_KEY = dbutils.secrets.get(scope=SECRET_SCOPE_NAME, key=SECRET_KEY_NAME)

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", 
    STORAGE_ACCOUNT_KEY
)

def load_config_from_yaml(yaml_path: str) -> IngestionContract:
    logger.info(f"Cargando {yaml_path}")
    with open(yaml_path) as f:
        yaml_dict = yaml.safe_load(f)
    config = IngestionContract(**yaml_dict)
    logger.info(f"✓ {config.pipeline_info.domain} v{config.pipeline_info.version}")
    return config


def build_schema_from_config(config: IngestionContract) -> StructType:
    """Construir StructType desde schema_validation.fields del YAML."""
    type_map = {
        "StringType": StringType(),
        "DoubleType": DoubleType(),
        "IntegerType": IntegerType(),
        "TimestampType": TimestampType(),
        "DateType": DateType(),
    }
    fields = [
        StructField(f.name, type_map[f.type], f.nullable)
        for f in config.schema_validation.fields
    ]
    return StructType(fields)


def build_auto_loader_stream(spark: SparkSession, config: IngestionContract) -> DataFrame:
    """Construir readStream con Auto Loader dinámico."""
    source = config.source
    options = {
        "cloudFiles.format": source.options.get("cloudFiles.format", source.format),
        "cloudFiles.schemaEvolutionMode": source.options.get("cloudFiles.schemaEvolutionMode", "none"),
        "cloudFiles.rescuedDataColumn": config.schema_validation.rescued_data_column,
        "cloudFiles.maxBytesPerTrigger": source.options.get("cloudFiles.maxBytesPerTrigger", "134217728"),
        "cloudFiles.inferColumnTypes": source.options.get("cloudFiles.inferColumnTypes", "false"),
        # "cloudFiles.validateOptions": "false"
    }
    for key, val in source.options.items():
        if key.startswith("cloudFiles."):
            options[key] = val
    
    schema = build_schema_from_config(config)
    
    df_stream = spark.readStream \
        .format("cloudFiles") \
        .options(**options) \
        .schema(schema) \
        .load(source.path)
    
    logger.info(f"Auto Loader: {source.path} ({source.format})")
    return df_stream


def setup_spark_session(app_name: str) -> SparkSession:
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.streaming.schemaInference", "false") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "auto") \
        .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
        .config("spark.databricks.delta.autoCompact.enabled", "true") \
        .getOrCreate()
    return spark


def main(contract_path: str):
    logger.info("=" * 70)
    logger.info("MOTOR INGESTA FARMIA - Auto Loader")
    logger.info("=" * 70)
    
    # Task 0: Validación + Inicialización
    config = load_config_from_yaml(contract_path)
    
    spark = setup_spark_session(f"FarmIA-{config.pipeline_info.domain}")
    logger.info("SparkSession iniciada")
    
    try:
        _script_path = Path(__file__).resolve()
    except NameError:
        import inspect
        _script_path = Path(inspect.currentframe().f_code.co_filename).resolve()
    
    PROJECT_ROOT = _script_path.parents[1]
    initializer = LakehouseInitializer(spark, project_root=PROJECT_ROOT)
    initializer.initialize_all()
    
    # Task 1: Streaming + Procesamiento
    processor = UnifiedMemoryCoreProcessor(spark, config)
    logger.info("Procesador inicializado")
    
    df_stream = build_auto_loader_stream(spark, config)
    
    logger.info(f"\n[INICIANDO STREAMING] {config.pipeline_info.domain}")
    logger.info(f"Checkpoint: {config.checkpoint_location}")
    logger.info(f"Sink: {config.sink.table_name} ({config.sink.mode.value})")
    logger.info("=" * 70)
    
    query = df_stream \
        .writeStream \
        .foreachBatch(processor.process_batch) \
        .option("checkpointLocation", config.checkpoint_location) \
        .start()
    
    query.awaitTermination()
    
    logger.info("Streaming finalizado")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FarmIA Motor de Ingesta")
    parser.add_argument(
        "--contract_path",
        type=str,
        default="specs/control_plane/weather_domain.yaml",
        help="YAML contract path"
    )
    args = parser.parse_args()
    main(args.contract_path)
