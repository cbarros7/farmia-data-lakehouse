"""
Integración: Validación Pydantic + Motor Spark.

Ejemplo de flujo completo en FarmIA.
"""

import argparse
import datetime
import yaml
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from src.validation.models import IngestionContract
from src.engine.processors import UnifiedMemoryCoreProcessor
from src.engine.lakehouse_initializer import LakehouseInitializer


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



def main(contract_path: str):
    """
    Orquestar ingesta con Pydantic + Spark.
    
    Flujo:
    1. Cargar contrato YAML y validar (Pydantic fail-fast)
    2. Crear SparkSession
    3. Inicializar data lakehouse (self-healing DDL)
    4. Procesar datos 
    """
    logger.info("="*70)
    logger.info("PROCESADOR MOTOR INGESTA - FarmIA")
    logger.info("="*70)
    
    try:
        # Task 0: Validación de contrato (fail-fast, sin cluster)
        config = load_config_from_yaml(contract_path)
        logger.info(f"Contrato validado: {config.pipeline_info.domain}/{config.pipeline_info.subdomain}")
        
        # Crear SparkSession
        spark = setup_spark_session(f"FarmIA-{config.pipeline_info.domain}")
        logger.info("SparkSession creada")
        
        # Inicializar data lakehouse (self-healing)
        # Esto crea todos los esquemas y tablas leyendo DDL desde infra/databricks/queries/
        try:
            _script_path = Path(__file__).resolve()
        except NameError:
            import inspect
            _script_path = Path(inspect.currentframe().f_code.co_filename).resolve()

        PROJECT_ROOT = _script_path.parents[1]

        initializer = LakehouseInitializer(spark, project_root=PROJECT_ROOT)
        initializer.initialize_all()
        logger.info("Data Lakehouse inicializado desde DDL")
        
        # Task 1: Procesamiento de datos
        processor = UnifiedMemoryCoreProcessor(spark, config)
        logger.info("Procesador inicializado")
        
        # logger.info("\nProcesando datos simulados...")
        # # Mock schema para demostración (en producción viene de Auto Loader)
        # schema = StructType([
        #     StructField("city", StringType(), True),
        #     StructField("temperature", DoubleType(), True),
        #     StructField("humidity", DoubleType(), True),
        #     StructField("forecast_date", TimestampType(), True),
        # ])
        # mock_data = [
        #     ("Madrid", 25.5, 65.0, datetime.datetime.now()),
        #     ("Barcelona", 22.1, 70.0, datetime.datetime.now()),
        # ]
        # df = spark.createDataFrame(mock_data, schema=schema)
        # processor.process_batch(df, batch_id=1)
        
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
    
    args = parser.parse_args()
    main(args.contract_path)
