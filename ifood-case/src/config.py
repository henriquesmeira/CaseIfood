"""
Configurações centralizadas para o projeto iFood Case - Data Architect
"""

from typing import Dict, List

# Configurações do Data Lake
CATALOG_NAME = "main"
SCHEMA_NAME = "nyc_taxi"
TABLE_NAME = "trips_delta"

# Configurações de processamento
SPARK_CONFIGS = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true", 
    "spark.databricks.delta.schema.autoMerge.enabled": "true",
    "spark.sql.execution.arrow.pyspark.enabled": "true",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true"
}

# URLs dos dados de táxi NYC (Janeiro a Maio 2023)
DATA_SOURCES = {
    "yellow": {
        "2023-01": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
        "2023-02": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet", 
        "2023-03": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet",
        "2023-04": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-04.parquet",
        "2023-05": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-05.parquet"
    },
    "green": {
        "2023-01": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-01.parquet",
        "2023-02": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-02.parquet",
        "2023-03": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-03.parquet", 
        "2023-04": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-04.parquet",
        "2023-05": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-05.parquet"
    }
}

# Colunas obrigatórias conforme especificação do case
REQUIRED_COLUMNS = [
    "VendorID",
    "passenger_count", 
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

# Mapeamento de colunas entre yellow e green taxis
# Green taxis usam lpep_pickup_datetime e lpep_dropoff_datetime
COLUMN_MAPPING = {
    "green": {
        "lpep_pickup_datetime": "tpep_pickup_datetime",
        "lpep_dropoff_datetime": "tpep_dropoff_datetime"
    }
}

# Configurações de retry para downloads
RETRY_CONFIG = {
    "max_retries": 3,
    "initial_delay": 5,
    "backoff_multiplier": 2,
    "max_delay": 60
}

# Configurações de validação de dados
DATA_QUALITY_RULES = {
    "passenger_count": {"min": 0, "max": 10},
    "total_amount": {"min": 0, "max": 1000},
    "trip_distance": {"min": 0, "max": 100}
}

# Configurações de particionamento
PARTITION_COLUMNS = ["taxi_type", "year", "month"]

# Configurações de otimização
OPTIMIZATION_CONFIG = {
    "auto_optimize": True,
    "optimize_write": True,
    "auto_compact": True,
    "z_order_columns": ["tpep_pickup_datetime", "taxi_type"]
}

def get_full_table_name() -> str:
    """Retorna o nome completo da tabela no formato catalog.schema.table"""
    return f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}"

def get_required_columns_for_taxi_type(taxi_type: str) -> List[str]:
    """
    Retorna as colunas obrigatórias ajustadas para o tipo de táxi
    
    Args:
        taxi_type: 'yellow' ou 'green'
        
    Returns:
        Lista de colunas obrigatórias para o tipo específico
    """
    columns = REQUIRED_COLUMNS.copy()
    
    if taxi_type == "green":
        # Para green taxis, substitui as colunas de datetime
        mapping = COLUMN_MAPPING["green"]
        for i, col in enumerate(columns):
            if col in mapping.values():
                # Encontra a chave original
                original_col = next(k for k, v in mapping.items() if v == col)
                columns[i] = original_col
                
    return columns
