"""
Módulo principal para ingestão de dados de táxi NYC no Data Lake
Implementa download, processamento e criação de tabelas Delta Lake
"""

import time
from typing import Tuple, Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, col, year, month, hour, to_timestamp
from pyspark.sql.types import *

from config import (
    DATA_SOURCES, SPARK_CONFIGS, CATALOG_NAME, SCHEMA_NAME, TABLE_NAME,
    REQUIRED_COLUMNS, COLUMN_MAPPING, RETRY_CONFIG, PARTITION_COLUMNS,
    get_full_table_name, get_required_columns_for_taxi_type
)


class NYCTaxiDataIngestion:
    """
    Classe principal para ingestão de dados de táxi NYC
    """
    
    def __init__(self, spark: SparkSession):
        """
        Inicializa a classe de ingestão
        
        Args:
            spark: Sessão do Spark
        """
        self.spark = spark
        self.setup_spark_configs()
        self.setup_catalog_and_schema()
        
        # Contadores para monitoramento
        self.successful_loads = 0
        self.failed_loads = 0
        self.total_records = 0
        
    def setup_spark_configs(self):
        """Configura otimizações do Spark"""
        print("🔧 Configurando otimizações do Spark...")
        
        for key, value in SPARK_CONFIGS.items():
            try:
                self.spark.conf.set(key, value)
            except Exception as e:
                print(f"⚠️  Aviso: Não foi possível definir {key}: {e}")
                
    def setup_catalog_and_schema(self):
        """Cria catálogo e schema se não existirem"""
        try:
            print(f"📁 Configurando catálogo {CATALOG_NAME} e schema {SCHEMA_NAME}...")
            
            self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")
            self.spark.sql(f"USE CATALOG {CATALOG_NAME}")
            self.spark.sql(f"USE {SCHEMA_NAME}")
            
            print(f"✅ Catálogo e schema configurados com sucesso")
            
        except Exception as e:
            print(f"⚠️  Aviso: Problema na configuração do catálogo: {e}")
            print("📝 Continuando com configuração padrão...")
    
    def standardize_columns(self, df: DataFrame, taxi_type: str) -> DataFrame:
        """
        Padroniza as colunas entre diferentes tipos de táxi
        
        Args:
            df: DataFrame original
            taxi_type: Tipo do táxi ('yellow' ou 'green')
            
        Returns:
            DataFrame com colunas padronizadas
        """
        # Aplica mapeamento de colunas se necessário
        if taxi_type in COLUMN_MAPPING:
            mapping = COLUMN_MAPPING[taxi_type]
            for old_col, new_col in mapping.items():
                if old_col in df.columns:
                    df = df.withColumnRenamed(old_col, new_col)
        
        # Garante que as colunas obrigatórias existem
        for col_name in REQUIRED_COLUMNS:
            if col_name not in df.columns:
                print(f"⚠️  Coluna obrigatória {col_name} não encontrada em {taxi_type}")
                # Adiciona coluna nula se não existir
                df = df.withColumn(col_name, lit(None))
        
        # Seleciona apenas as colunas necessárias + algumas extras úteis
        available_cols = df.columns
        cols_to_select = []
        
        # Colunas obrigatórias
        for col_name in REQUIRED_COLUMNS:
            if col_name in available_cols:
                cols_to_select.append(col_name)
        
        # Colunas extras úteis para análise
        extra_cols = ["trip_distance", "fare_amount", "tip_amount", "tolls_amount"]
        for col_name in extra_cols:
            if col_name in available_cols:
                cols_to_select.append(col_name)
        
        return df.select(*cols_to_select)
    
    def add_metadata_columns(self, df: DataFrame, taxi_type: str, year_month: str) -> DataFrame:
        """
        Adiciona colunas de metadados para particionamento e análise
        
        Args:
            df: DataFrame original
            taxi_type: Tipo do táxi
            year_month: Ano-mês no formato YYYY-MM
            
        Returns:
            DataFrame com colunas de metadados
        """
        year_val, month_val = year_month.split("-")
        
        df_with_meta = df.withColumn("taxi_type", lit(taxi_type)) \
                        .withColumn("year", lit(int(year_val))) \
                        .withColumn("month", lit(int(month_val))) \
                        .withColumn("year_month", lit(year_month))
        
        # Adiciona coluna de hora do pickup se a coluna de datetime existir
        if "tpep_pickup_datetime" in df.columns:
            df_with_meta = df_with_meta.withColumn(
                "pickup_hour", 
                hour(col("tpep_pickup_datetime"))
            )
        
        return df_with_meta
    
    def load_data_with_retry(self, url: str, taxi_type: str, year_month: str) -> Tuple[Optional[DataFrame], int]:
        """
        Carrega dados de uma URL com retry automático
        
        Args:
            url: URL do arquivo Parquet
            taxi_type: Tipo do táxi
            year_month: Ano-mês
            
        Returns:
            Tupla (DataFrame, número de registros) ou (None, 0) em caso de falha
        """
        max_retries = RETRY_CONFIG["max_retries"]
        delay = RETRY_CONFIG["initial_delay"]
        
        for attempt in range(max_retries):
            try:
                print(f"  📥 Tentativa {attempt + 1}: Carregando {taxi_type} {year_month}")
                
                # Lê o arquivo Parquet
                df = self.spark.read.parquet(url)
                
                # Padroniza colunas
                df_standardized = self.standardize_columns(df, taxi_type)
                
                # Adiciona metadados
                df_final = self.add_metadata_columns(df_standardized, taxi_type, year_month)
                
                # Valida e conta registros
                count = df_final.count()
                
                if count == 0:
                    raise ValueError(f"Arquivo {year_month} está vazio")
                
                print(f"    ✅ {count:,} registros carregados com sucesso")
                return df_final, count
                
            except Exception as e:
                print(f"    ❌ Erro na tentativa {attempt + 1}: {str(e)[:100]}...")
                
                if attempt < max_retries - 1:
                    print(f"    ⏳ Aguardando {delay} segundos...")
                    time.sleep(delay)
                    delay = min(delay * RETRY_CONFIG["backoff_multiplier"], RETRY_CONFIG["max_delay"])
                else:
                    print(f"    💥 Falha definitiva para {taxi_type} {year_month}")
                    return None, 0
    
    def process_all_data(self) -> bool:
        """
        Processa todos os dados definidos em DATA_SOURCES
        
        Returns:
            True se pelo menos um arquivo foi processado com sucesso
        """
        print("🚀 Iniciando processamento completo dos dados...")
        
        all_dataframes = []
        
        for taxi_type, months in DATA_SOURCES.items():
            print(f"\n🚕 Processando táxis {taxi_type}:")
            
            for year_month, url in months.items():
                df, count = self.load_data_with_retry(url, taxi_type, year_month)
                
                if df is not None:
                    all_dataframes.append(df)
                    self.total_records += count
                    self.successful_loads += 1
                else:
                    self.failed_loads += 1
        
        if not all_dataframes:
            print("💥 Nenhum dado foi carregado com sucesso!")
            return False
        
        # Combina todos os DataFrames
        print(f"\n🔄 Combinando {len(all_dataframes)} DataFrames...")
        combined_df = all_dataframes[0]
        for df in all_dataframes[1:]:
            combined_df = combined_df.union(df)
        
        # Salva como tabela Delta
        return self.save_to_delta_table(combined_df)
    
    def save_to_delta_table(self, df: DataFrame) -> bool:
        """
        Salva DataFrame como tabela Delta Lake
        
        Args:
            df: DataFrame para salvar
            
        Returns:
            True se salvou com sucesso
        """
        try:
            table_name = get_full_table_name()
            print(f"💾 Salvando tabela Delta: {table_name}")
            
            # Escreve a tabela Delta com particionamento
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .option("optimizeWrite", "true") \
                .partitionBy(*PARTITION_COLUMNS) \
                .saveAsTable(table_name)
            
            print(f"✅ Tabela {table_name} criada com sucesso!")
            return True
            
        except Exception as e:
            print(f"❌ Erro ao salvar tabela: {e}")
            return False
    
    def validate_and_report(self):
        """Valida a tabela criada e gera relatório"""
        try:
            table_name = get_full_table_name()
            result_df = self.spark.table(table_name)
            final_count = result_df.count()
            
            print(f"\n📈 RESUMO FINAL:")
            print(f"   • Arquivos processados com sucesso: {self.successful_loads}")
            print(f"   • Arquivos com falha: {self.failed_loads}")
            print(f"   • Total de registros: {final_count:,}")
            print(f"   • Tabela: {table_name}")
            
            # Mostra distribuição por tipo
            print(f"\n📊 Distribuição por tipo de táxi:")
            result_df.groupBy("taxi_type").count().orderBy("taxi_type").show()
            
            # Mostra distribuição por mês
            print(f"\n📅 Distribuição por mês:")
            result_df.groupBy("year_month").count().orderBy("year_month").show()
            
            # Mostra schema
            print(f"\n📋 Schema da tabela:")
            result_df.printSchema()
            
            # Amostra dos dados
            print(f"\n🔍 Amostra dos dados (3 registros):")
            result_df.limit(3).show(truncate=False)
            
        except Exception as e:
            print(f"❌ Erro na validação: {e}")


def main():
    """Função principal para execução standalone"""
    # Inicializa Spark (para uso em ambiente Databricks)
    spark = SparkSession.builder.appName("NYC_Taxi_Data_Ingestion").getOrCreate()
    
    # Cria instância da classe de ingestão
    ingestion = NYCTaxiDataIngestion(spark)
    
    # Processa todos os dados
    success = ingestion.process_all_data()
    
    if success:
        # Valida e gera relatório
        ingestion.validate_and_report()
        print(f"\n🏁 Processamento concluído com sucesso!")
    else:
        print(f"\n💥 Processamento falhou!")
    
    return success


if __name__ == "__main__":
    main()
