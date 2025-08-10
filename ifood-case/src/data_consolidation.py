"""
Módulo para consolidação de dados no Delta Lake usando PySpark
Implementa as camadas Raw, Bronze, Silver e Gold
"""

from typing import List, Dict, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    lit, col, year, month, hour, dayofweek, when, 
    isnan, isnull, regexp_replace, trim, upper,
    avg, count, sum as spark_sum, min as spark_min, max as spark_max
)
from pyspark.sql.types import *

from config import (
    CATALOG_NAME, SCHEMA_NAME, DBFS_RAW_DIR, 
    REQUIRED_COLUMNS, COLUMN_MAPPING, PARTITION_COLUMNS,
    get_table_name
)


class DataLakeConsolidator:
    """
    Classe para consolidação de dados no Data Lake
    Implementa arquitetura em camadas: Raw → Bronze → Silver → Gold
    """
    
    def __init__(self, spark: SparkSession):
        """
        Inicializa o consolidador
        
        Args:
            spark: Sessão do Spark
        """
        self.spark = spark
        self.setup_environment()
        
        # Contadores para monitoramento
        self.processed_files = 0
        self.total_records = 0
        self.processing_errors = []
    
    def setup_environment(self):
        """Configura ambiente Spark e catálogo"""
        try:
            print("🔧 Configurando ambiente Delta Lake...")
            
            # Configurações essenciais
            essential_configs = {
                "spark.databricks.delta.schema.autoMerge.enabled": "true"
            }
            
            for config, value in essential_configs.items():
                try:
                    self.spark.conf.set(config, value)
                    print(f"✅ {config} aplicada")
                except Exception as e:
                    print(f"⚠️  {config} não aplicada: {e}")
            
            # Cria catálogo e schema
            self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")
            self.spark.sql(f"USE CATALOG {CATALOG_NAME}")
            self.spark.sql(f"USE {SCHEMA_NAME}")
            
            print(f"✅ Ambiente configurado: {CATALOG_NAME}.{SCHEMA_NAME}")
            
        except Exception as e:
            print(f"⚠️  Problema na configuração: {e}")
    
    def create_raw_layer(self, extracted_files: List[Dict]) -> bool:
        """
        Cria camada Raw - dados brutos sem transformação
        
        Args:
            extracted_files: Lista de arquivos extraídos
            
        Returns:
            True se criação foi bem-sucedida
        """
        try:
            print("\n📥 CRIANDO CAMADA RAW")
            print("="*40)
            
            if not extracted_files:
                print("❌ Nenhum arquivo para processar")
                return False
            
            all_dataframes = []
            
            for file_info in extracted_files:
                try:
                    taxi_type = file_info['taxi_type']
                    year_month = file_info['year_month']
                    dbfs_path = file_info['dbfs_path']
                    
                    print(f"  📂 Processando {taxi_type} {year_month}")
                    
                    # Lê arquivo Parquet
                    df = self.spark.read.parquet(dbfs_path)
                    
                    # Adiciona metadados de origem
                    df_with_metadata = df.withColumn("source_file", lit(file_info['filename'])) \
                                        .withColumn("taxi_type", lit(taxi_type)) \
                                        .withColumn("year_month", lit(year_month)) \
                                        .withColumn("ingestion_timestamp", lit(self.spark.sql("SELECT current_timestamp()").collect()[0][0]))
                    
                    record_count = df_with_metadata.count()
                    print(f"    ✅ {record_count:,} registros")
                    
                    all_dataframes.append(df_with_metadata)
                    self.processed_files += 1
                    self.total_records += record_count
                    
                except Exception as e:
                    error_msg = f"Erro ao processar {file_info['filename']}: {e}"
                    print(f"    ❌ {error_msg}")
                    self.processing_errors.append(error_msg)
            
            if not all_dataframes:
                print("❌ Nenhum DataFrame foi criado")
                return False
            
            # Combina todos os DataFrames
            print(f"\n🔄 Combinando {len(all_dataframes)} DataFrames...")
            combined_df = all_dataframes[0]
            for df in all_dataframes[1:]:
                combined_df = combined_df.union(df)
            
            # Salva camada Raw
            raw_table = get_table_name("raw")
            print(f"💾 Salvando camada Raw: {raw_table}")
            
            combined_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable(raw_table)
            
            print(f"✅ Camada Raw criada: {self.total_records:,} registros")
            return True
            
        except Exception as e:
            print(f"❌ Erro na criação da camada Raw: {e}")
            return False
    
    def create_bronze_layer(self) -> bool:
        """
        Cria camada Bronze - dados padronizados e limpos
        
        Returns:
            True se criação foi bem-sucedida
        """
        try:
            print("\n🥉 CRIANDO CAMADA BRONZE")
            print("="*40)
            
            raw_table = get_table_name("raw")
            bronze_table = get_table_name("bronze")
            
            # Lê dados da camada Raw
            raw_df = self.spark.table(raw_table)
            print(f"📂 Carregando dados Raw: {raw_df.count():,} registros")
            
            # Padronização de colunas
            bronze_df = self.standardize_schema(raw_df)
            
            # Limpeza básica
            bronze_df = self.basic_data_cleaning(bronze_df)
            
            # Adiciona colunas derivadas
            bronze_df = self.add_derived_columns(bronze_df)
            
            # Salva camada Bronze
            print(f"💾 Salvando camada Bronze: {bronze_table}")
            
            bronze_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .partitionBy("taxi_type", "year", "month") \
                .saveAsTable(bronze_table)
            
            bronze_count = bronze_df.count()
            print(f"✅ Camada Bronze criada: {bronze_count:,} registros")
            return True
            
        except Exception as e:
            print(f"❌ Erro na criação da camada Bronze: {e}")
            return False
    
    def standardize_schema(self, df: DataFrame) -> DataFrame:
        """
        Padroniza schema entre diferentes tipos de táxi
        
        Args:
            df: DataFrame original
            
        Returns:
            DataFrame com schema padronizado
        """
        print("  🔧 Padronizando schema...")
        
        # Aplica mapeamento de colunas para green taxis
        for taxi_type, mapping in COLUMN_MAPPING.items():
            if taxi_type in df.select("taxi_type").distinct().rdd.map(lambda r: r[0]).collect():
                for old_col, new_col in mapping.items():
                    if old_col in df.columns:
                        df = df.withColumnRenamed(old_col, new_col)
        
        # Garante que colunas obrigatórias existem
        for col_name in REQUIRED_COLUMNS:
            if col_name not in df.columns:
                df = df.withColumn(col_name, lit(None))
        
        # Seleciona colunas necessárias
        essential_cols = REQUIRED_COLUMNS + [
            "trip_distance", "fare_amount", "tip_amount", "tolls_amount",
            "taxi_type", "year_month", "source_file", "ingestion_timestamp"
        ]
        
        available_cols = [c for c in essential_cols if c in df.columns]
        return df.select(*available_cols)
    
    def basic_data_cleaning(self, df: DataFrame) -> DataFrame:
        """
        Limpeza básica dos dados
        
        Args:
            df: DataFrame original
            
        Returns:
            DataFrame limpo
        """
        print("  🧹 Aplicando limpeza básica...")
        
        # Remove registros com valores críticos nulos
        df_clean = df.filter(
            col("tpep_pickup_datetime").isNotNull() &
            col("tpep_dropoff_datetime").isNotNull() &
            col("total_amount").isNotNull()
        )
        
        # Remove valores negativos em campos monetários
        df_clean = df_clean.filter(col("total_amount") >= 0)
        
        # Remove viagens com duração negativa
        df_clean = df_clean.filter(
            col("tpep_pickup_datetime") <= col("tpep_dropoff_datetime")
        )
        
        # Limita passenger_count a valores razoáveis
        df_clean = df_clean.filter(
            (col("passenger_count").isNull()) | 
            ((col("passenger_count") >= 0) & (col("passenger_count") <= 10))
        )
        
        return df_clean
    
    def add_derived_columns(self, df: DataFrame) -> DataFrame:
        """
        Adiciona colunas derivadas para análise
        
        Args:
            df: DataFrame original
            
        Returns:
            DataFrame com colunas derivadas
        """
        print("  ➕ Adicionando colunas derivadas...")
        
        # Extrai componentes de data/hora
        df_enhanced = df.withColumn("pickup_year", year(col("tpep_pickup_datetime"))) \
                       .withColumn("pickup_month", month(col("tpep_pickup_datetime"))) \
                       .withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
                       .withColumn("pickup_dayofweek", dayofweek(col("tpep_pickup_datetime")))
        
        # Adiciona nome do dia da semana
        df_enhanced = df_enhanced.withColumn("pickup_dayname",
            when(col("pickup_dayofweek") == 1, "Domingo")
            .when(col("pickup_dayofweek") == 2, "Segunda")
            .when(col("pickup_dayofweek") == 3, "Terça")
            .when(col("pickup_dayofweek") == 4, "Quarta")
            .when(col("pickup_dayofweek") == 5, "Quinta")
            .when(col("pickup_dayofweek") == 6, "Sexta")
            .when(col("pickup_dayofweek") == 7, "Sábado")
        )
        
        # Adiciona período do dia
        df_enhanced = df_enhanced.withColumn("pickup_period",
            when(col("pickup_hour").between(6, 11), "Manhã")
            .when(col("pickup_hour").between(12, 17), "Tarde")
            .when(col("pickup_hour").between(18, 23), "Noite")
            .otherwise("Madrugada")
        )
        
        # Calcula duração da viagem em minutos
        df_enhanced = df_enhanced.withColumn("trip_duration_minutes",
            (col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")) / 60
        )
        
        return df_enhanced
    
    def create_silver_layer(self) -> bool:
        """
        Cria camada Silver - dados enriquecidos e validados
        
        Returns:
            True se criação foi bem-sucedida
        """
        try:
            print("\n🥈 CRIANDO CAMADA SILVER")
            print("="*40)
            
            bronze_table = get_table_name("bronze")
            silver_table = get_table_name("silver")
            
            # Lê dados da camada Bronze
            bronze_df = self.spark.table(bronze_table)
            print(f"📂 Carregando dados Bronze: {bronze_df.count():,} registros")
            
            # Aplicar regras de qualidade mais rigorosas
            silver_df = self.apply_quality_rules(bronze_df)
            
            # Enriquecimento adicional
            silver_df = self.enrich_data(silver_df)
            
            # Salva camada Silver
            print(f"💾 Salvando camada Silver: {silver_table}")
            
            silver_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .partitionBy("taxi_type", "pickup_year", "pickup_month") \
                .saveAsTable(silver_table)
            
            silver_count = silver_df.count()
            print(f"✅ Camada Silver criada: {silver_count:,} registros")
            return True
            
        except Exception as e:
            print(f"❌ Erro na criação da camada Silver: {e}")
            return False
    
    def apply_quality_rules(self, df: DataFrame) -> DataFrame:
        """
        Aplica regras de qualidade rigorosas
        
        Args:
            df: DataFrame original
            
        Returns:
            DataFrame com qualidade validada
        """
        print("  ✅ Aplicando regras de qualidade...")
        
        # Remove outliers extremos
        df_quality = df.filter(
            (col("total_amount") <= 1000) &  # Tarifas muito altas
            (col("trip_duration_minutes").between(1, 1440)) &  # 1 min a 24h
            ((col("trip_distance").isNull()) | (col("trip_distance") <= 100))  # Distâncias muito altas
        )
        
        # Padroniza passenger_count nulo para 1
        df_quality = df_quality.withColumn("passenger_count",
            when(col("passenger_count").isNull() | (col("passenger_count") == 0), 1)
            .otherwise(col("passenger_count"))
        )
        
        return df_quality
    
    def enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enriquece dados com informações adicionais
        
        Args:
            df: DataFrame original
            
        Returns:
            DataFrame enriquecido
        """
        print("  🌟 Enriquecendo dados...")
        
        # Adiciona flag de fim de semana
        df_enriched = df.withColumn("is_weekend",
            col("pickup_dayofweek").isin([1, 7])  # Domingo e Sábado
        )
        
        # Adiciona categoria de tarifa
        df_enriched = df_enriched.withColumn("fare_category",
            when(col("total_amount") <= 10, "Baixa")
            .when(col("total_amount") <= 25, "Média")
            .when(col("total_amount") <= 50, "Alta")
            .otherwise("Premium")
        )
        
        # Adiciona categoria de passageiros
        df_enriched = df_enriched.withColumn("passenger_category",
            when(col("passenger_count") == 1, "Individual")
            .when(col("passenger_count") == 2, "Casal")
            .when(col("passenger_count").between(3, 4), "Família")
            .otherwise("Grupo")
        )
        
        return df_enriched
    
    def create_gold_layer(self) -> bool:
        """
        Cria camada Gold - dados agregados para análises
        
        Returns:
            True se criação foi bem-sucedida
        """
        try:
            print("\n🥇 CRIANDO CAMADA GOLD")
            print("="*40)
            
            silver_table = get_table_name("silver")
            gold_table = get_table_name("gold")
            
            # Lê dados da camada Silver
            silver_df = self.spark.table(silver_table)
            print(f"📂 Carregando dados Silver: {silver_df.count():,} registros")
            
            # Cria agregações para análises
            gold_df = self.create_analytical_aggregations(silver_df)
            
            # Salva camada Gold
            print(f"💾 Salvando camada Gold: {gold_table}")
            
            gold_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable(gold_table)
            
            gold_count = gold_df.count()
            print(f"✅ Camada Gold criada: {gold_count:,} registros")
            return True
            
        except Exception as e:
            print(f"❌ Erro na criação da camada Gold: {e}")
            return False
    
    def create_analytical_aggregations(self, df: DataFrame) -> DataFrame:
        """
        Cria agregações para análises de negócio
        
        Args:
            df: DataFrame da camada Silver
            
        Returns:
            DataFrame com agregações
        """
        print("  📊 Criando agregações analíticas...")
        
        # Agregação por taxi_type, ano, mês e hora
        gold_df = df.groupBy(
            "taxi_type", "pickup_year", "pickup_month", "pickup_hour",
            "pickup_period", "pickup_dayname", "is_weekend"
        ).agg(
            count("*").alias("total_trips"),
            avg("total_amount").alias("avg_total_amount"),
            spark_sum("total_amount").alias("sum_total_amount"),
            avg("passenger_count").alias("avg_passenger_count"),
            spark_sum("passenger_count").alias("sum_passenger_count"),
            avg("trip_duration_minutes").alias("avg_trip_duration"),
            spark_min("total_amount").alias("min_total_amount"),
            spark_max("total_amount").alias("max_total_amount")
        )
        
        # Adiciona year_month para compatibilidade
        gold_df = gold_df.withColumn("year_month",
            when(col("pickup_month") < 10, 
                 concat(col("pickup_year"), lit("-0"), col("pickup_month")))
            .otherwise(concat(col("pickup_year"), lit("-"), col("pickup_month")))
        )
        
        return gold_df
    
    def optimize_tables(self):
        """Otimiza todas as tabelas Delta criadas"""
        try:
            print("\n⚡ OTIMIZANDO TABELAS DELTA")
            print("="*40)
            
            tables = ["raw", "bronze", "silver", "gold"]
            
            for layer in tables:
                try:
                    table_name = get_table_name(layer)
                    print(f"  🔧 Otimizando {table_name}...")
                    
                    # OPTIMIZE
                    self.spark.sql(f"OPTIMIZE {table_name}")
                    
                    # ANALYZE TABLE
                    self.spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
                    
                    print(f"    ✅ {layer} otimizada")
                    
                except Exception as e:
                    print(f"    ⚠️  Erro na otimização de {layer}: {e}")
            
            print("✅ Otimização concluída")
            
        except Exception as e:
            print(f"❌ Erro na otimização: {e}")
    
    def run_full_consolidation(self, extracted_files: List[Dict]) -> bool:
        """
        Executa consolidação completa: Raw → Bronze → Silver → Gold
        
        Args:
            extracted_files: Lista de arquivos extraídos
            
        Returns:
            True se consolidação foi bem-sucedida
        """
        print("🚀 INICIANDO CONSOLIDAÇÃO COMPLETA DO DATA LAKE")
        print("="*60)
        
        try:
            # Camada Raw
            if not self.create_raw_layer(extracted_files):
                return False
            
            # Camada Bronze
            if not self.create_bronze_layer():
                return False
            
            # Camada Silver
            if not self.create_silver_layer():
                return False
            
            # Camada Gold
            if not self.create_gold_layer():
                return False
            
            # Otimização
            self.optimize_tables()
            
            # Relatório final
            self.print_consolidation_report()
            
            return True
            
        except Exception as e:
            print(f"❌ Erro na consolidação: {e}")
            return False
    
    def print_consolidation_report(self):
        """Imprime relatório da consolidação"""
        print(f"\n📊 RELATÓRIO DE CONSOLIDAÇÃO")
        print("="*50)
        
        try:
            tables = ["raw", "bronze", "silver", "gold"]
            
            for layer in tables:
                table_name = get_table_name(layer)
                try:
                    count = self.spark.table(table_name).count()
                    print(f"   • {layer.upper()}: {count:,} registros")
                except:
                    print(f"   • {layer.upper()}: Erro ao contar registros")
            
            print(f"\n✅ Consolidação concluída com sucesso!")
            print(f"📊 Arquivos processados: {self.processed_files}")
            print(f"📈 Total de registros: {self.total_records:,}")
            
            if self.processing_errors:
                print(f"\n⚠️  Erros durante processamento:")
                for error in self.processing_errors:
                    print(f"   • {error}")
            
        except Exception as e:
            print(f"❌ Erro no relatório: {e}")


def main():
    """Função principal para execução standalone"""
    spark = SparkSession.builder.appName("Data_Lake_Consolidation").getOrCreate()
    
    consolidator = DataLakeConsolidator(spark)
    
    # Para teste, assume que arquivos já foram extraídos
    # Em uso real, receberia lista de extracted_files
    extracted_files = []  # Seria passado pelo módulo de extração
    
    success = consolidator.run_full_consolidation(extracted_files)
    
    return success


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
