# Databricks notebook source
# MAGIC %md
# MAGIC # Case Técnico iFood - Data Architect
# MAGIC ## Notebook 1: Ingestão de Dados
# MAGIC 
# MAGIC Este notebook implementa a ingestão completa dos dados de táxi NYC no Data Lake.
# MAGIC 
# MAGIC ### Objetivos:
# MAGIC - Baixar dados de táxi NYC (Janeiro a Maio 2023)
# MAGIC - Processar e padronizar os dados
# MAGIC - Criar tabela Delta Lake particionada
# MAGIC - Validar qualidade dos dados

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração Inicial

# COMMAND ----------

# Importações necessárias
from pyspark.sql.functions import lit, col, year, month, hour, when
from pyspark.sql.types import *
import time

# Configurações do Spark para otimização
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

print("✅ Configurações aplicadas com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Definição das Fontes de Dados

# COMMAND ----------

# URLs dos dados de táxi NYC (Janeiro a Maio 2023)
data_sources = {
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

# Configurações da tabela
catalog_name = "main"
schema_name = "nyc_taxi"
table_name = "trips_delta"

# Colunas obrigatórias conforme especificação
required_columns = [
    "VendorID",
    "passenger_count", 
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

print(f"📊 Total de arquivos para processar: {sum(len(months) for months in data_sources.values())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Configuração do Catálogo e Schema

# COMMAND ----------

# Cria catálogo e schema se não existirem
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
    spark.sql(f"USE CATALOG {catalog_name}")
    spark.sql(f"USE {schema_name}")
    
    print(f"✅ Catálogo {catalog_name} e schema {schema_name} configurados")
    
except Exception as e:
    print(f"⚠️ Aviso: {e}")
    print("📝 Continuando com configuração padrão...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Funções de Processamento

# COMMAND ----------

def standardize_columns(df, taxi_type):
    """
    Padroniza as colunas entre diferentes tipos de táxi
    """
    # Para green taxis, renomeia colunas de datetime
    if taxi_type == "green":
        if "lpep_pickup_datetime" in df.columns:
            df = df.withColumnRenamed("lpep_pickup_datetime", "tpep_pickup_datetime")
        if "lpep_dropoff_datetime" in df.columns:
            df = df.withColumnRenamed("lpep_dropoff_datetime", "tpep_dropoff_datetime")
    
    # Seleciona apenas colunas necessárias
    available_cols = df.columns
    cols_to_select = []
    
    # Colunas obrigatórias
    for col_name in required_columns:
        if col_name in available_cols:
            cols_to_select.append(col_name)
    
    # Colunas extras úteis
    extra_cols = ["trip_distance", "fare_amount", "tip_amount", "tolls_amount"]
    for col_name in extra_cols:
        if col_name in available_cols:
            cols_to_select.append(col_name)
    
    return df.select(*cols_to_select)

def add_metadata_columns(df, taxi_type, year_month):
    """
    Adiciona colunas de metadados
    """
    year_val, month_val = year_month.split("-")
    
    df_with_meta = df.withColumn("taxi_type", lit(taxi_type)) \
                    .withColumn("year", lit(int(year_val))) \
                    .withColumn("month", lit(int(month_val))) \
                    .withColumn("year_month", lit(year_month))
    
    # Adiciona hora do pickup
    if "tpep_pickup_datetime" in df.columns:
        df_with_meta = df_with_meta.withColumn(
            "pickup_hour", 
            hour(col("tpep_pickup_datetime"))
        )
    
    return df_with_meta

def load_data_with_retry(url, taxi_type, year_month, max_retries=3):
    """
    Carrega dados com retry automático
    """
    for attempt in range(max_retries):
        try:
            print(f"  📥 Tentativa {attempt + 1}: {taxi_type} {year_month}")
            
            # Lê o arquivo
            df = spark.read.parquet(url)
            
            # Padroniza colunas
            df_standardized = standardize_columns(df, taxi_type)
            
            # Adiciona metadados
            df_final = add_metadata_columns(df_standardized, taxi_type, year_month)
            
            # Valida
            count = df_final.count()
            if count == 0:
                raise ValueError("Arquivo vazio")
            
            print(f"    ✅ {count:,} registros carregados")
            return df_final, count
            
        except Exception as e:
            print(f"    ❌ Erro: {str(e)[:100]}...")
            if attempt < max_retries - 1:
                print(f"    ⏳ Aguardando 5 segundos...")
                time.sleep(5)
            else:
                print(f"    💥 Falha definitiva")
                return None, 0

print("✅ Funções de processamento definidas!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Processamento Principal

# COMMAND ----------

# Contadores
successful_loads = 0
failed_loads = 0
total_records = 0
all_dataframes = []

print("🚀 Iniciando processamento dos dados...")

# Processa cada tipo de táxi e mês
for taxi_type, months in data_sources.items():
    print(f"\n🚕 Processando táxis {taxi_type}:")
    
    for year_month, url in months.items():
        df, count = load_data_with_retry(url, taxi_type, year_month)
        
        if df is not None:
            all_dataframes.append(df)
            total_records += count
            successful_loads += 1
        else:
            failed_loads += 1

print(f"\n📊 Resumo do carregamento:")
print(f"   • Sucessos: {successful_loads}")
print(f"   • Falhas: {failed_loads}")
print(f"   • Total de registros: {total_records:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Criação da Tabela Delta

# COMMAND ----------

if all_dataframes:
    print(f"🔄 Combinando {len(all_dataframes)} DataFrames...")
    
    # Combina todos os DataFrames
    combined_df = all_dataframes[0]
    for df in all_dataframes[1:]:
        combined_df = combined_df.union(df)
    
    print(f"📊 DataFrame combinado: {combined_df.count():,} registros")
    
    # Salva como tabela Delta
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    print(f"💾 Salvando tabela: {full_table_name}")
    
    try:
        combined_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .option("optimizeWrite", "true") \
            .partitionBy("taxi_type", "year", "month") \
            .saveAsTable(full_table_name)
        
        print(f"✅ Tabela {full_table_name} criada com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro ao salvar: {e}")
        
else:
    print("💥 Nenhum dado foi carregado!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validação e Verificação

# COMMAND ----------

try:
    # Carrega a tabela criada
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    result_df = spark.table(full_table_name)
    
    print(f"📊 VALIDAÇÃO DA TABELA: {full_table_name}")
    print("="*60)
    
    # Contagem total
    final_count = result_df.count()
    print(f"Total de registros: {final_count:,}")
    
    # Distribuição por tipo
    print(f"\n📈 Distribuição por tipo de táxi:")
    result_df.groupBy("taxi_type").count().orderBy("taxi_type").show()
    
    # Distribuição por mês
    print(f"\n📅 Distribuição por mês:")
    result_df.groupBy("year_month").count().orderBy("year_month").show()
    
    # Schema
    print(f"\n📋 Schema da tabela:")
    result_df.printSchema()
    
    # Amostra
    print(f"\n🔍 Amostra dos dados:")
    result_df.limit(5).show(truncate=False)
    
except Exception as e:
    print(f"❌ Erro na validação: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Otimização da Tabela

# COMMAND ----------

try:
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    
    print("🔧 Otimizando tabela Delta...")
    
    # OPTIMIZE para compactação
    spark.sql(f"OPTIMIZE {full_table_name}")
    print("✅ Compactação concluída")
    
    # Estatísticas
    try:
        spark.sql(f"ANALYZE TABLE {full_table_name} COMPUTE STATISTICS")
        print("✅ Estatísticas atualizadas")
    except Exception as e:
        print(f"⚠️ Estatísticas não atualizadas: {e}")
    
except Exception as e:
    print(f"❌ Erro na otimização: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Resumo Final

# COMMAND ----------

print("🎉 INGESTÃO DE DADOS CONCLUÍDA!")
print("="*50)
print(f"✅ Tabela criada: {catalog_name}.{schema_name}.{table_name}")
print(f"📊 Registros processados: {total_records:,}")
print(f"🗂️ Particionamento: taxi_type, year, month")
print(f"🔧 Otimizações aplicadas: OPTIMIZE, ANALYZE")
print("\n🎯 Próximos passos:")
print("   1. Execute o notebook de análises de negócio")
print("   2. Explore os dados com análise exploratória")
print("   3. Responda às perguntas do case técnico")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comandos Úteis para Consultas
# MAGIC 
# MAGIC ```sql
# MAGIC -- Consulta básica
# MAGIC SELECT * FROM main.nyc_taxi.trips_delta LIMIT 10;
# MAGIC 
# MAGIC -- Contagem por tipo
# MAGIC SELECT taxi_type, COUNT(*) as trips 
# MAGIC FROM main.nyc_taxi.trips_delta 
# MAGIC GROUP BY taxi_type;
# MAGIC 
# MAGIC -- Média de tarifa por mês
# MAGIC SELECT year_month, AVG(total_amount) as avg_fare
# MAGIC FROM main.nyc_taxi.trips_delta 
# MAGIC WHERE total_amount > 0
# MAGIC GROUP BY year_month 
# MAGIC ORDER BY year_month;
# MAGIC ```
