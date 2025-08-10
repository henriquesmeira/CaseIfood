# Databricks notebook source
# MAGIC %md
# MAGIC # Case Técnico iFood - Data Architect
# MAGIC ## Notebook 2: Consolidação PySpark (Data Lake)
# MAGIC 
# MAGIC Este notebook implementa a **segunda etapa** da arquitetura:
# MAGIC 
# MAGIC ### Arquitetura V2:
# MAGIC 1. **Python**: Extração dos dados (notebook anterior)
# MAGIC 2. **PySpark**: Consolidação no Delta Lake (este notebook)
# MAGIC 3. **SQL**: Respostas finais do desafio (próximo notebook)
# MAGIC
# MAGIC ### Responsabilidades deste Notebook:
# MAGIC - Criação das camadas Raw → Bronze → Silver → Gold
# MAGIC - Padronização e limpeza dos dados
# MAGIC - Otimização das tabelas Delta Lake
# MAGIC - Preparação para análises SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração e Imports

# COMMAND ----------

from pyspark.sql.functions import (
    lit, col, year, month, hour, dayofweek, when, 
    isnan, isnull, avg, count, sum as spark_sum, 
    min as spark_min, max as spark_max, concat
)
from pyspark.sql.types import *

# Configurações do Data Lake
CATALOG_NAME = "main"
SCHEMA_NAME = "nyc_taxi"
DBFS_RAW_DIR = "/tmp/nyc_taxi/raw"  # Usando /tmp que é permitido

# Nomes das tabelas por camada
RAW_TABLE = "raw_trips"
BRONZE_TABLE = "bronze_trips"
SILVER_TABLE = "silver_trips"
GOLD_TABLE = "gold_trips"

# Colunas obrigatórias
REQUIRED_COLUMNS = [
    "VendorID",
    "passenger_count", 
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

def get_table_name(layer: str) -> str:
    """Retorna nome completo da tabela"""
    layer_tables = {
        "raw": RAW_TABLE,
        "bronze": BRONZE_TABLE, 
        "silver": SILVER_TABLE,
        "gold": GOLD_TABLE
    }
    table = layer_tables.get(layer, RAW_TABLE)
    return f"{CATALOG_NAME}.{SCHEMA_NAME}.{table}"

print("🔄 CONSOLIDAÇÃO DATA LAKE - PYSPARK")
print("="*50)
print(f"📊 Camadas: Raw → Bronze → Silver → Gold")
print(f"🏗️  Catálogo: {CATALOG_NAME}.{SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuração do Ambiente

# COMMAND ----------

# Configurações essenciais do Spark
configs_to_try = {
    "spark.databricks.delta.schema.autoMerge.enabled": "true",
    "spark.sql.execution.arrow.pyspark.enabled": "true"
}

print("🔧 Configurando ambiente Spark...")
for config, value in configs_to_try.items():
    try:
        spark.conf.set(config, value)
        print(f"✅ {config} aplicada")
    except Exception as e:
        print(f"⚠️  {config} não aplicada: {e}")

# Cria catálogo e schema
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")
    spark.sql(f"USE CATALOG {CATALOG_NAME}")
    spark.sql(f"USE {SCHEMA_NAME}")
    print(f"✅ Ambiente configurado: {CATALOG_NAME}.{SCHEMA_NAME}")
except Exception as e:
    print(f"⚠️  Problema na configuração: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificação dos Dados Extraídos

# COMMAND ----------

print("🔍 VERIFICANDO DADOS EXTRAÍDOS")
print("="*40)

try:
    # Lista arquivos no DBFS
    dbfs_files = dbutils.fs.ls(DBFS_RAW_DIR)
    
    print(f"📂 Arquivos encontrados ({len(dbfs_files)}):")
    extracted_files = []
    
    for file_info in dbfs_files:
        filename = file_info.name
        file_size_mb = file_info.size / (1024 * 1024)
        print(f"   • {filename}: {file_size_mb:.1f} MB")
        
        # Extrai informações do nome do arquivo
        if filename.endswith('.parquet'):
            parts = filename.replace('.parquet', '').split('_')
            if len(parts) >= 3:
                taxi_type = parts[0]
                year_month = f"{parts[2]}-{parts[3]}" if len(parts) >= 4 else parts[2]
                
                extracted_files.append({
                    'taxi_type': taxi_type,
                    'year_month': year_month,
                    'filename': filename,
                    'dbfs_path': f"{DBFS_RAW_DIR}/{filename}",
                    'size_mb': file_size_mb
                })
    
    print(f"\n✅ {len(extracted_files)} arquivos prontos para processamento")
    
except Exception as e:
    print(f"❌ Erro ao verificar arquivos: {e}")
    extracted_files = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Camada RAW - Dados Brutos

# COMMAND ----------

print("📥 CRIANDO CAMADA RAW")
print("="*30)

if not extracted_files:
    print("❌ Nenhum arquivo para processar")
    dbutils.notebook.exit("Falha: Nenhum arquivo encontrado")

all_dataframes = []
processed_files = 0
total_records = 0

for file_info in extracted_files:
    try:
        taxi_type = file_info['taxi_type']
        year_month = file_info['year_month']
        dbfs_path = file_info['dbfs_path']
        
        print(f"  📂 Processando {taxi_type} {year_month}")
        
        # Lê arquivo Parquet
        df = spark.read.parquet(dbfs_path)
        
        # Adiciona metadados de origem
        df_with_metadata = df.withColumn("source_file", lit(file_info['filename'])) \
                            .withColumn("taxi_type", lit(taxi_type)) \
                            .withColumn("year_month", lit(year_month)) \
                            .withColumn("ingestion_timestamp", lit(spark.sql("SELECT current_timestamp()").collect()[0][0]))
        
        record_count = df_with_metadata.count()
        print(f"    ✅ {record_count:,} registros")
        
        all_dataframes.append(df_with_metadata)
        processed_files += 1
        total_records += record_count
        
    except Exception as e:
        print(f"    ❌ Erro ao processar {file_info['filename']}: {e}")

if not all_dataframes:
    print("❌ Nenhum DataFrame foi criado")
    dbutils.notebook.exit("Falha: Nenhum DataFrame criado")

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

print(f"✅ Camada Raw criada: {total_records:,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Camada BRONZE - Dados Padronizados

# COMMAND ----------

print("🥉 CRIANDO CAMADA BRONZE")
print("="*30)

# Lê dados da camada Raw
raw_table = get_table_name("raw")
raw_df = spark.table(raw_table)
print(f"📂 Carregando dados Raw: {raw_df.count():,} registros")

# Função para padronizar schema
def standardize_schema(df):
    """Padroniza schema entre diferentes tipos de táxi"""
    print("  🔧 Padronizando schema...")
    
    # Mapeamento de colunas para green taxis
    column_mapping = {
        "lpep_pickup_datetime": "tpep_pickup_datetime",
        "lpep_dropoff_datetime": "tpep_dropoff_datetime"
    }
    
    # Aplica mapeamento para green taxis
    green_df = df.filter(col("taxi_type") == "green")
    yellow_df = df.filter(col("taxi_type") == "yellow")
    
    # Renomeia colunas do green taxi
    for old_col, new_col in column_mapping.items():
        if old_col in green_df.columns:
            green_df = green_df.withColumnRenamed(old_col, new_col)
    
    # Combina novamente
    if green_df.count() > 0 and yellow_df.count() > 0:
        df = yellow_df.union(green_df)
    elif green_df.count() > 0:
        df = green_df
    else:
        df = yellow_df
    
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

# Função para limpeza básica
def basic_data_cleaning(df):
    """Limpeza básica dos dados"""
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

# Função para adicionar colunas derivadas
def add_derived_columns(df):
    """Adiciona colunas derivadas para análise"""
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

# Aplica transformações
bronze_df = standardize_schema(raw_df)
bronze_df = basic_data_cleaning(bronze_df)
bronze_df = add_derived_columns(bronze_df)

# Salva camada Bronze
bronze_table = get_table_name("bronze")
print(f"💾 Salvando camada Bronze: {bronze_table}")

bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .partitionBy("taxi_type", "pickup_year", "pickup_month") \
    .saveAsTable(bronze_table)

bronze_count = bronze_df.count()
print(f"✅ Camada Bronze criada: {bronze_count:,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Camada SILVER - Dados Enriquecidos

# COMMAND ----------

print("🥈 CRIANDO CAMADA SILVER")
print("="*30)

# Lê dados da camada Bronze
bronze_table = get_table_name("bronze")
bronze_df = spark.table(bronze_table)
print(f"📂 Carregando dados Bronze: {bronze_df.count():,} registros")

# Função para aplicar regras de qualidade
def apply_quality_rules(df):
    """Aplica regras de qualidade rigorosas"""
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

# Função para enriquecer dados
def enrich_data(df):
    """Enriquece dados com informações adicionais"""
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

# Aplica transformações
silver_df = apply_quality_rules(bronze_df)
silver_df = enrich_data(silver_df)

# Salva camada Silver
silver_table = get_table_name("silver")
print(f"💾 Salvando camada Silver: {silver_table}")

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .partitionBy("taxi_type", "pickup_year", "pickup_month") \
    .saveAsTable(silver_table)

silver_count = silver_df.count()
print(f"✅ Camada Silver criada: {silver_count:,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Camada GOLD - Dados Agregados

# COMMAND ----------

print("🥇 CRIANDO CAMADA GOLD")
print("="*30)

# Lê dados da camada Silver
silver_table = get_table_name("silver")
silver_df = spark.table(silver_table)
print(f"📂 Carregando dados Silver: {silver_df.count():,} registros")

# Função para criar agregações analíticas
def create_analytical_aggregations(df):
    """Cria agregações para análises de negócio"""
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

# Cria agregações
gold_df = create_analytical_aggregations(silver_df)

# Salva camada Gold
gold_table = get_table_name("gold")
print(f"💾 Salvando camada Gold: {gold_table}")

gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(gold_table)

gold_count = gold_df.count()
print(f"✅ Camada Gold criada: {gold_count:,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Otimização das Tabelas

# COMMAND ----------

print("⚡ OTIMIZANDO TABELAS DELTA")
print("="*30)

tables = ["raw", "bronze", "silver", "gold"]

for layer in tables:
    try:
        table_name = get_table_name(layer)
        print(f"  🔧 Otimizando {table_name}...")
        
        # OPTIMIZE
        spark.sql(f"OPTIMIZE {table_name}")
        
        # ANALYZE TABLE
        spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
        
        print(f"    ✅ {layer} otimizada")
        
    except Exception as e:
        print(f"    ⚠️  Erro na otimização de {layer}: {e}")

print("✅ Otimização concluída")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Relatório de Consolidação

# COMMAND ----------

print("📊 RELATÓRIO DE CONSOLIDAÇÃO")
print("="*40)

try:
    tables = ["raw", "bronze", "silver", "gold"]
    
    for layer in tables:
        table_name = get_table_name(layer)
        try:
            count = spark.table(table_name).count()
            print(f"   • {layer.upper()}: {count:,} registros")
        except:
            print(f"   • {layer.upper()}: Erro ao contar registros")
    
    print(f"\n✅ Consolidação concluída com sucesso!")
    print(f"📊 Arquivos processados: {processed_files}")
    print(f"📈 Total de registros originais: {total_records:,}")
    
    # Mostra schema da tabela Gold
    print(f"\n📋 Schema da tabela Gold:")
    spark.table(get_table_name("gold")).printSchema()
    
    # Mostra amostra da tabela Gold
    print(f"\n🔍 Amostra da tabela Gold:")
    spark.table(get_table_name("gold")).limit(3).show(truncate=False)
    
except Exception as e:
    print(f"❌ Erro no relatório: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Preparação para Análises SQL

# COMMAND ----------

print("📊 PREPARANDO PARA ANÁLISES SQL")
print("="*40)

gold_table = get_table_name("gold")

# Verifica se tabela Gold está pronta
try:
    gold_df = spark.table(gold_table)
    gold_count = gold_df.count()
    
    print(f"✅ Tabela Gold pronta: {gold_table}")
    print(f"📊 Registros disponíveis: {gold_count:,}")
    
    # Mostra distribuição dos dados
    print(f"\n📈 Distribuição dos dados:")
    gold_df.groupBy("taxi_type", "year_month").agg(
        spark_sum("total_trips").alias("viagens")
    ).orderBy("taxi_type", "year_month").show()
    
    print(f"\n💡 Exemplos de consultas SQL:")
    print(f"```sql")
    print(f"-- Pergunta 1: Média Yellow Taxis")
    print(f"SELECT ROUND(SUM(sum_total_amount)/SUM(total_trips), 2) as media_yellow")
    print(f"FROM {gold_table}")
    print(f"WHERE taxi_type = 'yellow';")
    print(f"")
    print(f"-- Pergunta 2: Passageiros por hora em Maio")
    print(f"SELECT pickup_hour, ROUND(SUM(sum_passenger_count)/SUM(total_trips), 2) as media_pass")
    print(f"FROM {gold_table}")
    print(f"WHERE pickup_month = 5 AND pickup_year = 2023")
    print(f"GROUP BY pickup_hour ORDER BY pickup_hour;")
    print(f"```")
    
except Exception as e:
    print(f"❌ Erro ao verificar tabela Gold: {e}")

print(f"\n🚀 Próximo passo: Execute o notebook de Análises SQL")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo da Consolidação
# MAGIC 
# MAGIC ### ✅ Etapa 2 Concluída: Consolidação PySpark
# MAGIC 
# MAGIC **O que foi feito:**
# MAGIC - ✅ Criação da camada Raw (dados brutos)
# MAGIC - ✅ Criação da camada Bronze (dados padronizados)
# MAGIC - ✅ Criação da camada Silver (dados enriquecidos)
# MAGIC - ✅ Criação da camada Gold (dados agregados)
# MAGIC - ✅ Otimização das tabelas Delta Lake
# MAGIC 
# MAGIC **Transformações aplicadas:**
# MAGIC - Padronização de schema (Yellow vs Green taxis)
# MAGIC - Limpeza e validação de dados
# MAGIC - Enriquecimento com colunas derivadas
# MAGIC - Agregações para análises de negócio
# MAGIC 
# MAGIC **Tabelas criadas:**
# MAGIC - `main.nyc_taxi.raw_trips` - Dados brutos
# MAGIC - `main.nyc_taxi.bronze_trips` - Dados padronizados
# MAGIC - `main.nyc_taxi.silver_trips` - Dados enriquecidos
# MAGIC - `main.nyc_taxi.gold_trips` - Dados agregados (para análises)
# MAGIC 
# MAGIC ### 🚀 Próximos Passos:
# MAGIC 1. **Notebook 3**: Análises SQL (Respostas às perguntas do case)
# MAGIC 
# MAGIC ### 🏗️ Arquitetura Implementada:
# MAGIC ```
# MAGIC [Python Download] → [DBFS Storage] → [PySpark Processing] → [SQL Analysis]
# MAGIC      ✅ FEITO           ✅ FEITO         ✅ FEITO            📊 PRÓXIMO
# MAGIC ```
