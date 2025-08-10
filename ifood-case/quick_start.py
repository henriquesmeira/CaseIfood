"""
Script de execução rápida para o Case Técnico iFood
Execute este script no Databricks para rodar todo o pipeline
"""

# MAGIC %md
# MAGIC # Case Técnico iFood - Execução Rápida
# MAGIC 
# MAGIC Este script executa todo o pipeline de forma automatizada:
# MAGIC 1. Ingestão dos dados
# MAGIC 2. Análises de negócio
# MAGIC 3. Análise exploratória

# COMMAND ----------

print("🚀 INICIANDO CASE TÉCNICO IFOOD - DATA ARCHITECT")
print("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração Inicial

# COMMAND ----------

from pyspark.sql.functions import lit, col, avg, count, hour, sum as spark_sum, when, desc
import time

# Configurações otimizadas
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

print("✅ Configurações aplicadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingestão de Dados (Versão Simplificada)

# COMMAND ----------

# URLs dos dados (versão reduzida para execução rápida)
data_sources = {
    "yellow": {
        "2023-01": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
        "2023-02": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet",
        "2023-05": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-05.parquet"
    },
    "green": {
        "2023-01": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-01.parquet",
        "2023-05": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-05.parquet"
    }
}

table_name = "nyc_taxi_quick"

print(f"📊 Processando {sum(len(months) for months in data_sources.values())} arquivos...")

# COMMAND ----------

# Função de processamento simplificada
def process_data_quick(url, taxi_type, year_month):
    try:
        print(f"  📥 Carregando {taxi_type} {year_month}")
        
        df = spark.read.parquet(url)
        
        # Padroniza colunas para green taxis
        if taxi_type == "green":
            if "lpep_pickup_datetime" in df.columns:
                df = df.withColumnRenamed("lpep_pickup_datetime", "tpep_pickup_datetime")
            if "lpep_dropoff_datetime" in df.columns:
                df = df.withColumnRenamed("lpep_dropoff_datetime", "tpep_dropoff_datetime")
        
        # Seleciona colunas essenciais
        essential_cols = ["VendorID", "passenger_count", "total_amount", 
                         "tpep_pickup_datetime", "tpep_dropoff_datetime"]
        
        available_cols = [c for c in essential_cols if c in df.columns]
        df_selected = df.select(*available_cols)
        
        # Adiciona metadados
        year_val, month_val = year_month.split("-")
        df_final = df_selected.withColumn("taxi_type", lit(taxi_type)) \
                             .withColumn("year", lit(int(year_val))) \
                             .withColumn("month", lit(int(month_val))) \
                             .withColumn("year_month", lit(year_month))
        
        count = df_final.count()
        print(f"    ✅ {count:,} registros")
        return df_final, count
        
    except Exception as e:
        print(f"    ❌ Erro: {str(e)[:100]}")
        return None, 0

# COMMAND ----------

# Processa todos os dados
all_dfs = []
total_records = 0

for taxi_type, months in data_sources.items():
    print(f"\n🚕 Processando {taxi_type}:")
    for year_month, url in months.items():
        df, count = process_data_quick(url, taxi_type, year_month)
        if df is not None:
            all_dfs.append(df)
            total_records += count

print(f"\n📊 Total processado: {total_records:,} registros")

# COMMAND ----------

# Cria tabela Delta
if all_dfs:
    print("🔄 Combinando DataFrames...")
    combined_df = all_dfs[0]
    for df in all_dfs[1:]:
        combined_df = combined_df.union(df)
    
    print("💾 Salvando tabela Delta...")
    combined_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("taxi_type") \
        .saveAsTable(table_name)
    
    print(f"✅ Tabela {table_name} criada!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Análises de Negócio - Respostas do Case

# COMMAND ----------

print("📊 RESPONDENDO ÀS PERGUNTAS DO CASE TÉCNICO")
print("="*50)

df = spark.table(table_name)

# PERGUNTA 1: Média Yellow Taxis
print("\n🎯 PERGUNTA 1: Média de valor total Yellow Taxis")
print("-" * 50)

yellow_avg = (
    df
    .filter(col("taxi_type") == "yellow")
    .filter(col("total_amount").isNotNull() & (col("total_amount") > 0))
    .agg(avg("total_amount").alias("avg_amount"))
    .collect()[0]["avg_amount"]
)

yellow_monthly = (
    df
    .filter(col("taxi_type") == "yellow")
    .filter(col("total_amount").isNotNull() & (col("total_amount") > 0))
    .groupBy("year_month")
    .agg(avg("total_amount").alias("avg_amount"))
    .orderBy("year_month")
)

print(f"💰 RESPOSTA: ${yellow_avg:.2f} (média geral)")
print("\n📅 Por mês:")
yellow_monthly.show()

# COMMAND ----------

# PERGUNTA 2: Passageiros por hora em Maio
print("\n🎯 PERGUNTA 2: Passageiros por hora em Maio")
print("-" * 50)

may_hourly = (
    df
    .filter(col("year_month") == "2023-05")
    .filter(col("passenger_count").isNotNull() & (col("passenger_count") > 0))
    .filter(col("tpep_pickup_datetime").isNotNull())
    .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
    .groupBy("pickup_hour")
    .agg(
        avg("passenger_count").alias("avg_passengers"),
        count("*").alias("trips")
    )
    .orderBy("pickup_hour")
)

print("👥 RESPOSTA: Média de passageiros por hora em Maio 2023")
may_hourly.show(24)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Análise Exploratória Resumida

# COMMAND ----------

print("🔍 ANÁLISE EXPLORATÓRIA RESUMIDA")
print("="*40)

# Estatísticas gerais
total_trips = df.count()
print(f"📊 Total de viagens: {total_trips:,}")

# Distribuição por tipo
print("\n🚕 Distribuição por tipo:")
df.groupBy("taxi_type").count().show()

# Distribuição por mês
print("\n📅 Distribuição por mês:")
df.groupBy("year_month").count().orderBy("year_month").show()

# Estatísticas de tarifas
print("\n💰 Estatísticas de tarifas:")
df.filter(col("total_amount") > 0).describe("total_amount").show()

# Estatísticas de passageiros
print("\n👥 Estatísticas de passageiros:")
df.filter(col("passenger_count") > 0).describe("passenger_count").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resumo Final

# COMMAND ----------

print("🎉 CASE TÉCNICO IFOOD - RESUMO FINAL")
print("="*50)

# Recalcula métricas finais
final_yellow_avg = (
    df
    .filter(col("taxi_type") == "yellow")
    .filter(col("total_amount") > 0)
    .agg(avg("total_amount"))
    .collect()[0][0]
)

final_total_trips = df.count()
final_yellow_trips = df.filter(col("taxi_type") == "yellow").count()

print(f"\n✅ RESULTADOS PRINCIPAIS:")
print(f"   • Tabela criada: {table_name}")
print(f"   • Total de registros: {final_total_trips:,}")
print(f"   • Viagens Yellow Taxi: {final_yellow_trips:,}")

print(f"\n🎯 RESPOSTAS ÀS PERGUNTAS:")
print(f"   1. Média Yellow Taxis: ${final_yellow_avg:.2f}")
print(f"   2. Passageiros por hora: Ver tabela detalhada acima")

print(f"\n📊 DADOS PROCESSADOS:")
print(f"   • Período: Janeiro, Fevereiro e Maio 2023")
print(f"   • Tipos: Yellow e Green Taxis")
print(f"   • Formato: Delta Lake particionado")

print(f"\n🚀 PRÓXIMOS PASSOS:")
print(f"   • Execute os notebooks completos para análise detalhada")
print(f"   • Explore a tabela: SELECT * FROM {table_name} LIMIT 10")
print(f"   • Crie visualizações adicionais conforme necessário")

print(f"\n🏁 Case técnico executado com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comandos SQL para Consultas Adicionais

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consulta básica da tabela criada
# MAGIC SELECT * FROM nyc_taxi_quick LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificação das respostas
# MAGIC 
# MAGIC -- Pergunta 1: Média Yellow Taxis
# MAGIC SELECT 
# MAGIC     year_month,
# MAGIC     ROUND(AVG(total_amount), 2) as avg_fare,
# MAGIC     COUNT(*) as trips
# MAGIC FROM nyc_taxi_quick 
# MAGIC WHERE taxi_type = 'yellow' AND total_amount > 0
# MAGIC GROUP BY year_month
# MAGIC ORDER BY year_month;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pergunta 2: Passageiros por hora em Maio
# MAGIC SELECT 
# MAGIC     HOUR(tpep_pickup_datetime) as hour,
# MAGIC     ROUND(AVG(passenger_count), 2) as avg_passengers,
# MAGIC     COUNT(*) as trips
# MAGIC FROM nyc_taxi_quick 
# MAGIC WHERE year_month = '2023-05' 
# MAGIC   AND passenger_count > 0 
# MAGIC   AND tpep_pickup_datetime IS NOT NULL
# MAGIC GROUP BY HOUR(tpep_pickup_datetime)
# MAGIC ORDER BY hour;
