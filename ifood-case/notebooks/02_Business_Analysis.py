# Databricks notebook source
# MAGIC %md
# MAGIC # Case Técnico iFood - Data Architect
# MAGIC ## Notebook 2: Análises de Negócio
# MAGIC 
# MAGIC Este notebook responde às perguntas específicas do case técnico:
# MAGIC 
# MAGIC 1. **Qual a média de valor total (total_amount) recebido em um mês considerando todos os yellow taxis da frota?**
# MAGIC 2. **Qual a média de passageiros (passenger_count) por cada hora do dia que pegaram táxi no mês de maio considerando todos os táxis da frota?**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração Inicial

# COMMAND ----------

from pyspark.sql.functions import col, avg, count, hour, sum as spark_sum, when, desc
from pyspark.sql.types import *

# Configuração da tabela
catalog_name = "main"
schema_name = "nyc_taxi"
table_name = "trips_delta"
full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

print(f"📊 Analisando dados da tabela: {full_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Carregamento e Visão Geral dos Dados

# COMMAND ----------

# Carrega os dados
df = spark.table(full_table_name)

print(f"📈 Visão geral dos dados:")
print(f"   • Total de registros: {df.count():,}")
print(f"   • Período: Janeiro a Maio 2023")

# Verifica distribuição por tipo de táxi
print(f"\n🚕 Distribuição por tipo de táxi:")
df.groupBy("taxi_type").count().orderBy("taxi_type").show()

# Verifica distribuição por mês
print(f"\n📅 Distribuição por mês:")
df.groupBy("year_month").count().orderBy("year_month").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. PERGUNTA 1: Média de Valor Total - Yellow Taxis por Mês
# MAGIC 
# MAGIC **Pergunta:** Qual a média de valor total (total_amount) recebido em um mês considerando todos os yellow taxis da frota?

# COMMAND ----------

print("="*80)
print("📊 PERGUNTA 1: Média de valor total dos Yellow Taxis por mês")
print("="*80)

# Filtra yellow taxis e calcula médias por mês
yellow_monthly_avg = (
    df
    .filter(col("taxi_type") == "yellow")
    .filter(col("total_amount").isNotNull() & (col("total_amount") > 0))
    .groupBy("year_month")
    .agg(
        avg("total_amount").alias("avg_total_amount"),
        count("*").alias("total_trips"),
        spark_sum("total_amount").alias("total_revenue")
    )
    .orderBy("year_month")
)

print("\n📈 Resultados detalhados por mês (Yellow Taxis):")
print("-" * 70)

# Coleta e exibe resultados
results = yellow_monthly_avg.collect()

for row in results:
    month = row["year_month"]
    avg_amount = row["avg_total_amount"]
    total_trips = row["total_trips"]
    total_revenue = row["total_revenue"]
    
    print(f"🗓️  {month}:")
    print(f"   • Média por viagem: ${avg_amount:.2f}")
    print(f"   • Total de viagens: {total_trips:,}")
    print(f"   • Receita total: ${total_revenue:,.2f}")
    print()

# Calcula média geral
overall_avg = (
    df
    .filter(col("taxi_type") == "yellow")
    .filter(col("total_amount").isNotNull() & (col("total_amount") > 0))
    .agg(avg("total_amount").alias("overall_avg"))
    .collect()[0]["overall_avg"]
)

print(f"🎯 RESPOSTA PRINCIPAL:")
print(f"   Média geral de valor total por viagem (Yellow Taxis): ${overall_avg:.2f}")

# Exibe tabela formatada
print(f"\n📋 Tabela Resumo:")
yellow_monthly_avg.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. PERGUNTA 2: Média de Passageiros por Hora em Maio
# MAGIC 
# MAGIC **Pergunta:** Qual a média de passageiros (passenger_count) por cada hora do dia que pegaram táxi no mês de maio considerando todos os táxis da frota?

# COMMAND ----------

print("="*80)
print("📊 PERGUNTA 2: Média de passageiros por hora em Maio (Todos os táxis)")
print("="*80)

# Filtra dados de maio e calcula médias por hora
may_hourly_avg = (
    df
    .filter(col("year_month") == "2023-05")
    .filter(col("passenger_count").isNotNull() & (col("passenger_count") > 0))
    .filter(col("tpep_pickup_datetime").isNotNull())
    .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
    .groupBy("pickup_hour")
    .agg(
        avg("passenger_count").alias("avg_passengers"),
        count("*").alias("total_trips"),
        spark_sum("passenger_count").alias("total_passengers")
    )
    .orderBy("pickup_hour")
)

print("\n📈 Resultados detalhados por hora (Maio 2023 - Todos os táxis):")
print("-" * 80)

# Coleta resultados
hourly_results = may_hourly_avg.collect()

# Encontra picos
max_avg = max(row["avg_passengers"] for row in hourly_results)
min_avg = min(row["avg_passengers"] for row in hourly_results)

for row in hourly_results:
    hour_val = row["pickup_hour"]
    avg_passengers = row["avg_passengers"]
    total_trips = row["total_trips"]
    total_passengers = row["total_passengers"]
    
    # Indicadores de pico
    indicator = ""
    if avg_passengers == max_avg:
        indicator = " 🔥 PICO"
    elif avg_passengers == min_avg:
        indicator = " 📉 MENOR"
    
    print(f"🕐 {hour_val:02d}:00h - Média: {avg_passengers:.2f} passageiros/viagem{indicator}")
    print(f"     Viagens: {total_trips:,} | Total passageiros: {total_passengers:,}")
    print()

print(f"🎯 RESPOSTA PRINCIPAL:")
print(f"   A tabela acima mostra a média de passageiros por hora em Maio 2023")

# Exibe tabela formatada
print(f"\n📋 Tabela Detalhada (24 horas):")
may_hourly_avg.show(24, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Análise Complementar por Períodos do Dia

# COMMAND ----------

print("\n📊 Análise Complementar - Períodos do Dia (Maio 2023):")
print("-" * 60)

# Análise por períodos do dia
periods_analysis = (
    df
    .filter(col("year_month") == "2023-05")
    .filter(col("passenger_count").isNotNull() & (col("passenger_count") > 0))
    .filter(col("tpep_pickup_datetime").isNotNull())
    .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
    .withColumn("period", 
        when(col("pickup_hour").between(6, 11), "Manhã (6h-11h)")
        .when(col("pickup_hour").between(12, 17), "Tarde (12h-17h)")
        .when(col("pickup_hour").between(18, 23), "Noite (18h-23h)")
        .otherwise("Madrugada (0h-5h)")
    )
    .groupBy("period")
    .agg(
        avg("passenger_count").alias("avg_passengers"),
        count("*").alias("total_trips"),
        spark_sum("passenger_count").alias("total_passengers")
    )
    .orderBy(desc("avg_passengers"))
)

period_results = periods_analysis.collect()

for row in period_results:
    period = row["period"]
    avg_pass = row["avg_passengers"]
    trips = row["total_trips"]
    total_pass = row["total_passengers"]
    
    print(f"   • {period}:")
    print(f"     - Média: {avg_pass:.2f} passageiros/viagem")
    print(f"     - Viagens: {trips:,}")
    print(f"     - Total passageiros: {total_pass:,}")
    print()

periods_analysis.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Análises Adicionais e Insights

# COMMAND ----------

print("="*80)
print("💡 ANÁLISES ADICIONAIS E INSIGHTS")
print("="*80)

# Comparação Yellow vs Green para contexto
print("\n🚕 Comparação Yellow vs Green Taxis (Contexto):")
comparison = (
    df
    .filter(col("total_amount").isNotNull() & (col("total_amount") > 0))
    .groupBy("taxi_type")
    .agg(
        avg("total_amount").alias("avg_fare"),
        avg("passenger_count").alias("avg_passengers"),
        count("*").alias("total_trips"),
        spark_sum("total_amount").alias("total_revenue")
    )
)
comparison.show(truncate=False)

# Evolução mensal geral
print("\n📈 Evolução Mensal - Todos os Táxis:")
monthly_evolution = (
    df
    .filter(col("total_amount").isNotNull() & (col("total_amount") > 0))
    .groupBy("year_month")
    .agg(
        count("*").alias("total_trips"),
        avg("total_amount").alias("avg_fare"),
        avg("passenger_count").alias("avg_passengers"),
        spark_sum("total_amount").alias("total_revenue")
    )
    .orderBy("year_month")
)
monthly_evolution.show(truncate=False)

# Top 5 horas com mais viagens em maio
print("\n🔝 Top 5 Horas com Mais Viagens (Maio 2023):")
top_hours_may = (
    df
    .filter(col("year_month") == "2023-05")
    .filter(col("tpep_pickup_datetime").isNotNull())
    .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
    .groupBy("pickup_hour")
    .agg(count("*").alias("total_trips"))
    .orderBy(desc("total_trips"))
    .limit(5)
)
top_hours_may.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Resumo das Respostas

# COMMAND ----------

print("="*80)
print("📋 RESUMO DAS RESPOSTAS - CASE TÉCNICO IFOOD")
print("="*80)

# Recalcula para o resumo final
yellow_overall_avg = (
    df
    .filter(col("taxi_type") == "yellow")
    .filter(col("total_amount").isNotNull() & (col("total_amount") > 0))
    .agg(avg("total_amount"))
    .collect()[0][0]
)

print(f"\n🎯 PERGUNTA 1 - Média de valor total Yellow Taxis:")
print(f"   RESPOSTA: ${yellow_overall_avg:.2f} por viagem")
print(f"   (Considerando todos os meses de Janeiro a Maio 2023)")

print(f"\n🎯 PERGUNTA 2 - Média de passageiros por hora em Maio:")
print(f"   RESPOSTA: Veja tabela detalhada acima com 24 horas")
print(f"   (Considerando todos os tipos de táxi em Maio 2023)")

# Estatísticas adicionais para contexto
total_yellow_trips = df.filter(col("taxi_type") == "yellow").count()
total_may_trips = df.filter(col("year_month") == "2023-05").count()

print(f"\n📊 Estatísticas de Contexto:")
print(f"   • Total viagens Yellow Taxis (Jan-Mai): {total_yellow_trips:,}")
print(f"   • Total viagens em Maio (todos táxis): {total_may_trips:,}")
print(f"   • Período analisado: Janeiro a Maio 2023")
print(f"   • Tipos de táxi: Yellow e Green")

print(f"\n✅ Análises concluídas com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Consultas SQL Equivalentes
# MAGIC 
# MAGIC Para referência, as mesmas análises podem ser feitas com SQL:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PERGUNTA 1: Média de valor total Yellow Taxis por mês
# MAGIC SELECT 
# MAGIC     year_month,
# MAGIC     ROUND(AVG(total_amount), 2) as avg_total_amount,
# MAGIC     COUNT(*) as total_trips,
# MAGIC     ROUND(SUM(total_amount), 2) as total_revenue
# MAGIC FROM main.nyc_taxi.trips_delta 
# MAGIC WHERE taxi_type = 'yellow' 
# MAGIC   AND total_amount IS NOT NULL 
# MAGIC   AND total_amount > 0
# MAGIC GROUP BY year_month
# MAGIC ORDER BY year_month;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PERGUNTA 2: Média de passageiros por hora em Maio
# MAGIC SELECT 
# MAGIC     HOUR(tpep_pickup_datetime) as pickup_hour,
# MAGIC     ROUND(AVG(passenger_count), 2) as avg_passengers,
# MAGIC     COUNT(*) as total_trips,
# MAGIC     SUM(passenger_count) as total_passengers
# MAGIC FROM main.nyc_taxi.trips_delta 
# MAGIC WHERE year_month = '2023-05'
# MAGIC   AND passenger_count IS NOT NULL 
# MAGIC   AND passenger_count > 0
# MAGIC   AND tpep_pickup_datetime IS NOT NULL
# MAGIC GROUP BY HOUR(tpep_pickup_datetime)
# MAGIC ORDER BY pickup_hour;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusão
# MAGIC 
# MAGIC ✅ **Análises concluídas com sucesso!**
# MAGIC 
# MAGIC Este notebook respondeu às duas perguntas principais do case técnico:
# MAGIC 
# MAGIC 1. **Média de valor total Yellow Taxis**: Calculada por mês e geral
# MAGIC 2. **Média de passageiros por hora em Maio**: Detalhada para todas as 24 horas
# MAGIC 
# MAGIC Os dados foram processados usando PySpark no Databricks, conforme solicitado no case técnico.
# MAGIC 
# MAGIC **Próximos passos sugeridos:**
# MAGIC - Análise exploratória mais profunda
# MAGIC - Visualizações dos padrões identificados
# MAGIC - Análises preditivas ou de tendências
