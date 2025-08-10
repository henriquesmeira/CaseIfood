"""
Análises para responder às perguntas de negócio do case técnico iFood
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, hour, month, year, sum as spark_sum
from pyspark.sql.types import *
import sys
import os

# Adiciona o diretório src ao path para importar configurações
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from config import get_full_table_name


class BusinessQuestionsAnalyzer:
    """
    Classe para análise das perguntas de negócio
    """
    
    def __init__(self, spark: SparkSession):
        """
        Inicializa o analisador
        
        Args:
            spark: Sessão do Spark
        """
        self.spark = spark
        self.table_name = get_full_table_name()
        
    def load_data(self):
        """Carrega os dados da tabela Delta"""
        try:
            self.df = self.spark.table(self.table_name)
            print(f"✅ Dados carregados da tabela: {self.table_name}")
            print(f"📊 Total de registros: {self.df.count():,}")
            return True
        except Exception as e:
            print(f"❌ Erro ao carregar dados: {e}")
            return False
    
    def question_1_yellow_taxi_monthly_average(self):
        """
        Pergunta 1: Qual a média de valor total (total_amount) recebido em um mês 
        considerando todos os yellow taxis da frota?
        """
        print("\n" + "="*80)
        print("📊 PERGUNTA 1: Média de valor total dos Yellow Taxis por mês")
        print("="*80)
        
        try:
            # Filtra apenas yellow taxis e agrupa por mês
            yellow_monthly_avg = (
                self.df
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
            
            print("\n📈 Resultados por mês (Yellow Taxis):")
            print("-" * 60)
            
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
            
            # Média geral de todos os meses
            overall_avg = (
                self.df
                .filter(col("taxi_type") == "yellow")
                .filter(col("total_amount").isNotNull() & (col("total_amount") > 0))
                .agg(avg("total_amount").alias("overall_avg"))
                .collect()[0]["overall_avg"]
            )
            
            print(f"🎯 RESPOSTA PRINCIPAL:")
            print(f"   Média geral de valor total por viagem (Yellow Taxis): ${overall_avg:.2f}")
            
            # Mostra também a tabela formatada
            print(f"\n📋 Tabela Resumo:")
            yellow_monthly_avg.show(truncate=False)
            
            return results
            
        except Exception as e:
            print(f"❌ Erro na análise da Pergunta 1: {e}")
            return None
    
    def question_2_hourly_passenger_average_may(self):
        """
        Pergunta 2: Qual a média de passageiros (passenger_count) por cada hora do dia 
        que pegaram táxi no mês de maio considerando todos os táxis da frota?
        """
        print("\n" + "="*80)
        print("📊 PERGUNTA 2: Média de passageiros por hora em Maio (Todos os táxis)")
        print("="*80)
        
        try:
            # Filtra dados de maio e extrai hora do pickup
            may_hourly_avg = (
                self.df
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
            
            print("\n📈 Resultados por hora do dia (Maio 2023 - Todos os táxis):")
            print("-" * 70)
            
            results = may_hourly_avg.collect()
            
            # Encontra picos de demanda
            max_avg = max(row["avg_passengers"] for row in results)
            min_avg = min(row["avg_passengers"] for row in results)
            
            for row in results:
                hour_val = row["pickup_hour"]
                avg_passengers = row["avg_passengers"]
                total_trips = row["total_trips"]
                total_passengers = row["total_passengers"]
                
                # Adiciona indicador de pico
                indicator = ""
                if avg_passengers == max_avg:
                    indicator = " 🔥 PICO"
                elif avg_passengers == min_avg:
                    indicator = " 📉 MENOR"
                
                print(f"🕐 {hour_val:02d}:00h - Média: {avg_passengers:.2f} passageiros/viagem{indicator}")
                print(f"     Viagens: {total_trips:,} | Total passageiros: {total_passengers:,}")
                print()
            
            # Análise adicional por período do dia
            print(f"\n📊 Análise por Período do Dia:")
            print("-" * 40)
            
            periods_analysis = (
                self.df
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
                    count("*").alias("total_trips")
                )
                .orderBy("avg_passengers", ascending=False)
            )
            
            period_results = periods_analysis.collect()
            for row in period_results:
                period = row["period"]
                avg_pass = row["avg_passengers"]
                trips = row["total_trips"]
                print(f"   • {period}: {avg_pass:.2f} passageiros/viagem ({trips:,} viagens)")
            
            print(f"\n🎯 RESPOSTA PRINCIPAL:")
            print(f"   A tabela acima mostra a média de passageiros por hora em Maio 2023")
            
            # Mostra também a tabela formatada
            print(f"\n📋 Tabela Detalhada:")
            may_hourly_avg.show(24, truncate=False)  # 24 horas
            
            return results
            
        except Exception as e:
            print(f"❌ Erro na análise da Pergunta 2: {e}")
            return None
    
    def additional_insights(self):
        """Análises adicionais para insights de negócio"""
        print("\n" + "="*80)
        print("💡 INSIGHTS ADICIONAIS")
        print("="*80)
        
        try:
            # Comparação entre Yellow e Green taxis
            print("\n📊 Comparação Yellow vs Green Taxis:")
            print("-" * 50)
            
            comparison = (
                self.df
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
            
            # Evolução mensal
            print("\n📈 Evolução Mensal (Todos os táxis):")
            print("-" * 40)
            
            monthly_evolution = (
                self.df
                .filter(col("total_amount").isNotNull() & (col("total_amount") > 0))
                .groupBy("year_month")
                .agg(
                    count("*").alias("total_trips"),
                    avg("total_amount").alias("avg_fare"),
                    spark_sum("total_amount").alias("total_revenue")
                )
                .orderBy("year_month")
            )
            
            monthly_evolution.show(truncate=False)
            
        except Exception as e:
            print(f"❌ Erro nas análises adicionais: {e}")
    
    def run_all_analyses(self):
        """Executa todas as análises"""
        print("🚀 INICIANDO ANÁLISES DE NEGÓCIO - CASE IFOOD")
        print("="*80)
        
        # Carrega dados
        if not self.load_data():
            return False
        
        # Executa as perguntas principais
        print("\n🎯 RESPONDENDO ÀS PERGUNTAS DO CASE:")
        
        # Pergunta 1
        q1_results = self.question_1_yellow_taxi_monthly_average()
        
        # Pergunta 2  
        q2_results = self.question_2_hourly_passenger_average_may()
        
        # Insights adicionais
        self.additional_insights()
        
        print("\n" + "="*80)
        print("✅ ANÁLISES CONCLUÍDAS COM SUCESSO!")
        print("="*80)
        
        return True


def main():
    """Função principal para execução das análises"""
    # Inicializa Spark
    spark = SparkSession.builder.appName("Business_Questions_Analysis").getOrCreate()
    
    # Cria analisador e executa
    analyzer = BusinessQuestionsAnalyzer(spark)
    success = analyzer.run_all_analyses()
    
    if not success:
        print("💥 Falha na execução das análises!")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
