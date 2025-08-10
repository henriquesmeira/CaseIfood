"""
Análise exploratória dos dados de táxi NYC
Gera insights e visualizações para entendimento do dataset
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, min as spark_min, max as spark_max,
    hour, dayofweek, when, desc, asc, percentile_approx, stddev
)
import sys
import os

# Adiciona o diretório src ao path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from config import get_full_table_name


class ExploratoryAnalyzer:
    """
    Classe para análise exploratória dos dados
    """
    
    def __init__(self, spark: SparkSession):
        """
        Inicializa o analisador exploratório
        
        Args:
            spark: Sessão do Spark
        """
        self.spark = spark
        self.table_name = get_full_table_name()
        self.df = None
        
    def load_and_overview(self):
        """Carrega dados e mostra visão geral"""
        try:
            self.df = self.spark.table(self.table_name)
            
            print("📊 ANÁLISE EXPLORATÓRIA - DADOS NYC TAXI")
            print("="*60)
            
            print(f"\n📈 Visão Geral do Dataset:")
            print(f"   • Total de registros: {self.df.count():,}")
            print(f"   • Total de colunas: {len(self.df.columns)}")
            
            print(f"\n📋 Schema da Tabela:")
            self.df.printSchema()
            
            return True
            
        except Exception as e:
            print(f"❌ Erro ao carregar dados: {e}")
            return False
    
    def analyze_data_distribution(self):
        """Analisa distribuição dos dados"""
        print("\n" + "="*60)
        print("📊 DISTRIBUIÇÃO DOS DADOS")
        print("="*60)
        
        # Distribuição por tipo de táxi
        print("\n🚕 Distribuição por Tipo de Táxi:")
        taxi_dist = (
            self.df
            .groupBy("taxi_type")
            .agg(
                count("*").alias("total_trips"),
                avg("total_amount").alias("avg_fare"),
                spark_sum("total_amount").alias("total_revenue")
            )
            .orderBy("total_trips", ascending=False)
        )
        taxi_dist.show(truncate=False)
        
        # Distribuição por mês
        print("\n📅 Distribuição por Mês:")
        monthly_dist = (
            self.df
            .groupBy("year_month")
            .agg(
                count("*").alias("total_trips"),
                avg("total_amount").alias("avg_fare")
            )
            .orderBy("year_month")
        )
        monthly_dist.show(truncate=False)
        
        # Distribuição por dia da semana
        print("\n📆 Distribuição por Dia da Semana:")
        weekday_dist = (
            self.df
            .filter(col("tpep_pickup_datetime").isNotNull())
            .withColumn("day_of_week", dayofweek(col("tpep_pickup_datetime")))
            .withColumn("day_name", 
                when(col("day_of_week") == 1, "Domingo")
                .when(col("day_of_week") == 2, "Segunda")
                .when(col("day_of_week") == 3, "Terça")
                .when(col("day_of_week") == 4, "Quarta")
                .when(col("day_of_week") == 5, "Quinta")
                .when(col("day_of_week") == 6, "Sexta")
                .when(col("day_of_week") == 7, "Sábado")
            )
            .groupBy("day_of_week", "day_name")
            .agg(
                count("*").alias("total_trips"),
                avg("total_amount").alias("avg_fare")
            )
            .orderBy("day_of_week")
        )
        weekday_dist.show(truncate=False)
    
    def analyze_fare_patterns(self):
        """Analisa padrões de tarifas"""
        print("\n" + "="*60)
        print("💰 ANÁLISE DE TARIFAS")
        print("="*60)
        
        # Estatísticas descritivas de tarifas
        print("\n📊 Estatísticas de Total Amount:")
        fare_stats = (
            self.df
            .filter(col("total_amount").isNotNull() & (col("total_amount") > 0))
            .agg(
                spark_min("total_amount").alias("min_fare"),
                percentile_approx("total_amount", 0.25).alias("q1_fare"),
                percentile_approx("total_amount", 0.5).alias("median_fare"),
                avg("total_amount").alias("mean_fare"),
                percentile_approx("total_amount", 0.75).alias("q3_fare"),
                spark_max("total_amount").alias("max_fare"),
                stddev("total_amount").alias("std_fare")
            )
        )
        fare_stats.show(truncate=False)
        
        # Distribuição de tarifas por faixas
        print("\n💵 Distribuição por Faixas de Tarifa:")
        fare_ranges = (
            self.df
            .filter(col("total_amount").isNotNull() & (col("total_amount") > 0))
            .withColumn("fare_range",
                when(col("total_amount") <= 10, "$0-10")
                .when(col("total_amount") <= 20, "$10-20")
                .when(col("total_amount") <= 30, "$20-30")
                .when(col("total_amount") <= 50, "$30-50")
                .when(col("total_amount") <= 100, "$50-100")
                .otherwise("$100+")
            )
            .groupBy("fare_range")
            .agg(
                count("*").alias("trip_count"),
                (count("*") * 100.0 / self.df.count()).alias("percentage")
            )
            .orderBy("trip_count", ascending=False)
        )
        fare_ranges.show(truncate=False)
        
        # Top 10 tarifas mais altas
        print("\n🔝 Top 10 Tarifas Mais Altas:")
        top_fares = (
            self.df
            .filter(col("total_amount").isNotNull())
            .select("taxi_type", "total_amount", "passenger_count", "year_month")
            .orderBy(desc("total_amount"))
            .limit(10)
        )
        top_fares.show(truncate=False)
    
    def analyze_passenger_patterns(self):
        """Analisa padrões de passageiros"""
        print("\n" + "="*60)
        print("👥 ANÁLISE DE PASSAGEIROS")
        print("="*60)
        
        # Distribuição de número de passageiros
        print("\n📊 Distribuição por Número de Passageiros:")
        passenger_dist = (
            self.df
            .filter(col("passenger_count").isNotNull() & (col("passenger_count") > 0))
            .groupBy("passenger_count")
            .agg(
                count("*").alias("trip_count"),
                avg("total_amount").alias("avg_fare")
            )
            .orderBy("passenger_count")
        )
        passenger_dist.show(truncate=False)
        
        # Passageiros por tipo de táxi
        print("\n🚕 Passageiros por Tipo de Táxi:")
        passenger_by_type = (
            self.df
            .filter(col("passenger_count").isNotNull() & (col("passenger_count") > 0))
            .groupBy("taxi_type")
            .agg(
                avg("passenger_count").alias("avg_passengers"),
                spark_sum("passenger_count").alias("total_passengers"),
                count("*").alias("total_trips")
            )
        )
        passenger_by_type.show(truncate=False)
    
    def analyze_temporal_patterns(self):
        """Analisa padrões temporais"""
        print("\n" + "="*60)
        print("⏰ ANÁLISE TEMPORAL")
        print("="*60)
        
        # Padrão por hora do dia
        print("\n🕐 Distribuição por Hora do Dia:")
        hourly_pattern = (
            self.df
            .filter(col("tpep_pickup_datetime").isNotNull())
            .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
            .groupBy("pickup_hour")
            .agg(
                count("*").alias("trip_count"),
                avg("total_amount").alias("avg_fare"),
                avg("passenger_count").alias("avg_passengers")
            )
            .orderBy("pickup_hour")
        )
        hourly_pattern.show(24, truncate=False)
        
        # Identificar picos de demanda
        print("\n📈 Análise de Picos de Demanda:")
        peak_hours = (
            hourly_pattern
            .orderBy(desc("trip_count"))
            .limit(5)
        )
        print("Top 5 horas com mais viagens:")
        peak_hours.show(truncate=False)
        
        # Períodos do dia
        print("\n🌅 Análise por Período do Dia:")
        period_analysis = (
            self.df
            .filter(col("tpep_pickup_datetime").isNotNull())
            .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
            .withColumn("period",
                when(col("pickup_hour").between(6, 11), "Manhã")
                .when(col("pickup_hour").between(12, 17), "Tarde")
                .when(col("pickup_hour").between(18, 23), "Noite")
                .otherwise("Madrugada")
            )
            .groupBy("period")
            .agg(
                count("*").alias("trip_count"),
                avg("total_amount").alias("avg_fare"),
                avg("passenger_count").alias("avg_passengers")
            )
            .orderBy(desc("trip_count"))
        )
        period_analysis.show(truncate=False)
    
    def analyze_data_quality_issues(self):
        """Analisa problemas de qualidade dos dados"""
        print("\n" + "="*60)
        print("🔍 ANÁLISE DE QUALIDADE DOS DADOS")
        print("="*60)
        
        total_records = self.df.count()
        
        # Valores nulos por coluna
        print("\n❌ Análise de Valores Nulos:")
        null_analysis = []
        
        for col_name in self.df.columns:
            null_count = self.df.filter(col(col_name).isNull()).count()
            null_percentage = (null_count / total_records) * 100
            null_analysis.append((col_name, null_count, null_percentage))
        
        # Ordena por percentual de nulos
        null_analysis.sort(key=lambda x: x[2], reverse=True)
        
        for col_name, null_count, null_percentage in null_analysis:
            if null_count > 0:
                print(f"   • {col_name}: {null_count:,} nulos ({null_percentage:.2f}%)")
        
        # Valores extremos
        print("\n⚠️  Análise de Valores Extremos:")
        
        # Tarifas negativas ou zero
        negative_fares = self.df.filter(col("total_amount") <= 0).count()
        print(f"   • Tarifas <= 0: {negative_fares:,} registros")
        
        # Passageiros zero
        zero_passengers = self.df.filter(col("passenger_count") == 0).count()
        print(f"   • Viagens com 0 passageiros: {zero_passengers:,} registros")
        
        # Viagens muito longas (mais de 24 horas)
        if "tpep_pickup_datetime" in self.df.columns and "tpep_dropoff_datetime" in self.df.columns:
            long_trips = (
                self.df
                .filter(col("tpep_pickup_datetime").isNotNull() & col("tpep_dropoff_datetime").isNotNull())
                .filter((col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")) > 86400)
                .count()
            )
            print(f"   • Viagens > 24 horas: {long_trips:,} registros")
    
    def generate_summary_insights(self):
        """Gera insights resumidos"""
        print("\n" + "="*60)
        print("💡 INSIGHTS PRINCIPAIS")
        print("="*60)
        
        try:
            # Insight 1: Tipo de táxi mais popular
            most_popular_taxi = (
                self.df
                .groupBy("taxi_type")
                .count()
                .orderBy(desc("count"))
                .first()
            )
            
            print(f"\n🚕 Tipo de táxi mais utilizado: {most_popular_taxi['taxi_type']} ({most_popular_taxi['count']:,} viagens)")
            
            # Insight 2: Mês com mais viagens
            busiest_month = (
                self.df
                .groupBy("year_month")
                .count()
                .orderBy(desc("count"))
                .first()
            )
            
            print(f"📅 Mês mais movimentado: {busiest_month['year_month']} ({busiest_month['count']:,} viagens)")
            
            # Insight 3: Tarifa média geral
            avg_fare = (
                self.df
                .filter(col("total_amount") > 0)
                .agg(avg("total_amount"))
                .collect()[0][0]
            )
            
            print(f"💰 Tarifa média geral: ${avg_fare:.2f}")
            
            # Insight 4: Número médio de passageiros
            avg_passengers = (
                self.df
                .filter(col("passenger_count") > 0)
                .agg(avg("passenger_count"))
                .collect()[0][0]
            )
            
            print(f"👥 Número médio de passageiros: {avg_passengers:.2f}")
            
        except Exception as e:
            print(f"❌ Erro ao gerar insights: {e}")
    
    def run_complete_analysis(self):
        """Executa análise exploratória completa"""
        print("🚀 INICIANDO ANÁLISE EXPLORATÓRIA COMPLETA")
        
        # Carrega dados e visão geral
        if not self.load_and_overview():
            return False
        
        # Executa todas as análises
        self.analyze_data_distribution()
        self.analyze_fare_patterns()
        self.analyze_passenger_patterns()
        self.analyze_temporal_patterns()
        self.analyze_data_quality_issues()
        self.generate_summary_insights()
        
        print("\n" + "="*60)
        print("✅ ANÁLISE EXPLORATÓRIA CONCLUÍDA!")
        print("="*60)
        
        return True


def main():
    """Função principal"""
    spark = SparkSession.builder.appName("Exploratory_Analysis").getOrCreate()
    
    analyzer = ExploratoryAnalyzer(spark)
    success = analyzer.run_complete_analysis()
    
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
