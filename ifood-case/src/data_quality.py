"""
Módulo para validação e qualidade de dados
Implementa verificações de integridade e limpeza dos dados
"""

from typing import Dict, List, Tuple, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull, sum as spark_sum, avg, min as spark_min, max as spark_max
from pyspark.sql.types import *

from config import DATA_QUALITY_RULES, REQUIRED_COLUMNS, get_full_table_name


class DataQualityValidator:
    """
    Classe para validação de qualidade dos dados
    """
    
    def __init__(self, spark: SparkSession):
        """
        Inicializa o validador
        
        Args:
            spark: Sessão do Spark
        """
        self.spark = spark
        self.quality_issues = []
        
    def check_required_columns(self, df: DataFrame) -> Dict[str, Any]:
        """
        Verifica se todas as colunas obrigatórias estão presentes
        
        Args:
            df: DataFrame para validar
            
        Returns:
            Dicionário com resultado da validação
        """
        missing_columns = []
        present_columns = df.columns
        
        for col_name in REQUIRED_COLUMNS:
            if col_name not in present_columns:
                missing_columns.append(col_name)
        
        result = {
            "check": "required_columns",
            "status": "PASS" if not missing_columns else "FAIL",
            "missing_columns": missing_columns,
            "present_columns": present_columns
        }
        
        if missing_columns:
            self.quality_issues.append(f"Colunas obrigatórias ausentes: {missing_columns}")
            
        return result
    
    def check_null_values(self, df: DataFrame) -> Dict[str, Any]:
        """
        Verifica valores nulos nas colunas importantes
        
        Args:
            df: DataFrame para validar
            
        Returns:
            Dicionário com estatísticas de valores nulos
        """
        total_rows = df.count()
        null_stats = {}
        
        for col_name in REQUIRED_COLUMNS:
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
                
                null_stats[col_name] = {
                    "null_count": null_count,
                    "null_percentage": round(null_percentage, 2)
                }
                
                # Alerta se mais de 10% dos valores são nulos
                if null_percentage > 10:
                    self.quality_issues.append(
                        f"Coluna {col_name} tem {null_percentage:.2f}% de valores nulos"
                    )
        
        return {
            "check": "null_values",
            "total_rows": total_rows,
            "null_statistics": null_stats
        }
    
    def check_data_ranges(self, df: DataFrame) -> Dict[str, Any]:
        """
        Verifica se os valores estão dentro dos ranges esperados
        
        Args:
            df: DataFrame para validar
            
        Returns:
            Dicionário com resultado das validações de range
        """
        range_violations = {}
        
        for col_name, rules in DATA_QUALITY_RULES.items():
            if col_name in df.columns:
                # Conta valores fora do range
                min_val = rules.get("min")
                max_val = rules.get("max")
                
                violations = 0
                if min_val is not None:
                    violations += df.filter(col(col_name) < min_val).count()
                if max_val is not None:
                    violations += df.filter(col(col_name) > max_val).count()
                
                if violations > 0:
                    range_violations[col_name] = {
                        "violations": violations,
                        "expected_range": f"{min_val} - {max_val}"
                    }
                    self.quality_issues.append(
                        f"Coluna {col_name} tem {violations} valores fora do range {min_val}-{max_val}"
                    )
        
        return {
            "check": "data_ranges",
            "violations": range_violations
        }
    
    def check_datetime_validity(self, df: DataFrame) -> Dict[str, Any]:
        """
        Verifica validade das colunas de datetime
        
        Args:
            df: DataFrame para validar
            
        Returns:
            Dicionário com resultado da validação de datetime
        """
        datetime_issues = {}
        datetime_columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
        
        for col_name in datetime_columns:
            if col_name in df.columns:
                # Verifica valores nulos
                null_count = df.filter(col(col_name).isNull()).count()
                
                # Verifica se pickup é antes de dropoff
                if all(c in df.columns for c in datetime_columns):
                    invalid_trips = df.filter(
                        col("tpep_pickup_datetime") > col("tpep_dropoff_datetime")
                    ).count()
                    
                    if invalid_trips > 0:
                        datetime_issues["invalid_trip_duration"] = invalid_trips
                        self.quality_issues.append(
                            f"{invalid_trips} viagens com pickup após dropoff"
                        )
                
                if null_count > 0:
                    datetime_issues[f"{col_name}_nulls"] = null_count
        
        return {
            "check": "datetime_validity",
            "issues": datetime_issues
        }
    
    def generate_data_profile(self, df: DataFrame) -> Dict[str, Any]:
        """
        Gera perfil estatístico dos dados
        
        Args:
            df: DataFrame para perfilar
            
        Returns:
            Dicionário com estatísticas descritivas
        """
        profile = {
            "total_rows": df.count(),
            "total_columns": len(df.columns),
            "columns": df.columns
        }
        
        # Estatísticas para colunas numéricas
        numeric_columns = ["passenger_count", "total_amount", "trip_distance", "fare_amount"]
        
        for col_name in numeric_columns:
            if col_name in df.columns:
                stats = df.select(
                    spark_min(col(col_name)).alias("min"),
                    spark_max(col(col_name)).alias("max"),
                    avg(col(col_name)).alias("avg"),
                    count(col(col_name)).alias("count")
                ).collect()[0]
                
                profile[f"{col_name}_stats"] = {
                    "min": stats["min"],
                    "max": stats["max"], 
                    "avg": round(stats["avg"], 2) if stats["avg"] else None,
                    "count": stats["count"]
                }
        
        return profile
    
    def run_full_validation(self, df: DataFrame) -> Dict[str, Any]:
        """
        Executa todas as validações de qualidade
        
        Args:
            df: DataFrame para validar
            
        Returns:
            Relatório completo de qualidade
        """
        print("🔍 Iniciando validação de qualidade dos dados...")
        
        # Limpa issues anteriores
        self.quality_issues = []
        
        # Executa todas as verificações
        results = {
            "data_profile": self.generate_data_profile(df),
            "required_columns": self.check_required_columns(df),
            "null_values": self.check_null_values(df),
            "data_ranges": self.check_data_ranges(df),
            "datetime_validity": self.check_datetime_validity(df)
        }
        
        # Adiciona resumo de issues
        results["quality_summary"] = {
            "total_issues": len(self.quality_issues),
            "issues": self.quality_issues,
            "overall_status": "PASS" if len(self.quality_issues) == 0 else "WARNING"
        }
        
        return results
    
    def print_quality_report(self, validation_results: Dict[str, Any]):
        """
        Imprime relatório de qualidade formatado
        
        Args:
            validation_results: Resultados da validação
        """
        print("\n" + "="*60)
        print("📊 RELATÓRIO DE QUALIDADE DOS DADOS")
        print("="*60)
        
        # Perfil dos dados
        profile = validation_results["data_profile"]
        print(f"\n📈 Perfil dos Dados:")
        print(f"   • Total de registros: {profile['total_rows']:,}")
        print(f"   • Total de colunas: {profile['total_columns']}")
        
        # Estatísticas numéricas
        print(f"\n📊 Estatísticas das Colunas Principais:")
        for key, value in profile.items():
            if key.endswith("_stats") and isinstance(value, dict):
                col_name = key.replace("_stats", "")
                print(f"   • {col_name}:")
                print(f"     - Min: {value['min']}")
                print(f"     - Max: {value['max']}")
                print(f"     - Média: {value['avg']}")
                print(f"     - Registros válidos: {value['count']:,}")
        
        # Valores nulos
        null_stats = validation_results["null_values"]["null_statistics"]
        print(f"\n🔍 Análise de Valores Nulos:")
        for col_name, stats in null_stats.items():
            if stats["null_count"] > 0:
                print(f"   • {col_name}: {stats['null_count']:,} nulos ({stats['null_percentage']}%)")
        
        # Issues de qualidade
        summary = validation_results["quality_summary"]
        print(f"\n⚠️  Resumo de Qualidade:")
        print(f"   • Status geral: {summary['overall_status']}")
        print(f"   • Total de issues: {summary['total_issues']}")
        
        if summary["issues"]:
            print(f"\n🚨 Issues Identificados:")
            for i, issue in enumerate(summary["issues"], 1):
                print(f"   {i}. {issue}")
        else:
            print(f"\n✅ Nenhum issue de qualidade identificado!")


def validate_table_quality(spark: SparkSession) -> Dict[str, Any]:
    """
    Função principal para validar qualidade da tabela Delta
    
    Args:
        spark: Sessão do Spark
        
    Returns:
        Resultados da validação
    """
    try:
        table_name = get_full_table_name()
        df = spark.table(table_name)
        
        validator = DataQualityValidator(spark)
        results = validator.run_full_validation(df)
        validator.print_quality_report(results)
        
        return results
        
    except Exception as e:
        print(f"❌ Erro na validação de qualidade: {e}")
        return {"error": str(e)}


if __name__ == "__main__":
    # Para execução standalone
    spark = SparkSession.builder.appName("Data_Quality_Validation").getOrCreate()
    validate_table_quality(spark)
