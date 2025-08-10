"""
Pipeline principal para execução completa do projeto
Orquestra ingestão, validação e preparação dos dados
"""

import time
from pyspark.sql import SparkSession

from data_ingestion import NYCTaxiDataIngestion
from data_quality import DataQualityValidator, validate_table_quality
from config import get_full_table_name


class NYCTaxiPipeline:
    """
    Pipeline principal para processamento completo dos dados de táxi NYC
    """
    
    def __init__(self):
        """Inicializa o pipeline"""
        self.spark = SparkSession.builder \
            .appName("NYC_Taxi_Complete_Pipeline") \
            .getOrCreate()
        
        self.start_time = time.time()
        
    def run_complete_pipeline(self) -> bool:
        """
        Executa o pipeline completo
        
        Returns:
            True se executou com sucesso
        """
        print("🚀 INICIANDO PIPELINE COMPLETO - NYC TAXI DATA")
        print("="*60)
        
        try:
            # Etapa 1: Ingestão dos dados
            print("\n📥 ETAPA 1: INGESTÃO DOS DADOS")
            print("-" * 40)
            
            ingestion = NYCTaxiDataIngestion(self.spark)
            ingestion_success = ingestion.process_all_data()
            
            if not ingestion_success:
                print("❌ Falha na ingestão dos dados. Pipeline interrompido.")
                return False
            
            # Etapa 2: Validação de qualidade
            print("\n🔍 ETAPA 2: VALIDAÇÃO DE QUALIDADE")
            print("-" * 40)
            
            validation_results = validate_table_quality(self.spark)
            
            if "error" in validation_results:
                print("⚠️  Problemas na validação, mas continuando...")
            
            # Etapa 3: Otimização da tabela
            print("\n⚡ ETAPA 3: OTIMIZAÇÃO DA TABELA")
            print("-" * 40)
            
            optimization_success = self.optimize_table()
            
            # Etapa 4: Relatório final
            print("\n📊 ETAPA 4: RELATÓRIO FINAL")
            print("-" * 40)
            
            self.generate_final_report(ingestion, validation_results)
            
            return True
            
        except Exception as e:
            print(f"💥 Erro crítico no pipeline: {e}")
            return False
        
        finally:
            self.cleanup()
    
    def optimize_table(self) -> bool:
        """
        Otimiza a tabela Delta criada
        
        Returns:
            True se otimizou com sucesso
        """
        try:
            table_name = get_full_table_name()
            
            print(f"🔧 Otimizando tabela {table_name}...")
            
            # OPTIMIZE para compactação de arquivos
            self.spark.sql(f"OPTIMIZE {table_name}")
            print("✅ Compactação de arquivos concluída")
            
            # Z-ORDER para otimizar consultas por datetime e taxi_type
            try:
                self.spark.sql(f"OPTIMIZE {table_name} ZORDER BY (tpep_pickup_datetime, taxi_type)")
                print("✅ Z-ORDER aplicado para otimizar consultas")
            except Exception as e:
                print(f"⚠️  Z-ORDER não aplicado: {e}")
            
            # ANALYZE TABLE para estatísticas
            try:
                self.spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
                print("✅ Estatísticas da tabela atualizadas")
            except Exception as e:
                print(f"⚠️  Estatísticas não atualizadas: {e}")
            
            return True
            
        except Exception as e:
            print(f"❌ Erro na otimização: {e}")
            return False
    
    def generate_final_report(self, ingestion: NYCTaxiDataIngestion, validation_results: dict):
        """
        Gera relatório final do pipeline
        
        Args:
            ingestion: Instância da classe de ingestão
            validation_results: Resultados da validação
        """
        end_time = time.time()
        duration = end_time - self.start_time
        
        print("\n" + "="*60)
        print("📋 RELATÓRIO FINAL DO PIPELINE")
        print("="*60)
        
        # Informações de tempo
        print(f"\n⏱️  Tempo de Execução:")
        print(f"   • Duração total: {duration:.2f} segundos ({duration/60:.2f} minutos)")
        
        # Informações de ingestão
        print(f"\n📥 Ingestão de Dados:")
        print(f"   • Arquivos processados com sucesso: {ingestion.successful_loads}")
        print(f"   • Arquivos com falha: {ingestion.failed_loads}")
        print(f"   • Total de registros ingeridos: {ingestion.total_records:,}")
        
        # Informações de qualidade
        if "quality_summary" in validation_results:
            quality = validation_results["quality_summary"]
            print(f"\n🔍 Qualidade dos Dados:")
            print(f"   • Status geral: {quality['overall_status']}")
            print(f"   • Issues identificados: {quality['total_issues']}")
        
        # Informações da tabela final
        try:
            table_name = get_full_table_name()
            df = self.spark.table(table_name)
            final_count = df.count()
            
            print(f"\n📊 Tabela Final:")
            print(f"   • Nome: {table_name}")
            print(f"   • Total de registros: {final_count:,}")
            print(f"   • Colunas: {len(df.columns)}")
            
            # Distribuição por tipo e mês
            print(f"\n📈 Distribuição dos Dados:")
            print("   Por tipo de táxi:")
            type_dist = df.groupBy("taxi_type").count().collect()
            for row in type_dist:
                print(f"     - {row['taxi_type']}: {row['count']:,} registros")
            
            print("   Por mês:")
            month_dist = df.groupBy("year_month").count().orderBy("year_month").collect()
            for row in month_dist:
                print(f"     - {row['year_month']}: {row['count']:,} registros")
                
        except Exception as e:
            print(f"⚠️  Erro ao gerar estatísticas finais: {e}")
        
        # Próximos passos
        print(f"\n🎯 Próximos Passos:")
        print(f"   1. Execute as análises em 'analysis/business_questions.py'")
        print(f"   2. Explore os dados com 'analysis/exploratory_analysis.py'")
        print(f"   3. Consulte a tabela: {get_full_table_name()}")
        
        print(f"\n✅ Pipeline executado com sucesso!")
        print("="*60)
    
    def cleanup(self):
        """Limpeza final do pipeline"""
        try:
            # Limpa cache do Spark
            self.spark.catalog.clearCache()
            print("🧹 Cache do Spark limpo")
        except Exception as e:
            print(f"⚠️  Erro na limpeza: {e}")


def main():
    """Função principal para execução do pipeline"""
    pipeline = NYCTaxiPipeline()
    success = pipeline.run_complete_pipeline()
    
    if success:
        print("\n🎉 Pipeline concluído com sucesso!")
        return 0
    else:
        print("\n💥 Pipeline falhou!")
        return 1


if __name__ == "__main__":
    exit(main())
