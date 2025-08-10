"""
Pipeline principal V2 - Arquitetura Python + PySpark + SQL
Orquestra: Extração Python → Consolidação PySpark → Análises SQL
"""

import time
from pyspark.sql import SparkSession

from data_extraction import NYCTaxiDataExtractor
from data_consolidation import DataLakeConsolidator
from config import get_table_name


class NYCTaxiPipelineV2:
    """
    Pipeline principal V2 com arquitetura em camadas
    """
    
    def __init__(self):
        """Inicializa o pipeline"""
        self.spark = SparkSession.builder \
            .appName("NYC_Taxi_Pipeline_V2") \
            .getOrCreate()
        
        self.start_time = time.time()
        self.extraction_results = None
        self.consolidation_success = False
        
    def run_complete_pipeline(self) -> bool:
        """
        Executa o pipeline completo V2
        
        Returns:
            True se executou com sucesso
        """
        print("🚀 INICIANDO PIPELINE V2 - NYC TAXI DATA")
        print("Arquitetura: Python → PySpark → SQL")
        print("="*60)
        
        try:
            # Etapa 1: Extração via Python
            print("\n📥 ETAPA 1: EXTRAÇÃO DE DADOS (PYTHON)")
            print("-" * 50)
            
            extractor = NYCTaxiDataExtractor()
            self.extraction_results = extractor.extract_all_data()
            
            if self.extraction_results['success_count'] == 0:
                print("❌ Falha na extração dos dados. Pipeline interrompido.")
                return False
            
            print(f"✅ Extração concluída: {self.extraction_results['success_count']} arquivos")
            
            # Etapa 2: Consolidação via PySpark
            print("\n🔄 ETAPA 2: CONSOLIDAÇÃO DATA LAKE (PYSPARK)")
            print("-" * 50)
            
            consolidator = DataLakeConsolidator(self.spark)
            self.consolidation_success = consolidator.run_full_consolidation(
                self.extraction_results['successful_files']
            )
            
            if not self.consolidation_success:
                print("❌ Falha na consolidação. Pipeline interrompido.")
                return False
            
            print("✅ Consolidação concluída: Camadas Raw → Bronze → Silver → Gold")
            
            # Etapa 3: Preparação para análises SQL
            print("\n📊 ETAPA 3: PREPARAÇÃO PARA ANÁLISES (SQL)")
            print("-" * 50)
            
            self.prepare_sql_analysis()
            
            # Etapa 4: Relatório final
            print("\n📋 ETAPA 4: RELATÓRIO FINAL")
            print("-" * 50)
            
            self.generate_final_report()
            
            return True
            
        except Exception as e:
            print(f"💥 Erro crítico no pipeline: {e}")
            return False
        
        finally:
            self.cleanup()
    
    def prepare_sql_analysis(self):
        """
        Prepara ambiente para análises SQL
        """
        try:
            print("🔧 Preparando ambiente para análises SQL...")
            
            # Verifica se todas as tabelas foram criadas
            tables_to_check = ["raw", "bronze", "silver", "gold"]
            available_tables = []
            
            for layer in tables_to_check:
                table_name = get_table_name(layer)
                try:
                    count = self.spark.table(table_name).count()
                    available_tables.append(layer)
                    print(f"  ✅ {table_name}: {count:,} registros")
                except Exception as e:
                    print(f"  ❌ {table_name}: Não disponível ({e})")
            
            if "gold" in available_tables:
                print("✅ Camada Gold disponível - Pronto para análises SQL!")
                
                # Mostra exemplo de consulta
                print("\n📝 Exemplo de consulta SQL:")
                print("```sql")
                print("SELECT taxi_type, year_month, ")
                print("       SUM(total_trips) as viagens,")
                print("       ROUND(SUM(sum_total_amount)/SUM(total_trips), 2) as tarifa_media")
                print("FROM main.nyc_taxi.gold_trips")
                print("GROUP BY taxi_type, year_month")
                print("ORDER BY taxi_type, year_month;")
                print("```")
                
            else:
                print("⚠️  Camada Gold não disponível - Análises limitadas")
            
        except Exception as e:
            print(f"❌ Erro na preparação SQL: {e}")
    
    def run_sql_business_questions(self):
        """
        Executa as perguntas de negócio em SQL
        """
        try:
            print("\n📊 EXECUTANDO PERGUNTAS DE NEGÓCIO (SQL)")
            print("="*50)
            
            gold_table = get_table_name("gold")
            
            # Pergunta 1: Média Yellow Taxis
            print("\n🎯 PERGUNTA 1: Média de valor total Yellow Taxis")
            print("-" * 50)
            
            query1 = f"""
            SELECT 
                ROUND(SUM(sum_total_amount) / SUM(total_trips), 2) as media_valor_total,
                SUM(total_trips) as total_viagens,
                ROUND(SUM(sum_total_amount), 2) as receita_total
            FROM {gold_table}
            WHERE taxi_type = 'yellow'
            """
            
            result1 = self.spark.sql(query1)
            print("💰 RESPOSTA:")
            result1.show(truncate=False)
            
            # Detalhamento por mês
            query1_detail = f"""
            SELECT 
                year_month,
                ROUND(AVG(avg_total_amount), 2) as media_valor,
                SUM(total_trips) as viagens
            FROM {gold_table}
            WHERE taxi_type = 'yellow'
            GROUP BY year_month
            ORDER BY year_month
            """
            
            result1_detail = self.spark.sql(query1_detail)
            print("\n📅 Detalhamento por mês:")
            result1_detail.show(truncate=False)
            
            # Pergunta 2: Passageiros por hora em Maio
            print("\n🎯 PERGUNTA 2: Passageiros por hora em Maio")
            print("-" * 50)
            
            query2 = f"""
            SELECT 
                pickup_hour as hora,
                ROUND(SUM(sum_passenger_count) / SUM(total_trips), 2) as media_passageiros,
                SUM(total_trips) as viagens
            FROM {gold_table}
            WHERE pickup_month = 5 AND pickup_year = 2023
            GROUP BY pickup_hour
            ORDER BY pickup_hour
            """
            
            result2 = self.spark.sql(query2)
            print("👥 RESPOSTA:")
            result2.show(24, truncate=False)
            
            # Identifica picos
            query2_peaks = f"""
            SELECT 
                pickup_hour as hora,
                ROUND(SUM(sum_passenger_count) / SUM(total_trips), 2) as media_passageiros,
                SUM(total_trips) as viagens
            FROM {gold_table}
            WHERE pickup_month = 5 AND pickup_year = 2023
            GROUP BY pickup_hour
            ORDER BY media_passageiros DESC
            LIMIT 3
            """
            
            result2_peaks = self.spark.sql(query2_peaks)
            print("\n🔥 Top 3 horas com mais passageiros:")
            result2_peaks.show(truncate=False)
            
        except Exception as e:
            print(f"❌ Erro nas consultas SQL: {e}")
    
    def generate_final_report(self):
        """
        Gera relatório final do pipeline V2
        """
        end_time = time.time()
        duration = end_time - self.start_time
        
        print("\n" + "="*60)
        print("📋 RELATÓRIO FINAL DO PIPELINE V2")
        print("="*60)
        
        # Informações de tempo
        print(f"\n⏱️  Tempo de Execução:")
        print(f"   • Duração total: {duration:.2f} segundos ({duration/60:.2f} minutos)")
        
        # Informações de extração
        if self.extraction_results:
            print(f"\n📥 Extração de Dados (Python):")
            print(f"   • Arquivos extraídos: {self.extraction_results['success_count']}")
            print(f"   • Arquivos com falha: {self.extraction_results['failure_count']}")
            print(f"   • Taxa de sucesso: {self.extraction_results['success_count']/(self.extraction_results['success_count']+self.extraction_results['failure_count'])*100:.1f}%")
        
        # Informações de consolidação
        print(f"\n🔄 Consolidação Data Lake (PySpark):")
        print(f"   • Status: {'✅ Sucesso' if self.consolidation_success else '❌ Falha'}")
        
        if self.consolidation_success:
            try:
                # Conta registros em cada camada
                layers = ["raw", "bronze", "silver", "gold"]
                print(f"   • Camadas criadas:")
                
                for layer in layers:
                    table_name = get_table_name(layer)
                    try:
                        count = self.spark.table(table_name).count()
                        print(f"     - {layer.upper()}: {count:,} registros")
                    except:
                        print(f"     - {layer.upper()}: Erro ao contar")
                        
            except Exception as e:
                print(f"   • Erro ao gerar estatísticas: {e}")
        
        # Informações de análises
        print(f"\n📊 Análises SQL:")
        print(f"   • Arquivo SQL: sql/business_questions.sql")
        print(f"   • Tabela principal: {get_table_name('gold')}")
        print(f"   • Perguntas implementadas: 2 principais + análises complementares")
        
        # Arquitetura implementada
        print(f"\n🏗️  Arquitetura Implementada:")
        print(f"   • Extração: Python (requests + DBFS)")
        print(f"   • Processamento: PySpark (4 camadas)")
        print(f"   • Análises: SQL (consultas otimizadas)")
        print(f"   • Armazenamento: Delta Lake (particionado)")
        
        # Próximos passos
        print(f"\n🎯 Próximos Passos:")
        print(f"   1. Execute: %run sql/business_questions.sql")
        print(f"   2. Explore: SELECT * FROM {get_table_name('gold')} LIMIT 10")
        print(f"   3. Analise: Consultas personalizadas na camada Gold")
        print(f"   4. Visualize: Crie dashboards com os dados")
        
        print(f"\n✅ Pipeline V2 executado com sucesso!")
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
    """Função principal para execução do pipeline V2"""
    pipeline = NYCTaxiPipelineV2()
    success = pipeline.run_complete_pipeline()
    
    if success:
        print("\n🎉 Pipeline V2 concluído com sucesso!")
        
        # Executa análises SQL se solicitado
        try:
            pipeline.run_sql_business_questions()
        except Exception as e:
            print(f"⚠️  Análises SQL não executadas: {e}")
            print("💡 Execute manualmente: %run sql/business_questions.sql")
        
        return 0
    else:
        print("\n💥 Pipeline V2 falhou!")
        return 1


if __name__ == "__main__":
    exit(main())
