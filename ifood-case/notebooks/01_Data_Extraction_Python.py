# Databricks notebook source
# MAGIC %md
# MAGIC # Case Técnico iFood - Data Architect
# MAGIC ## Notebook 1: Extração de Dados (Python)
# MAGIC 
# MAGIC Este notebook implementa a **primeira etapa** da nova arquitetura:
# MAGIC 
# MAGIC ### 🏗️ Arquitetura V2:
# MAGIC 1. **Python**: Extração dos dados (este notebook)
# MAGIC 2. **PySpark**: Consolidação no Delta Lake (próximo notebook)
# MAGIC 3. **SQL**: Respostas finais do desafio (último notebook)
# MAGIC 
# MAGIC ### 📥 Responsabilidades deste Notebook:
# MAGIC - Download dos arquivos Parquet via Python
# MAGIC - Upload para DBFS (Databricks File System)
# MAGIC - Validação dos arquivos baixados

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração e Imports

# COMMAND ----------

import requests
import os
import time
from typing import Dict, List, Tuple

# Configurações
LOCAL_DATA_DIR = "/tmp/nyc_taxi_data"
DBFS_RAW_DIR = "/FileStore/nyc_taxi/raw"

# URLs dos dados NYC Taxi (Janeiro a Maio 2023)
DATA_SOURCES = {
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

print("🚀 EXTRAÇÃO DE DADOS NYC TAXI - PYTHON")
print("="*50)
print(f"📊 Total de arquivos: {sum(len(months) for months in DATA_SOURCES.values())}")
print(f"📁 Diretório local: {LOCAL_DATA_DIR}")
print(f"📁 Diretório DBFS: {DBFS_RAW_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Funções de Download

# COMMAND ----------

def download_file_python(url: str, local_path: str, max_retries: int = 3) -> bool:
    """
    Download de arquivo via Python requests
    """
    for attempt in range(max_retries):
        try:
            filename = os.path.basename(local_path)
            print(f"  📥 Download tentativa {attempt + 1}: {filename}")
            
            # Headers para simular browser
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
            
            # Download com streaming e timeout
            response = requests.get(
                url, 
                headers=headers, 
                stream=True, 
                timeout=600,  # 10 minutos
                verify=True
            )
            response.raise_for_status()
            
            # Salva arquivo em chunks
            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        
                        # Progress a cada 50MB
                        if total_size > 0 and downloaded_size % (50 * 1024 * 1024) == 0:
                            progress = (downloaded_size / total_size * 100)
                            print(f"    📊 Progresso: {progress:.1f}%")
            
            # Verifica tamanho final
            file_size = os.path.getsize(local_path)
            if file_size == 0:
                raise ValueError("Arquivo baixado está vazio")
            
            print(f"    ✅ Download concluído: {file_size / (1024*1024):.1f} MB")
            return True
            
        except Exception as e:
            print(f"    ❌ Erro: {str(e)[:150]}...")
            
            # Remove arquivo parcial
            try:
                if os.path.exists(local_path):
                    os.remove(local_path)
            except:
                pass
            
            if attempt < max_retries - 1:
                delay = 10 * (2 ** attempt)  # Backoff exponencial
                print(f"    ⏳ Aguardando {delay} segundos...")
                time.sleep(delay)
            else:
                print(f"    💥 Falha definitiva após {max_retries} tentativas")
                return False

def upload_to_dbfs(local_path: str, dbfs_path: str) -> bool:
    """
    Upload do arquivo local para DBFS
    """
    try:
        filename = os.path.basename(local_path)
        print(f"  📤 Upload para DBFS: {filename}")
        
        # Verifica se arquivo local existe
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Arquivo local não encontrado: {local_path}")
        
        # Cria diretório DBFS se não existir
        try:
            dbutils.fs.mkdirs(os.path.dirname(dbfs_path))
        except:
            pass  # Diretório pode já existir
        
        # Remove arquivo DBFS se existir
        try:
            dbutils.fs.rm(dbfs_path)
        except:
            pass  # Arquivo pode não existir
        
        # Copia arquivo
        dbutils.fs.cp(f"file:{local_path}", dbfs_path)
        
        # Verifica se upload foi bem-sucedido
        file_info = dbutils.fs.ls(dbfs_path)
        if file_info:
            dbfs_size = file_info[0].size
            local_size = os.path.getsize(local_path)
            
            if dbfs_size != local_size:
                raise ValueError(f"Tamanhos diferentes: local={local_size}, dbfs={dbfs_size}")
            
            print(f"    ✅ Upload concluído: {dbfs_size / (1024*1024):.1f} MB")
            return True
        else:
            raise ValueError("Arquivo não encontrado no DBFS após upload")
        
    except Exception as e:
        print(f"    ❌ Erro no upload: {e}")
        return False

print("✅ Funções de download definidas!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Preparação do Ambiente

# COMMAND ----------

# Cria diretório local
os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
print(f"📁 Diretório local criado: {LOCAL_DATA_DIR}")

# Limpa diretório DBFS se existir
try:
    dbutils.fs.rm(DBFS_RAW_DIR, True)
    print(f"🗑️  Diretório DBFS limpo: {DBFS_RAW_DIR}")
except:
    print(f"📁 Diretório DBFS será criado: {DBFS_RAW_DIR}")

# Cria diretório DBFS
try:
    dbutils.fs.mkdirs(DBFS_RAW_DIR)
    print(f"✅ Diretório DBFS criado: {DBFS_RAW_DIR}")
except Exception as e:
    print(f"⚠️  Erro ao criar diretório DBFS: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Extração Principal

# COMMAND ----------

# Contadores
downloaded_files = []
failed_downloads = []

print("🚀 INICIANDO EXTRAÇÃO DOS DADOS")
print("="*40)

for taxi_type, months in DATA_SOURCES.items():
    print(f"\n🚕 Processando {taxi_type}:")
    
    for year_month, url in months.items():
        filename = f"{taxi_type}_tripdata_{year_month}.parquet"
        local_path = os.path.join(LOCAL_DATA_DIR, filename)
        dbfs_path = f"{DBFS_RAW_DIR}/{filename}"
        
        # Download
        if download_file_python(url, local_path):
            # Upload para DBFS
            if upload_to_dbfs(local_path, dbfs_path):
                downloaded_files.append({
                    'taxi_type': taxi_type,
                    'year_month': year_month,
                    'filename': filename,
                    'local_path': local_path,
                    'dbfs_path': dbfs_path,
                    'url': url
                })
                
                # Remove arquivo local para economizar espaço
                try:
                    os.remove(local_path)
                    print(f"    🗑️  Arquivo local removido")
                except:
                    pass
            else:
                failed_downloads.append({
                    'filename': filename,
                    'reason': 'Upload para DBFS falhou'
                })
        else:
            failed_downloads.append({
                'filename': filename,
                'reason': 'Download falhou'
            })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Relatório de Extração

# COMMAND ----------

print("📊 RELATÓRIO DE EXTRAÇÃO")
print("="*40)
print(f"   • Sucessos: {len(downloaded_files)}")
print(f"   • Falhas: {len(failed_downloads)}")

total_files = len(downloaded_files) + len(failed_downloads)
if total_files > 0:
    success_rate = (len(downloaded_files) / total_files) * 100
    print(f"   • Taxa de sucesso: {success_rate:.1f}%")

if downloaded_files:
    print(f"\n✅ Arquivos extraídos com sucesso:")
    for file_info in downloaded_files:
        print(f"   • {file_info['filename']} → {file_info['dbfs_path']}")

if failed_downloads:
    print(f"\n❌ Arquivos com falha:")
    for file_info in failed_downloads:
        print(f"   • {file_info['filename']}: {file_info['reason']}")

print(f"\n📁 Arquivos disponíveis no DBFS: {DBFS_RAW_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validação dos Arquivos

# COMMAND ----------

print("🔍 VALIDAÇÃO DOS ARQUIVOS EXTRAÍDOS")
print("="*40)

try:
    # Lista arquivos no DBFS
    dbfs_files = dbutils.fs.ls(DBFS_RAW_DIR)
    
    print(f"📂 Arquivos encontrados no DBFS ({len(dbfs_files)}):")
    total_size = 0
    
    for file_info in dbfs_files:
        file_size_mb = file_info.size / (1024 * 1024)
        total_size += file_size_mb
        print(f"   • {file_info.name}: {file_size_mb:.1f} MB")
    
    print(f"\n📊 Tamanho total: {total_size:.1f} MB")
    
    # Verifica se todos os arquivos esperados estão presentes
    expected_files = []
    for taxi_type, months in DATA_SOURCES.items():
        for year_month in months.keys():
            expected_files.append(f"{taxi_type}_tripdata_{year_month}.parquet")
    
    present_files = [f.name for f in dbfs_files]
    missing_files = [f for f in expected_files if f not in present_files]
    
    if missing_files:
        print(f"\n⚠️  Arquivos ausentes ({len(missing_files)}):")
        for filename in missing_files:
            print(f"   • {filename}")
    else:
        print(f"\n✅ Todos os arquivos esperados estão presentes!")
    
except Exception as e:
    print(f"❌ Erro na validação: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Preparação para Próxima Etapa

# COMMAND ----------

# Salva informações dos arquivos para próximo notebook
if downloaded_files:
    print("📋 PREPARANDO PARA CONSOLIDAÇÃO PYSPARK")
    print("="*40)
    
    # Cria lista de arquivos para o próximo notebook
    files_for_consolidation = []
    
    for file_info in downloaded_files:
        files_for_consolidation.append({
            'taxi_type': file_info['taxi_type'],
            'year_month': file_info['year_month'],
            'dbfs_path': file_info['dbfs_path'],
            'filename': file_info['filename']
        })
    
    print(f"✅ {len(files_for_consolidation)} arquivos prontos para consolidação")
    print(f"📁 Localização: {DBFS_RAW_DIR}")
    
    # Mostra exemplo de como acessar no próximo notebook
    print(f"\n💡 Para usar no próximo notebook:")
    print(f"```python")
    print(f"# Listar arquivos extraídos")
    print(f"dbutils.fs.ls('{DBFS_RAW_DIR}')")
    print(f"")
    print(f"# Ler um arquivo específico")
    print(f"df = spark.read.parquet('{DBFS_RAW_DIR}/yellow_tripdata_2023-01.parquet')")
    print(f"```")
    
else:
    print("❌ Nenhum arquivo foi extraído com sucesso!")
    print("💡 Verifique a conectividade e tente novamente")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Limpeza e Finalização

# COMMAND ----------

# Limpa diretório local temporário
try:
    import shutil
    if os.path.exists(LOCAL_DATA_DIR):
        shutil.rmtree(LOCAL_DATA_DIR)
        print(f"🗑️  Diretório local removido: {LOCAL_DATA_DIR}")
except Exception as e:
    print(f"⚠️  Erro ao limpar diretório local: {e}")

print(f"\n🎉 EXTRAÇÃO CONCLUÍDA!")
print("="*30)
print(f"✅ Arquivos extraídos: {len(downloaded_files)}")
print(f"❌ Arquivos com falha: {len(failed_downloads)}")
print(f"📁 Dados disponíveis em: {DBFS_RAW_DIR}")
print(f"\n🚀 Próximo passo: Execute o notebook de Consolidação PySpark")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo da Extração
# MAGIC 
# MAGIC ### ✅ Etapa 1 Concluída: Extração Python
# MAGIC 
# MAGIC **O que foi feito:**
# MAGIC - ✅ Download de arquivos Parquet via Python requests
# MAGIC - ✅ Upload para DBFS (Databricks File System)
# MAGIC - ✅ Validação dos arquivos extraídos
# MAGIC - ✅ Preparação para próxima etapa
# MAGIC 
# MAGIC **Arquivos processados:**
# MAGIC - Yellow Taxis: Janeiro a Maio 2023
# MAGIC - Green Taxis: Janeiro a Maio 2023
# MAGIC - Total: 10 arquivos Parquet
# MAGIC 
# MAGIC **Localização dos dados:**
# MAGIC - DBFS: `/FileStore/nyc_taxi/raw/`
# MAGIC - Formato: Parquet (dados brutos)
# MAGIC 
# MAGIC ### 🚀 Próximos Passos:
# MAGIC 1. **Notebook 2**: Consolidação PySpark (Raw → Bronze → Silver → Gold)
# MAGIC 2. **Notebook 3**: Análises SQL (Respostas às perguntas do case)
# MAGIC 
# MAGIC ### 🏗️ Arquitetura Implementada:
# MAGIC ```
# MAGIC [Python Download] → [DBFS Storage] → [PySpark Processing] → [SQL Analysis]
# MAGIC      ✅ FEITO           ✅ FEITO         🔄 PRÓXIMO         📊 FINAL
# MAGIC ```
