"""
Módulo para extração de dados NYC Taxi via Python
Responsável por download e upload para DBFS
"""

import requests
import os
import time
from typing import Dict, List, Tuple, Optional

from config import DATA_SOURCES, LOCAL_DATA_DIR, DBFS_RAW_DIR, RETRY_CONFIG


class NYCTaxiDataExtractor:
    """
    Classe para extração de dados NYC Taxi via Python
    """
    
    def __init__(self):
        """Inicializa o extrator"""
        self.downloaded_files = []
        self.failed_downloads = []
        self.local_dir = LOCAL_DATA_DIR
        self.dbfs_dir = DBFS_RAW_DIR
        
        # Cria diretório local
        os.makedirs(self.local_dir, exist_ok=True)
        print(f"📁 Diretório local criado: {self.local_dir}")
    
    def download_file(self, url: str, local_path: str, max_retries: int = 3) -> bool:
        """
        Download de arquivo via Python requests
        
        Args:
            url: URL do arquivo
            local_path: Caminho local para salvar
            max_retries: Número máximo de tentativas
            
        Returns:
            True se download foi bem-sucedido
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
                
                # Verifica content-type
                content_type = response.headers.get('content-type', '')
                if 'application/octet-stream' not in content_type and 'application/x-parquet' not in content_type:
                    print(f"    ⚠️  Content-type inesperado: {content_type}")
                
                # Salva arquivo em chunks
                total_size = int(response.headers.get('content-length', 0))
                downloaded_size = 0
                
                with open(local_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                            
                            # Progress a cada 50MB
                            if downloaded_size % (50 * 1024 * 1024) == 0:
                                progress = (downloaded_size / total_size * 100) if total_size > 0 else 0
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
                    delay = RETRY_CONFIG["initial_delay"] * (RETRY_CONFIG["backoff_multiplier"] ** attempt)
                    delay = min(delay, RETRY_CONFIG["max_delay"])
                    print(f"    ⏳ Aguardando {delay} segundos...")
                    time.sleep(delay)
                else:
                    print(f"    💥 Falha definitiva após {max_retries} tentativas")
                    return False
    
    def upload_to_dbfs(self, local_path: str, dbfs_path: str) -> bool:
        """
        Upload do arquivo local para DBFS
        
        Args:
            local_path: Caminho do arquivo local
            dbfs_path: Caminho de destino no DBFS
            
        Returns:
            True se upload foi bem-sucedido
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
            try:
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
                raise ValueError(f"Verificação do upload falhou: {e}")
            
        except Exception as e:
            print(f"    ❌ Erro no upload: {e}")
            return False
    
    def extract_all_data(self) -> Dict:
        """
        Extrai todos os dados definidos em DATA_SOURCES
        
        Returns:
            Dicionário com resultados da extração
        """
        print("🚀 INICIANDO EXTRAÇÃO DE DADOS NYC TAXI")
        print("="*60)
        
        self.downloaded_files = []
        self.failed_downloads = []
        
        total_files = sum(len(months) for months in DATA_SOURCES.values())
        print(f"📊 Total de arquivos para processar: {total_files}")
        
        for taxi_type, months in DATA_SOURCES.items():
            print(f"\n🚕 Processando {taxi_type}:")
            
            for year_month, url in months.items():
                filename = f"{taxi_type}_tripdata_{year_month}.parquet"
                local_path = os.path.join(self.local_dir, filename)
                dbfs_path = f"{self.dbfs_dir}/{filename}"
                
                # Download
                if self.download_file(url, local_path):
                    # Upload para DBFS
                    if self.upload_to_dbfs(local_path, dbfs_path):
                        self.downloaded_files.append({
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
                        self.failed_downloads.append({
                            'filename': filename,
                            'reason': 'Upload para DBFS falhou'
                        })
                else:
                    self.failed_downloads.append({
                        'filename': filename,
                        'reason': 'Download falhou'
                    })
        
        # Relatório final
        self.print_extraction_report()
        
        return {
            'successful_files': self.downloaded_files,
            'failed_files': self.failed_downloads,
            'success_count': len(self.downloaded_files),
            'failure_count': len(self.failed_downloads),
            'total_files': total_files
        }
    
    def print_extraction_report(self):
        """Imprime relatório da extração"""
        print(f"\n📊 RELATÓRIO DE EXTRAÇÃO")
        print("="*40)
        print(f"   • Sucessos: {len(self.downloaded_files)}")
        print(f"   • Falhas: {len(self.failed_downloads)}")
        print(f"   • Taxa de sucesso: {len(self.downloaded_files)/(len(self.downloaded_files)+len(self.failed_downloads))*100:.1f}%")
        
        if self.downloaded_files:
            print(f"\n✅ Arquivos extraídos com sucesso:")
            for file_info in self.downloaded_files:
                print(f"   • {file_info['filename']} → {file_info['dbfs_path']}")
        
        if self.failed_downloads:
            print(f"\n❌ Arquivos com falha:")
            for file_info in self.failed_downloads:
                print(f"   • {file_info['filename']}: {file_info['reason']}")
        
        print(f"\n📁 Arquivos disponíveis no DBFS: {self.dbfs_dir}")
    
    def list_extracted_files(self) -> List[Dict]:
        """
        Lista arquivos extraídos com sucesso
        
        Returns:
            Lista de informações dos arquivos
        """
        return self.downloaded_files.copy()
    
    def cleanup_local_files(self):
        """Remove arquivos locais temporários"""
        try:
            import shutil
            if os.path.exists(self.local_dir):
                shutil.rmtree(self.local_dir)
                print(f"🗑️  Diretório local removido: {self.local_dir}")
        except Exception as e:
            print(f"⚠️  Erro ao limpar arquivos locais: {e}")


def main():
    """Função principal para execução standalone"""
    extractor = NYCTaxiDataExtractor()
    results = extractor.extract_all_data()
    
    if results['success_count'] > 0:
        print(f"\n🎉 Extração concluída com sucesso!")
        print(f"📊 {results['success_count']} de {results['total_files']} arquivos extraídos")
        return True
    else:
        print(f"\n💥 Extração falhou!")
        print(f"❌ Nenhum arquivo foi extraído com sucesso")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
