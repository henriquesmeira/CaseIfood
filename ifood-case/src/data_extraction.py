"""
Módulo de extração de dados para dados NYC Taxi
Gerencia download e upload para DBFS com tratamento robusto de erros
"""

import requests
import os
import time
from typing import Dict, List, Tuple, Optional

from config import DATA_SOURCES, LOCAL_DATA_DIR, DBFS_RAW_DIR


class NYCTaxiDataExtractor:
    """
    Extrator de dados para dados NYC Taxi via Python requests
    """

    def __init__(self):
        """Inicializa o extrator"""
        self.downloaded_files = []
        self.failed_downloads = []
        self.local_dir = LOCAL_DATA_DIR
        self.dbfs_dir = DBFS_RAW_DIR
        
        # Cria diretório local
        os.makedirs(self.local_dir, exist_ok=True)
        print(f"Diretório local criado: {self.local_dir}")
    
    def download_file(self, url: str, local_path: str, max_retries: int = 3) -> bool:
        """
        Faz download do arquivo via Python requests com lógica de retry

        Args:
            url: URL do arquivo
            local_path: Caminho local para salvar o arquivo
            max_retries: Número máximo de tentativas de retry

        Returns:
            True se o download foi bem-sucedido
        """
        for attempt in range(max_retries):
            try:
                filename = os.path.basename(local_path)
                print(f"  Downloading attempt {attempt + 1}: {filename}")
                
                # Headers to simulate browser
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Connection': 'keep-alive'
                }
                
                # Download with streaming and timeout
                response = requests.get(
                    url, 
                    headers=headers, 
                    stream=True, 
                    timeout=600,
                    verify=True
                )
                response.raise_for_status()
                
                # Save file in chunks
                total_size = int(response.headers.get('content-length', 0))
                downloaded_size = 0
                
                with open(local_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                            
                            # Progress every 50MB
                            if downloaded_size % (50 * 1024 * 1024) == 0:
                                progress = (downloaded_size / total_size * 100) if total_size > 0 else 0
                                print(f"    Progress: {progress:.1f}%")
                
                # Verify final size
                file_size = os.path.getsize(local_path)
                if file_size == 0:
                    raise ValueError("Downloaded file is empty")
                
                print(f"    Download completed: {file_size / (1024*1024):.1f} MB")
                return True
                
            except Exception as e:
                print(f"    Error: {str(e)[:150]}...")
                
                # Remove partial file
                try:
                    if os.path.exists(local_path):
                        os.remove(local_path)
                except:
                    pass
                
                if attempt < max_retries - 1:
                    delay = 10 * (2 ** attempt)
                    print(f"    Waiting {delay} seconds...")
                    time.sleep(delay)
                else:
                    print(f"    Final failure after {max_retries} attempts")
                    return False
    
    def upload_to_dbfs(self, local_path: str, dbfs_path: str) -> bool:
        """
        Upload local file to DBFS with fallback to local storage
        
        Args:
            local_path: Local file path
            dbfs_path: DBFS destination path
            
        Returns:
            True if upload was successful or fallback applied
        """
        try:
            filename = os.path.basename(local_path)
            print(f"  Uploading to DBFS: {filename}")
            
            # Verify local file exists
            if not os.path.exists(local_path):
                raise FileNotFoundError(f"Local file not found: {local_path}")
            
            # Create DBFS directory if it doesn't exist
            try:
                dbutils.fs.mkdirs(os.path.dirname(dbfs_path))
            except Exception as mkdir_error:
                print(f"    Warning: DBFS directory creation failed: {mkdir_error}")
                if "Public DBFS root is disabled" in str(mkdir_error):
                    print(f"    DBFS public access disabled - keeping file local")
                    return True
                raise mkdir_error
            
            # Remove existing DBFS file
            try:
                dbutils.fs.rm(dbfs_path)
            except:
                pass
            
            # Copy file to DBFS
            dbutils.fs.cp(f"file:{local_path}", dbfs_path)
            
            # Verify upload success
            file_info = dbutils.fs.ls(dbfs_path)
            if file_info:
                dbfs_size = file_info[0].size
                local_size = os.path.getsize(local_path)
                
                if dbfs_size != local_size:
                    raise ValueError(f"Size mismatch: local={local_size}, dbfs={dbfs_size}")
                
                print(f"    Upload completed: {dbfs_size / (1024*1024):.1f} MB")
                return True
            else:
                raise ValueError("File not found in DBFS after upload")
            
        except Exception as e:
            print(f"    Upload error: {e}")
            if "Public DBFS root is disabled" in str(e):
                print(f"    Keeping file local: {local_path}")
                return True
            return False
    
    def extract_all_data(self) -> Dict:
        """
        Extract all data defined in DATA_SOURCES
        
        Returns:
            Dictionary with extraction results
        """
        print("Starting NYC Taxi data extraction")
        print("=" * 50)
        
        self.downloaded_files = []
        self.failed_downloads = []
        
        total_files = sum(len(months) for months in DATA_SOURCES.values())
        print(f"Total files to process: {total_files}")
        
        for taxi_type, months in DATA_SOURCES.items():
            print(f"\nProcessing {taxi_type}:")
            
            for year_month, url in months.items():
                filename = f"{taxi_type}_tripdata_{year_month}.parquet"
                local_path = os.path.join(self.local_dir, filename)
                dbfs_path = f"{self.dbfs_dir}/{filename}"
                
                # Download
                if self.download_file(url, local_path):
                    # Upload to DBFS (or keep local if DBFS fails)
                    upload_success = self.upload_to_dbfs(local_path, dbfs_path)
                    
                    if upload_success:
                        # Check if file actually exists in DBFS
                        try:
                            dbutils.fs.ls(dbfs_path)
                            actual_dbfs_path = dbfs_path
                            can_remove_local = True
                        except:
                            # DBFS failed, use local file
                            actual_dbfs_path = f"file:{local_path}"
                            can_remove_local = False
                            print(f"    Using local file: {local_path}")
                        
                        self.downloaded_files.append({
                            'taxi_type': taxi_type,
                            'year_month': year_month,
                            'filename': filename,
                            'local_path': local_path,
                            'dbfs_path': actual_dbfs_path,
                            'url': url,
                            'is_local_only': not can_remove_local
                        })
                        
                        # Remove local file only if DBFS upload was successful
                        if can_remove_local:
                            try:
                                os.remove(local_path)
                                print(f"    Local file removed")
                            except:
                                pass
                        else:
                            print(f"    File kept locally")
                    else:
                        self.failed_downloads.append({
                            'filename': filename,
                            'reason': 'DBFS upload failed'
                        })
                else:
                    self.failed_downloads.append({
                        'filename': filename,
                        'reason': 'Download failed'
                    })
        
        # Final report
        self.print_extraction_report()
        
        return {
            'successful_files': self.downloaded_files,
            'failed_files': self.failed_downloads,
            'success_count': len(self.downloaded_files),
            'failure_count': len(self.failed_downloads),
            'total_files': total_files
        }
    
    def print_extraction_report(self):
        """Print extraction report"""
        print(f"\nExtraction Report")
        print("=" * 30)
        print(f"   Successful: {len(self.downloaded_files)}")
        print(f"   Failed: {len(self.failed_downloads)}")
        
        if len(self.downloaded_files) + len(self.failed_downloads) > 0:
            success_rate = len(self.downloaded_files) / (len(self.downloaded_files) + len(self.failed_downloads)) * 100
            print(f"   Success rate: {success_rate:.1f}%")
        
        if self.downloaded_files:
            print(f"\nSuccessful extractions:")
            for file_info in self.downloaded_files:
                location = "local" if file_info.get('is_local_only', False) else "DBFS"
                print(f"   {file_info['filename']} -> {location}")
        
        if self.failed_downloads:
            print(f"\nFailed extractions:")
            for file_info in self.failed_downloads:
                print(f"   {file_info['filename']}: {file_info['reason']}")
        
        print(f"\nFiles available in: {self.dbfs_dir}")
    
    def list_extracted_files(self) -> List[Dict]:
        """
        List successfully extracted files
        
        Returns:
            List of file information dictionaries
        """
        return self.downloaded_files.copy()
    
    def cleanup_local_files(self):
        """Remove temporary local files"""
        try:
            import shutil
            if os.path.exists(self.local_dir):
                shutil.rmtree(self.local_dir)
                print(f"Local directory removed: {self.local_dir}")
        except Exception as e:
            print(f"Warning: Error cleaning local files: {e}")


def main():
    """Main function for standalone execution"""
    extractor = NYCTaxiDataExtractor()
    results = extractor.extract_all_data()
    
    if results['success_count'] > 0:
        print(f"\nExtraction completed successfully!")
        print(f"{results['success_count']} of {results['total_files']} files extracted")
        return True
    else:
        print(f"\nExtraction failed!")
        print(f"No files were extracted successfully")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
