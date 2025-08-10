# Databricks notebook source
# MAGIC %md
# MAGIC # iFood Case - Data Architect
# MAGIC ## Notebook 1: Data Extraction (Python)
# MAGIC 
# MAGIC This notebook implements the first stage of the data pipeline:
# MAGIC 
# MAGIC ### Architecture:
# MAGIC 1. **Python**: Data extraction (this notebook)
# MAGIC 2. **PySpark**: Data consolidation (next notebook)
# MAGIC 3. **SQL**: Business analysis (final notebook)
# MAGIC 
# MAGIC ### Responsibilities:
# MAGIC - Download Parquet files via Python
# MAGIC - Upload to DBFS (Databricks File System)
# MAGIC - Validate downloaded files

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration and Imports

# COMMAND ----------

import requests
import os
import time
from typing import Dict, List, Tuple

# Configuration
LOCAL_DATA_DIR = "/tmp/nyc_taxi_data"
DBFS_RAW_DIR = "/tmp/nyc_taxi/raw"

# NYC Taxi data URLs (January to May 2023)
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

print("NYC Taxi Data Extraction - Python")
print("=" * 40)
print(f"Total files: {sum(len(months) for months in DATA_SOURCES.values())}")
print(f"Local directory: {LOCAL_DATA_DIR}")
print(f"DBFS directory: {DBFS_RAW_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Download Functions

# COMMAND ----------

def download_file_python(url: str, local_path: str, max_retries: int = 3) -> bool:
    """
    Download file via Python requests
    """
    for attempt in range(max_retries):
        try:
            filename = os.path.basename(local_path)
            print(f"  Download attempt {attempt + 1}: {filename}")
            
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
                        if total_size > 0 and downloaded_size % (50 * 1024 * 1024) == 0:
                            progress = (downloaded_size / total_size * 100)
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

def upload_to_dbfs(local_path: str, dbfs_path: str) -> bool:
    """
    Upload local file to DBFS (with fallback to local storage)
    """
    try:
        filename = os.path.basename(local_path)
        print(f"  Upload to DBFS: {filename}")
        
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
        
        # Copy file
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

print("Download functions defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Environment Setup

# COMMAND ----------

# Create local directory
os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
print(f"Local directory created: {LOCAL_DATA_DIR}")

# Clean DBFS directory if exists
try:
    dbutils.fs.rm(DBFS_RAW_DIR, True)
    print(f"DBFS directory cleaned: {DBFS_RAW_DIR}")
except:
    print(f"DBFS directory will be created: {DBFS_RAW_DIR}")

# Create DBFS directory
try:
    dbutils.fs.mkdirs(DBFS_RAW_DIR)
    print(f"DBFS directory created: {DBFS_RAW_DIR}")
except Exception as e:
    print(f"Warning: Error creating DBFS directory: {e}")
    print("Note: Using /tmp directory which is allowed in Community Edition")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Main Extraction

# COMMAND ----------

# Counters
downloaded_files = []
failed_downloads = []

print("Starting data extraction")
print("=" * 30)

for taxi_type, months in DATA_SOURCES.items():
    print(f"\nProcessing {taxi_type}:")
    
    for year_month, url in months.items():
        filename = f"{taxi_type}_tripdata_{year_month}.parquet"
        local_path = os.path.join(LOCAL_DATA_DIR, filename)
        dbfs_path = f"{DBFS_RAW_DIR}/{filename}"
        
        # Download
        if download_file_python(url, local_path):
            # Upload to DBFS (or keep local if DBFS fails)
            upload_success = upload_to_dbfs(local_path, dbfs_path)
            
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
                
                downloaded_files.append({
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
                failed_downloads.append({
                    'filename': filename,
                    'reason': 'DBFS upload failed'
                })
        else:
            failed_downloads.append({
                'filename': filename,
                'reason': 'Download failed'
            })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Extraction Report

# COMMAND ----------

print("Extraction Report")
print("=" * 30)
print(f"   Successful: {len(downloaded_files)}")
print(f"   Failed: {len(failed_downloads)}")

total_files = len(downloaded_files) + len(failed_downloads)
if total_files > 0:
    success_rate = (len(downloaded_files) / total_files) * 100
    print(f"   Success rate: {success_rate:.1f}%")

if downloaded_files:
    print(f"\nSuccessful extractions:")
    for file_info in downloaded_files:
        location = "local" if file_info.get('is_local_only', False) else "DBFS"
        print(f"   {file_info['filename']} -> {location}")

if failed_downloads:
    print(f"\nFailed extractions:")
    for file_info in failed_downloads:
        print(f"   {file_info['filename']}: {file_info['reason']}")

print(f"\nFiles available in: {DBFS_RAW_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. File Validation

# COMMAND ----------

print("Validating extracted files")
print("=" * 30)

try:
    # List files in DBFS
    dbfs_files = dbutils.fs.ls(DBFS_RAW_DIR)
    
    print(f"Files found in DBFS ({len(dbfs_files)}):")
    total_size = 0
    
    for file_info in dbfs_files:
        file_size_mb = file_info.size / (1024 * 1024)
        total_size += file_size_mb
        print(f"   {file_info.name}: {file_size_mb:.1f} MB")
    
    print(f"\nTotal size: {total_size:.1f} MB")
    
    # Check if all expected files are present
    expected_files = []
    for taxi_type, months in DATA_SOURCES.items():
        for year_month in months.keys():
            expected_files.append(f"{taxi_type}_tripdata_{year_month}.parquet")
    
    present_files = [f.name for f in dbfs_files]
    missing_files = [f for f in expected_files if f not in present_files]
    
    if missing_files:
        print(f"\nMissing files ({len(missing_files)}):")
        for filename in missing_files:
            print(f"   {filename}")
    else:
        print(f"\nAll expected files are present")
    
except Exception as e:
    print(f"Validation error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Preparation for Next Stage

# COMMAND ----------

# Save file information for next notebook
if downloaded_files:
    print("Preparing for PySpark consolidation")
    print("=" * 30)
    
    # Create list of files for consolidation
    files_for_consolidation = []
    
    for file_info in downloaded_files:
        files_for_consolidation.append({
            'taxi_type': file_info['taxi_type'],
            'year_month': file_info['year_month'],
            'dbfs_path': file_info['dbfs_path'],
            'filename': file_info['filename']
        })
    
    print(f"{len(files_for_consolidation)} files ready for consolidation")
    print(f"Location: {DBFS_RAW_DIR}")
    
    # Show example usage for next notebook
    print(f"\nFor use in next notebook:")
    
    # Check if there are local vs DBFS files
    local_files = [f for f in files_for_consolidation if f.get('is_local_only', False)]
    dbfs_files = [f for f in files_for_consolidation if not f.get('is_local_only', False)]
    
    if dbfs_files:
        print(f"# List DBFS files")
        print(f"dbutils.fs.ls('{DBFS_RAW_DIR}')")
        print(f"")
        print(f"# Read DBFS file")
        print(f"df = spark.read.parquet('{DBFS_RAW_DIR}/yellow_tripdata_2023-01.parquet')")
    
    if local_files:
        print(f"\nSome files are local only:")
        print(f"# Read local file")
        print(f"df = spark.read.parquet('file:/tmp/nyc_taxi_data/yellow_tripdata_2023-01.parquet')")
    
else:
    print("No files were extracted successfully")
    print("Check connectivity and try again")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Cleanup and Finalization

# COMMAND ----------

# Clean temporary local directory
try:
    import shutil
    if os.path.exists(LOCAL_DATA_DIR):
        shutil.rmtree(LOCAL_DATA_DIR)
        print(f"Local directory removed: {LOCAL_DATA_DIR}")
except Exception as e:
    print(f"Warning: Error cleaning local directory: {e}")

print(f"\nExtraction completed")
print("=" * 20)
print(f"Files extracted: {len(downloaded_files)}")
print(f"Files failed: {len(failed_downloads)}")

# Show data location
local_only = sum(1 for f in downloaded_files if f.get('is_local_only', False))
dbfs_files = len(downloaded_files) - local_only

if dbfs_files > 0:
    print(f"DBFS files: {dbfs_files} in {DBFS_RAW_DIR}")
if local_only > 0:
    print(f"Local files: {local_only} in {LOCAL_DATA_DIR}")

print(f"\nNext step: Execute PySpark consolidation notebook")
print(f"Note: Next notebook will automatically detect file locations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC ### Stage 1 Completed: Python Extraction
# MAGIC 
# MAGIC **What was done:**
# MAGIC - Downloaded Parquet files via Python requests
# MAGIC - Uploaded to DBFS (Databricks File System)
# MAGIC - Validated extracted files
# MAGIC - Prepared for next stage
# MAGIC 
# MAGIC **Files processed:**
# MAGIC - Yellow Taxis: January to May 2023
# MAGIC - Green Taxis: January to May 2023
# MAGIC - Total: 10 Parquet files
# MAGIC 
# MAGIC **Data location:**
# MAGIC - DBFS: `/tmp/nyc_taxi/raw/`
# MAGIC - Format: Parquet (raw data)
# MAGIC 
# MAGIC ### Next Steps:
# MAGIC 1. **Notebook 2**: PySpark Consolidation (Raw → Bronze → Silver → Gold)
# MAGIC 2. **Notebook 3**: SQL Analysis (Business questions answers)
# MAGIC 
# MAGIC ### Architecture Implemented:
# MAGIC ```
# MAGIC [Python Download] → [DBFS Storage] → [PySpark Processing] → [SQL Analysis]
# MAGIC      COMPLETED           COMPLETED         NEXT              FINAL
# MAGIC ```
