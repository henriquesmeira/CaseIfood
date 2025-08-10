# iFood Case - Data Architect

## Overview

This project implements a complete data engineering solution for NYC Taxi data analysis, answering specific questions from the iFood technical case for the Data Architect position.

### Main Objectives

1. **Data Ingestion**: Robust pipeline for downloading and processing NYC Taxi data
2. **Business Analysis**: Precise answers to technical case questions
3. **Scalable Architecture**: Implementation using PySpark and Delta Lake
4. **Data Quality**: Automated validation and cleaning

### Case Questions Answered

1. **Question 1**: What is the average total amount received per month considering all yellow taxis in the fleet?
2. **Question 2**: What is the average number of passengers per hour of the day who took taxis in May considering all taxis in the fleet?

### Solution Architecture

```
NYC Taxi Data → PySpark Processing → Delta Lake → SQL Analysis
```

## Project Structure V2

```
ifood-case/
├── src/                              # Source code V2
│   ├── config.py                     # 4-layer configuration
│   ├── data_extraction.py            # Python extraction
│   ├── data_consolidation.py         # PySpark consolidation
│   └── main_pipeline_v2.py           # V2 orchestrator
├── sql/                              # SQL queries
│   └── business_questions.sql        # Final answers
├── notebooks/                        # V2 notebooks
│   ├── 01_Data_Extraction_Python.py
│   ├── 02_Data_Consolidation_PySpark.py
│   └── 03_Business_Analysis_SQL.py
└── docs/                             # Documentation
```

## Solution Structure V2

### Architecture Implemented
```
Python (Extraction) → PySpark (Consolidation) → SQL (Analysis)
```

### 1. Python Extraction (src/)
- Robust download via requests
- Upload to DBFS
- Integrity validation

### 2. PySpark Consolidation (src/)
- **Raw Layer**: Raw data
- **Bronze Layer**: Standardized and clean
- **Silver Layer**: Enriched and validated
- **Gold Layer**: Aggregated for analysis

### 3. SQL Analysis (sql/)
- Optimized queries on Gold layer
- Case question answers
- Business insights

## Technologies Used V2

- **Python**: Data extraction (requests, os, time)
- **PySpark**: 4-layer Delta Lake consolidation
- **SQL**: Optimized final analysis
- **Delta Lake**: ACID storage with partitioning
- **Databricks Community Edition**: Execution environment

## How to Execute

### Option 1: V2 Notebooks Execution (Recommended)

1. **Clone this repository**
   ```bash
   git clone <your-repository>
   ```

2. **Import V2 notebooks to Databricks**
   - Access your Databricks workspace
   - Go to "Workspace" → "Import"
   - Upload files from `notebooks/` folder
   - Or import directly from GitHub

3. **Execute V2 notebooks in order:**
   - `01_Data_Extraction_Python.py` - Python extraction
   - `02_Data_Consolidation_PySpark.py` - 4-layer consolidation
   - `03_Business_Analysis_SQL.py` - Final SQL analysis

### Option 2: V2 Pipeline Execution

1. **Configure environment**
   ```bash
   pip install -r requirements.txt
   ```

2. **Execute complete V2 pipeline**
   ```python
   # In Databricks
   %run src/main_pipeline_v2.py
   ```

3. **Execute SQL queries**
   ```sql
   -- Load and execute
   %run sql/business_questions.sql
   ```

## Pipeline Flow

```
1. Python Extraction (src/data_extraction.py)
   ├── Download via requests
   ├── Upload to DBFS
   ├── Integrity validation
   └── Preparation for PySpark

2. PySpark Consolidation (src/data_consolidation.py)
   ├── Raw Layer (raw data)
   ├── Bronze Layer (standardized)
   ├── Silver Layer (enriched)
   ├── Gold Layer (aggregated)
   └── Delta Lake optimization

3. SQL Analysis (sql/business_questions.sql)
   ├── Question 1: Yellow Taxi Average
   ├── Question 2: Passengers per hour in May
   ├── Complementary analysis
   └── Business insights
```

## Results

### Data Processed (4 Layers)
- **Raw**: `main.nyc_taxi.raw_trips` (raw data)
- **Bronze**: `main.nyc_taxi.bronze_trips` (standardized)
- **Silver**: `main.nyc_taxi.silver_trips` (enriched)
- **Gold**: `main.nyc_taxi.gold_trips` (aggregated for analysis)

### Analysis Generated
- **Question 1**: Yellow taxi fare average (optimized SQL)
- **Question 2**: Passengers per hour in May (optimized SQL)
- **Complementary**: Additional insights and analysis

### V2 Technical Artifacts
- **Python Extraction**: Robust download with retry
- **PySpark Consolidation**: 4-layer Delta Lake
- **SQL Analysis**: Optimized queries on Gold layer
- **Documentation**: Updated guides for V2

## Data Sources

The data comes from the NYC Taxi & Limousine Commission:
- **URL**: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- **Period**: January to May 2023
- **Types**: Yellow and Green taxis
- **Format**: Parquet files

## Key Features

- **Robust Error Handling**: Retry logic and fallbacks
- **Scalable Architecture**: 4-layer data lake design
- **Performance Optimization**: Delta Lake with partitioning
- **Data Quality**: Automated validation and cleaning
- **Business Focus**: Direct answers to case questions

## Requirements

- Databricks Community Edition (or full workspace)
- Python 3.8+
- PySpark 3.x
- Delta Lake support

## Execution Time

- **V2 Sequential Execution**: 30-40 minutes
- **V2 Complete Pipeline**: 35-45 minutes
- **Individual Notebooks**: 10-20 minutes each

## Next Steps

1. Execute notebooks in order (01 → 02 → 03)
2. Or execute `main_pipeline_v2.py` for complete pipeline
3. Explore created Delta tables
4. Run custom SQL queries on `sql/business_questions.sql`

## Case Technical Completion

This project demonstrates:
- **Data Engineering**: Complete ETL pipeline
- **PySpark Expertise**: Advanced transformations and optimizations
- **SQL Proficiency**: Complex analytical queries
- **Architecture Design**: Scalable 4-layer data lake
- **Business Understanding**: Direct answers to case questions

The solution is production-ready and follows data engineering best practices.
