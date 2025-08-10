# Estrutura do Projeto - Case Técnico iFood

## 📁 Visão Geral da Estrutura

```
ifood-case/
├── 📋 README.md                         # Documentação principal do projeto
├── 🚀 EXECUTION_GUIDE.md                # Guia passo-a-passo de execução
├── 🔧 TECHNICAL_DETAILS.md              # Detalhes técnicos e arquitetura
├── 📊 PROJECT_STRUCTURE.md              # Este arquivo - estrutura do projeto
├── 📦 requirements.txt                  # Dependências Python
│
├── 📂 src/                              # Código fonte V2
│   ├── __init__.py
│   ├── config.py                        # Configurações das 4 camadas
│   ├── data_extraction.py               # Extração via Python
│   ├── data_consolidation.py            # Consolidação PySpark (4 camadas)
│   └── main_pipeline_v2.py              # Orquestrador completo V2
│
├── 📂 sql/                              # Consultas SQL
│   └── business_questions.sql           # Respostas finais em SQL
│
└── 📂 notebooks/                        # Notebooks V2 (Execução Sequencial)
    ├── 01_Data_Extraction_Python.py     # Extração Python
    ├── 02_Data_Consolidation_PySpark.py # Consolidação PySpark
    └── 03_Business_Analysis_SQL.py      # Análises SQL
```

## 📋 Descrição dos Arquivos

### 📄 Documentação

| Arquivo | Descrição | Público-Alvo |
|---------|-----------|--------------|
| `README.md` | Visão geral do projeto, objetivos e instruções básicas | Todos |
| `EXECUTION_GUIDE.md` | Guia detalhado de execução passo-a-passo | Executores |
| `TECHNICAL_DETAILS.md` | Arquitetura, decisões técnicas e otimizações | Desenvolvedores |
| `PROJECT_STRUCTURE.md` | Estrutura e organização do projeto | Desenvolvedores |

### ⚡ Execução Rápida

| Arquivo | Descrição | Uso |
|---------|-----------|-----|
| `quick_start.py` | Script completo para execução rápida no Databricks | Demonstração |
| `requirements.txt` | Dependências Python para desenvolvimento local | Setup |

### 🔧 Código Fonte V2 (`src/`)

| Arquivo | Responsabilidade | Principais Classes/Funções |
|---------|------------------|---------------------------|
| `config.py` | Configurações das 4 camadas | `get_table_name()`, camadas Raw/Bronze/Silver/Gold |
| `data_extraction.py` | Extração via Python | `NYCTaxiDataExtractor` |
| `data_consolidation.py` | Consolidação PySpark | `DataLakeConsolidator` |
| `main_pipeline_v2.py` | Orquestração completa V2 | `NYCTaxiPipelineV2` |

### 📊 Consultas SQL (`sql/`)

| Arquivo | Foco | Saídas |
|---------|------|--------|
| `business_questions.sql` | Perguntas do case + análises | Respostas SQL otimizadas |

### 📓 Notebooks V2 (`notebooks/`)

| Notebook | Objetivo | Tempo Estimado |
|----------|----------|----------------|
| `01_Data_Extraction_Python.py` | Extração via Python | 10-15 min |
| `02_Data_Consolidation_PySpark.py` | Consolidação 4 camadas | 15-20 min |
| `03_Business_Analysis_SQL.py` | Análises SQL finais | 5-10 min |

## 🔄 Fluxo de Execução

### Opção 1: Notebooks V2 (Recomendado)
```
1. 01_Data_Extraction_Python.py
   ├── Download via Python requests
   ├── Upload para DBFS
   ├── Validação dos arquivos
   └── Preparação para PySpark

2. 02_Data_Consolidation_PySpark.py
   ├── Camada Raw (dados brutos)
   ├── Camada Bronze (padronizados)
   ├── Camada Silver (enriquecidos)
   ├── Camada Gold (agregados)
   └── Otimização Delta Lake

3. 03_Business_Analysis_SQL.py
   ├── Pergunta 1: Média Yellow Taxis
   ├── Pergunta 2: Passageiros por hora (Maio)
   ├── Análises complementares
   └── Insights de negócio
```

### Opção 2: Pipeline Completo V2
```
main_pipeline_v2.py
├── Executa data_extraction.py (Python)
├── Executa data_consolidation.py (PySpark)
├── Executa business_questions.sql (SQL)
└── Gera relatórios finais
```

## 🎯 Pontos de Entrada

### Para Avaliadores do Case
1. **Execução Sequencial**: Execute notebooks V2 na ordem (01 → 02 → 03)
2. **Pipeline Completo**: Execute `main_pipeline_v2.py`
3. **Código Fonte**: Revise arquivos em `src/` (versão V2)

### Para Desenvolvedores
1. **Configurações**: Comece com `src/config.py` (4 camadas)
2. **Pipeline Principal**: `src/main_pipeline_v2.py`
3. **Consultas**: Adicione análises em `sql/`

### Para Usuários Finais
1. **Resultados**: Notebook `03_Business_Analysis_SQL.py`
2. **Consultas**: Arquivo `sql/business_questions.sql`
3. **Dados**: Tabela `main.nyc_taxi.gold_trips` (camada analítica)

## 📊 Saídas do Projeto

### Dados Processados (4 Camadas)
- **Raw**: `main.nyc_taxi.raw_trips` (dados brutos)
- **Bronze**: `main.nyc_taxi.bronze_trips` (padronizados)
- **Silver**: `main.nyc_taxi.silver_trips` (enriquecidos)
- **Gold**: `main.nyc_taxi.gold_trips` (agregados para análises)

### Análises Geradas
- **Pergunta 1**: Média de tarifas Yellow Taxis (SQL otimizado)
- **Pergunta 2**: Média de passageiros por hora em Maio (SQL otimizado)
- **Complementares**: Insights e análises adicionais

### Artefatos Técnicos V2
- **Extração Python**: Download robusto com retry
- **Consolidação PySpark**: 4 camadas Delta Lake
- **Análises SQL**: Consultas otimizadas na camada Gold
- **Documentação**: Guias atualizados para V2

## 🔧 Configurações Importantes

### Spark Configurations
```python
# Em config.py
SPARK_CONFIGS = {
    "spark.sql.adaptive.enabled": "true",
    "spark.databricks.delta.schema.autoMerge.enabled": "true",
    # ... outras configurações
}
```

### Fontes de Dados
```python
# URLs dos dados NYC Taxi
DATA_SOURCES = {
    "yellow": {"2023-01": "https://...", ...},
    "green": {"2023-01": "https://...", ...}
}
```

### Colunas Obrigatórias
```python
REQUIRED_COLUMNS = [
    "VendorID", "passenger_count", "total_amount",
    "tpep_pickup_datetime", "tpep_dropoff_datetime"
]
```

## 🚀 Como Começar

### Execução V2 Sequencial (30-40 minutos)
1. Abra o Databricks Community Edition
2. Importe os 3 notebooks V2
3. Execute na ordem: 01 → 02 → 03
4. Veja resultados SQL finais

### Execução V2 Pipeline Completo (35-45 minutos)
1. Clone o repositório
2. Importe `src/main_pipeline_v2.py`
3. Execute o pipeline completo
4. Explore tabelas Delta criadas

### Desenvolvimento Local
1. `pip install -r requirements.txt`
2. Configure ambiente Spark local
3. Adapte configurações em `config.py` (4 camadas)
4. Execute módulos V2 separadamente

## 📞 Suporte

- **Issues**: Problemas técnicos ou dúvidas
- **Documentação**: Consulte os arquivos `.md`
- **Código**: Comentários detalhados nos scripts
- **Logs**: Saídas detalhadas durante execução
