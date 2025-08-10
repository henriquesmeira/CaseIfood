# Estrutura do Projeto - Case Técnico iFood

## 📁 Visão Geral da Estrutura

```
ifood-case/
├── 📋 README.md                    # Documentação principal do projeto
├── 🚀 EXECUTION_GUIDE.md           # Guia passo-a-passo de execução
├── 🔧 TECHNICAL_DETAILS.md         # Detalhes técnicos e arquitetura
├── 📊 PROJECT_STRUCTURE.md         # Este arquivo - estrutura do projeto
├── ⚡ quick_start.py               # Script de execução rápida
├── 📦 requirements.txt             # Dependências Python
│
├── 📂 src/                         # Código fonte principal
│   ├── __init__.py
│   ├── config.py                   # Configurações centralizadas
│   ├── data_ingestion.py          # Pipeline de ingestão de dados
│   ├── data_quality.py            # Validação e qualidade dos dados
│   └── main_pipeline.py           # Orquestrador principal
│
├── 📂 analysis/                    # Scripts de análise
│   ├── __init__.py
│   ├── business_questions.py      # Respostas às perguntas do case
│   └── exploratory_analysis.py    # Análise exploratória completa
│
└── 📂 notebooks/                   # Notebooks Databricks
    ├── 01_Data_Ingestion.py       # Notebook de ingestão
    ├── 02_Business_Analysis.py    # Notebook de análises de negócio
    └── 03_Exploratory_Analysis.py # Notebook de análise exploratória
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

### 🔧 Código Fonte (`src/`)

| Arquivo | Responsabilidade | Principais Classes/Funções |
|---------|------------------|---------------------------|
| `config.py` | Configurações centralizadas | `DATA_SOURCES`, `SPARK_CONFIGS` |
| `data_ingestion.py` | Ingestão e processamento de dados | `NYCTaxiDataIngestion` |
| `data_quality.py` | Validação e qualidade | `DataQualityValidator` |
| `main_pipeline.py` | Orquestração completa | `NYCTaxiPipeline` |

### 📊 Análises (`analysis/`)

| Arquivo | Foco | Saídas |
|---------|------|--------|
| `business_questions.py` | Perguntas específicas do case | Respostas numéricas e tabelas |
| `exploratory_analysis.py` | Análise exploratória completa | Insights e visualizações |

### 📓 Notebooks (`notebooks/`)

| Notebook | Objetivo | Tempo Estimado |
|----------|----------|----------------|
| `01_Data_Ingestion.py` | Ingestão completa dos dados | 15-20 min |
| `02_Business_Analysis.py` | Respostas às perguntas do case | 5-10 min |
| `03_Exploratory_Analysis.py` | Análise exploratória detalhada | 10-15 min |

## 🔄 Fluxo de Execução

### Opção 1: Notebooks (Recomendado)
```
1. 01_Data_Ingestion.py
   ├── Download dos dados NYC Taxi
   ├── Processamento e padronização
   ├── Criação da tabela Delta Lake
   └── Validação inicial

2. 02_Business_Analysis.py
   ├── Pergunta 1: Média Yellow Taxis
   ├── Pergunta 2: Passageiros por hora (Maio)
   └── Análises complementares

3. 03_Exploratory_Analysis.py
   ├── Padrões temporais
   ├── Distribuições de dados
   ├── Qualidade dos dados
   └── Visualizações
```

### Opção 2: Scripts Python
```
1. main_pipeline.py
   ├── Executa data_ingestion.py
   ├── Executa data_quality.py
   └── Gera relatórios

2. business_questions.py
   ├── Carrega dados da tabela Delta
   └── Responde perguntas específicas

3. exploratory_analysis.py
   ├── Análise exploratória completa
   └── Insights de negócio
```

### Opção 3: Execução Rápida
```
quick_start.py
├── Ingestão simplificada (5 arquivos)
├── Análises principais
└── Resultados em ~10-15 minutos
```

## 🎯 Pontos de Entrada

### Para Avaliadores do Case
1. **Início Rápido**: Execute `quick_start.py` no Databricks
2. **Análise Completa**: Execute os notebooks na ordem
3. **Código Fonte**: Revise os arquivos em `src/`

### Para Desenvolvedores
1. **Configurações**: Comece com `src/config.py`
2. **Pipeline Principal**: `src/main_pipeline.py`
3. **Extensões**: Adicione análises em `analysis/`

### Para Usuários Finais
1. **Resultados**: Notebooks `02_Business_Analysis.py`
2. **Insights**: Notebook `03_Exploratory_Analysis.py`
3. **Dados**: Tabela `main.nyc_taxi.trips_delta`

## 📊 Saídas do Projeto

### Dados Processados
- **Tabela Delta**: `main.nyc_taxi.trips_delta`
- **Registros**: ~13-15 milhões (Jan-Mai 2023)
- **Particionamento**: `taxi_type`, `year`, `month`

### Análises Geradas
- **Pergunta 1**: Média de tarifas Yellow Taxis por mês
- **Pergunta 2**: Média de passageiros por hora em Maio
- **Exploratória**: Padrões temporais, distribuições, qualidade

### Artefatos Técnicos
- **Pipeline PySpark**: Código modular e reutilizável
- **Validações**: Relatórios de qualidade automatizados
- **Documentação**: Guias técnicos e de execução

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

### Execução Imediata (5 minutos)
1. Abra o Databricks Community Edition
2. Importe `quick_start.py`
3. Execute todas as células
4. Veja os resultados

### Execução Completa (30-45 minutos)
1. Clone o repositório
2. Importe os notebooks no Databricks
3. Execute na ordem: 01 → 02 → 03
4. Explore os resultados e análises

### Desenvolvimento Local
1. `pip install -r requirements.txt`
2. Configure ambiente Spark local
3. Adapte configurações em `config.py`
4. Execute scripts Python

## 📞 Suporte

- **Issues**: Problemas técnicos ou dúvidas
- **Documentação**: Consulte os arquivos `.md`
- **Código**: Comentários detalhados nos scripts
- **Logs**: Saídas detalhadas durante execução
