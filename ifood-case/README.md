# iFood Case - Data Architect

## Visão Geral

Este projeto implementa uma solução completa de engenharia de dados para análise dos dados de táxis de NYC, respondendo às perguntas específicas do case técnico iFood para a posição de Data Architect.

### Objetivos Principais

1. **Ingestão de Dados**: Pipeline robusto para download e processamento dos dados NYC Taxi
2. **Análise de Negócio**: Respostas precisas às perguntas do case técnico
3. **Arquitetura Escalável**: Implementação usando PySpark e Delta Lake
4. **Qualidade de Dados**: Validação e limpeza automatizadas

### Perguntas do Case Respondidas

1. **Pergunta 1**: Qual a média de valor total recebido em um mês considerando todos os yellow taxis da frota?
2. **Pergunta 2**: Qual a média de passageiros por cada hora do dia que pegaram táxi no mês de maio considerando todos os táxis da frota?

### Arquitetura da Solução

```
Dados NYC Taxi → Processamento PySpark → Delta Lake → Análise SQL
```

## Estrutura do Projeto V2

```
ifood-case/
├── src/                              # Código fonte V2
│   ├── config.py                     # Configuração das 4 camadas
│   ├── data_extraction.py            # Extração Python
│   ├── data_consolidation.py         # Consolidação PySpark
│   └── main_pipeline_v2.py           # Orquestrador V2
├── analysis/                        # Notebooks V2
    ├── sql/                              # Consultas SQL
│   └── business_questions.sql        # Respostas finais
│   ├── 01_Data_Extraction_Python.py
│   ├── 02_Data_Consolidation_PySpark.py
│   └── 03_Business_Analysis_SQL.py
└── docs/                             # Documentação
```

## Estrutura da Solução V2

### Arquitetura Implementada
```
Python (Extração) → PySpark (Consolidação) → SQL (Análise)
```

### 1. Extração Python (src/)
- Download robusto via requests
- Upload para DBFS
- Validação de integridade

### 2. Consolidação PySpark (src/)
- **Camada Raw**: Dados brutos
- **Camada Bronze**: Padronizados e limpos
- **Camada Silver**: Enriquecidos e validados
- **Camada Gold**: Agregados para análise

### 3. Análise SQL (sql/)
- Consultas otimizadas na camada Gold
- Respostas às perguntas do case
- Insights de negócio

## Tecnologias Utilizadas V2

- **Python**: Extração de dados (requests, os, time)
- **PySpark**: Consolidação Delta Lake em 4 camadas
- **SQL**: Análise final otimizada
- **Delta Lake**: Armazenamento ACID com particionamento
- **Databricks Community Edition**: Ambiente de execução

## Como Executar

### Opção 1: Execução dos Notebooks V2 (Recomendado)

1. **Clone este repositório**
   ```bash
   git clone <seu-repositorio>
   ```

2. **Importe os notebooks V2 para o Databricks**
   - Acesse seu workspace Databricks
   - Vá em "Workspace" → "Import"
   - Faça upload dos arquivos da pasta `notebooks/`
   - Ou importe diretamente do GitHub

3. **Execute os notebooks V2 na ordem:**
   - `01_Data_Extraction_Python.py` - Extração Python
   - `02_Data_Consolidation_PySpark.py` - Consolidação em 4 camadas
   - `03_Business_Analysis_SQL.py` - Análise SQL final

### Opção 2: Execução do Pipeline V2

1. **Configure o ambiente**
   ```bash
   pip install -r requirements.txt
   ```

2. **Execute o pipeline V2 completo**
   ```python
   # No Databricks
   %run src/main_pipeline_v2.py
   ```

3. **Execute as consultas SQL**
   ```sql
   -- Carregue e execute
   %run sql/business_questions.sql
   ```

## Fluxo do Pipeline

```
1. Extração Python (src/data_extraction.py)
   ├── Download via requests
   ├── Upload para DBFS
   ├── Validação de integridade
   └── Preparação para PySpark

2. Consolidação PySpark (src/data_consolidation.py)
   ├── Camada Raw (dados brutos)
   ├── Camada Bronze (padronizados)
   ├── Camada Silver (enriquecidos)
   ├── Camada Gold (agregados)
   └── Otimização Delta Lake

3. Análise SQL (sql/business_questions.sql)
   ├── Pergunta 1: Média Táxi Amarelo
   ├── Pergunta 2: Passageiros por hora em Maio
   ├── Análises complementares
   └── Insights de negócio
```

## Resultados

### Dados Processados (4 Camadas)
- **Raw**: `main.nyc_taxi.raw_trips` (dados brutos)
- **Bronze**: `main.nyc_taxi.bronze_trips` (padronizados)
- **Silver**: `main.nyc_taxi.silver_trips` (enriquecidos)
- **Gold**: `main.nyc_taxi.gold_trips` (agregados para análise)

### Análises Geradas
- **Pergunta 1**: Média de tarifa táxi amarelo (SQL otimizado)
- **Pergunta 2**: Passageiros por hora em Maio (SQL otimizado)
- **Complementares**: Insights e análises adicionais

### Artefatos Técnicos V2
- **Extração Python**: Download robusto com retry
- **Consolidação PySpark**: Delta Lake em 4 camadas
- **Análise SQL**: Consultas otimizadas na camada Gold
- **Documentação**: Guias atualizados para V2

## Fontes de Dados

Os dados vêm da NYC Taxi & Limousine Commission:
- **URL**: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- **Período**: Janeiro a Maio 2023
- **Tipos**: Táxis amarelos e verdes
- **Formato**: Arquivos Parquet

## Características Principais

- **Tratamento Robusto de Erros**: Lógica de retry e fallbacks
- **Arquitetura Escalável**: Design de data lake em 4 camadas
- **Otimização de Performance**: Delta Lake com particionamento
- **Qualidade de Dados**: Validação e limpeza automatizadas
- **Foco no Negócio**: Respostas diretas às perguntas do case

## Requisitos

- Databricks Community Edition (ou workspace completo)
- Python 3.8+
- PySpark 3.x
- Suporte ao Delta Lake

## Tempo de Execução

- **Execução Sequencial V2**: 30-40 minutos
- **Pipeline Completo V2**: 35-45 minutos
- **Notebooks Individuais**: 10-20 minutos cada

## Próximos Passos

1. Execute os notebooks na ordem (01 → 02 → 03)
2. Ou execute `main_pipeline_v2.py` para pipeline completo
3. Explore as tabelas Delta criadas
4. Execute consultas SQL customizadas em `sql/business_questions.sql`

## Conclusão do Case Técnico

Este projeto demonstra:
- **Engenharia de Dados**: Pipeline ETL completo
- **Expertise PySpark**: Transformações e otimizações avançadas
- **Proficiência SQL**: Consultas analíticas complexas
- **Design de Arquitetura**: Data lake escalável em 4 camadas
- **Entendimento de Negócio**: Respostas diretas às perguntas do case

A solução está pronta para produção e segue as melhores práticas de engenharia de dados.
