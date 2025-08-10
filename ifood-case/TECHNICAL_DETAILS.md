# Detalhes Técnicos - Case iFood Data Architect

## 🏗️ Arquitetura da Solução

### Visão Geral
```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Processing     │    │   Data Lake     │
│                 │    │                  │    │                 │
│ • NYC Taxi APIs │───▶│ • PySpark ETL    │───▶│ • Delta Lake    │
│ • Parquet Files │    │ • Data Quality   │    │ • Partitioned   │
│ • CloudFront    │    │ • Standardization│    │ • Optimized     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │    Analytics     │
                       │                  │
                       │ • Business Q&A   │
                       │ • Exploratory    │
                       │ • Visualizations │
                       └──────────────────┘
```

### Componentes Principais

#### 1. **Camada de Ingestão** (`src/data_ingestion.py`)
- **Responsabilidade**: Download e processamento inicial
- **Tecnologias**: PySpark, HTTP requests
- **Features**:
  - Retry automático com backoff exponencial
  - Padronização de schemas entre Yellow/Green taxis
  - Validação de dados em tempo real
  - Processamento distribuído

#### 2. **Camada de Qualidade** (`src/data_quality.py`)
- **Responsabilidade**: Validação e monitoramento
- **Features**:
  - Verificação de colunas obrigatórias
  - Análise de valores nulos e outliers
  - Validação de ranges de dados
  - Relatórios de qualidade automatizados

#### 3. **Camada de Armazenamento** (Delta Lake)
- **Tecnologia**: Delta Lake sobre Databricks
- **Características**:
  - ACID transactions
  - Schema evolution
  - Time travel
  - Otimizações automáticas (Z-ORDER, OPTIMIZE)

#### 4. **Camada de Análise** (`analysis/`)
- **Responsabilidade**: Business intelligence e insights
- **Componentes**:
  - Respostas às perguntas de negócio
  - Análise exploratória
  - Visualizações e relatórios

## 🔧 Decisões Técnicas

### Escolha do Delta Lake
**Justificativa**:
- ✅ ACID compliance para consistência
- ✅ Schema evolution para flexibilidade
- ✅ Performance otimizada para analytics
- ✅ Integração nativa com Databricks
- ✅ Suporte a time travel para auditoria

### Particionamento Estratégico
**Estratégia**: `taxi_type`, `year`, `month`
**Justificativa**:
- Consultas frequentes por tipo de táxi
- Análises temporais (mensal/anual)
- Otimização de pruning de partições
- Paralelização eficiente

### Padronização de Schema
**Problema**: Green taxis usam `lpep_*` vs Yellow `tpep_*`
**Solução**: Renomeação automática para `tpep_*`
**Benefício**: Schema unificado para análises

### Tratamento de Erros
**Estratégia**: Retry com backoff + logging detalhado
**Implementação**:
```python
for attempt in range(max_retries):
    try:
        # Processamento
        return success
    except Exception as e:
        if attempt < max_retries - 1:
            time.sleep(delay * (2 ** attempt))
        else:
            log_error_and_continue()
```

## 📊 Modelo de Dados

### Schema Final da Tabela
```sql
CREATE TABLE main.nyc_taxi.trips_delta (
    -- Colunas obrigatórias (conforme especificação)
    VendorID BIGINT,
    passenger_count DOUBLE,
    total_amount DOUBLE,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    
    -- Colunas adicionais úteis
    trip_distance DOUBLE,
    fare_amount DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    
    -- Metadados para particionamento e análise
    taxi_type STRING,
    year INT,
    month INT,
    year_month STRING,
    pickup_hour INT
)
USING DELTA
PARTITIONED BY (taxi_type, year, month)
```

### Qualidade dos Dados
**Métricas Monitoradas**:
- Taxa de valores nulos por coluna
- Outliers em tarifas e passageiros
- Consistência temporal (pickup < dropoff)
- Ranges válidos para campos numéricos

## ⚡ Otimizações Implementadas

### 1. **Spark Configurations**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
```

### 2. **Delta Lake Optimizations**
```sql
-- Compactação de arquivos pequenos
OPTIMIZE main.nyc_taxi.trips_delta

-- Z-ORDER para consultas por datetime e tipo
OPTIMIZE main.nyc_taxi.trips_delta 
ZORDER BY (tpep_pickup_datetime, taxi_type)

-- Estatísticas para otimizador
ANALYZE TABLE main.nyc_taxi.trips_delta COMPUTE STATISTICS
```

### 3. **Processamento Eficiente**
- Cache estratégico de DataFrames intermediários
- Union otimizado para combinação de datasets
- Seleção de colunas antes de joins
- Particionamento alinhado com padrões de consulta

## 🔍 Análises Implementadas

### Pergunta 1: Média Yellow Taxis
**Query Otimizada**:
```sql
SELECT 
    year_month,
    AVG(total_amount) as avg_fare,
    COUNT(*) as trips
FROM main.nyc_taxi.trips_delta 
WHERE taxi_type = 'yellow' 
  AND total_amount > 0
GROUP BY year_month
ORDER BY year_month
```

**Otimizações**:
- Filtro por partição (`taxi_type`)
- Predicate pushdown (`total_amount > 0`)
- Agregação distribuída

### Pergunta 2: Passageiros por Hora (Maio)
**Query Otimizada**:
```sql
SELECT 
    HOUR(tpep_pickup_datetime) as hour,
    AVG(passenger_count) as avg_passengers,
    COUNT(*) as trips
FROM main.nyc_taxi.trips_delta 
WHERE year_month = '2023-05'
  AND passenger_count > 0
GROUP BY HOUR(tpep_pickup_datetime)
ORDER BY hour
```

**Otimizações**:
- Filtro por partição (`year_month`)
- Função nativa `HOUR()` para extração
- Agregação por hora (24 grupos apenas)

## 🚀 Performance e Escalabilidade

### Métricas de Performance
- **Ingestão**: ~15-20 minutos para 5 meses de dados
- **Consultas**: Sub-segundo para agregações simples
- **Armazenamento**: ~60-70% de compressão vs CSV

### Escalabilidade Horizontal
- Processamento distribuído via Spark
- Particionamento para paralelização
- Auto-scaling do cluster Databricks

### Monitoramento
- Logs detalhados em cada etapa
- Contadores de sucesso/falha
- Métricas de qualidade automatizadas
- Relatórios de performance

## 🔒 Considerações de Segurança

### Acesso aos Dados
- Unity Catalog para governança
- Controle de acesso baseado em roles
- Auditoria de acessos

### Qualidade e Integridade
- Validações automáticas
- Checksums para integridade
- Versionamento via Delta Lake

## 📈 Próximas Melhorias

### Curto Prazo
- [ ] Implementar streaming para dados em tempo real
- [ ] Adicionar mais validações de qualidade
- [ ] Criar dashboards interativos

### Médio Prazo
- [ ] Machine Learning para detecção de anomalias
- [ ] Análises preditivas de demanda
- [ ] Integração com ferramentas de BI

### Longo Prazo
- [ ] Data mesh architecture
- [ ] Real-time analytics
- [ ] Advanced ML pipelines

## 🛠️ Ferramentas e Tecnologias

### Core Stack
- **PySpark**: Processamento distribuído
- **Delta Lake**: Storage layer
- **Databricks**: Plataforma de execução
- **Python**: Linguagem principal

### Bibliotecas Utilizadas
- `pyspark.sql`: DataFrames e SQL
- `pyspark.sql.functions`: Funções built-in
- `time`: Controle de retry
- `typing`: Type hints para qualidade

### Ambiente de Desenvolvimento
- **Databricks Community Edition**: Gratuito
- **Runtime 13.3 LTS**: Estável e otimizado
- **Auto-scaling clusters**: Performance adaptativa

## 📝 Documentação e Manutenibilidade

### Código Documentado
- Docstrings em todas as funções
- Comentários explicativos
- Type hints para clareza

### Estrutura Modular
- Separação de responsabilidades
- Configurações centralizadas
- Reutilização de componentes

### Testes e Validação
- Validações automáticas de dados
- Logs detalhados para debugging
- Relatórios de qualidade
