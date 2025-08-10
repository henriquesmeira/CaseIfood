# Guia de Execução - Case Técnico iFood

## 🚀 Execução Rápida (Databricks)

### 1. Preparação do Ambiente

1. **Acesse o Databricks Community Edition**
   - Vá para: https://community.cloud.databricks.com/
   - Faça login ou crie uma conta gratuita

### 2. Importação dos Notebooks

**Opção A: Upload Manual**
1. Baixe os arquivos da pasta `notebooks/`
2. No Databricks: "Workspace" → "Import"
3. Faça upload dos 3 notebooks

**Opção B: Import do GitHub**
1. No Databricks: "Workspace" → "Import"
2. Selecione "URL" e cole o link do repositório
3. Importe a pasta `notebooks/`

### 3. Execução Sequencial

Execute os notebooks **nesta ordem exata**:

#### 📥 Notebook 1: Data Ingestion
- **Arquivo**: `01_Data_Ingestion.py`
- **Tempo estimado**: 15-20 minutos
- **O que faz**:
  - Baixa dados de táxi NYC (Jan-Mai 2023)
  - Cria tabela Delta Lake particionada
  - Valida qualidade dos dados

#### 📊 Notebook 2: Business Analysis
- **Arquivo**: `02_Business_Analysis.py`
- **Tempo estimado**: 5-10 minutos
- **O que faz**:
  - Responde às perguntas do case técnico
  - Calcula médias de tarifas Yellow Taxis
  - Analisa passageiros por hora em Maio

#### 🔍 Notebook 3: Exploratory Analysis
- **Arquivo**: `03_Exploratory_Analysis.py`
- **Tempo estimado**: 10-15 minutos
- **O que faz**:
  - Análise exploratória completa
  - Padrões temporais e sazonais
  - Visualizações e insights

## 📋 Checklist de Execução

### ✅ Antes de Começar
- [ ] Conta Databricks criada e ativa
- [ ] Cluster iniciado e funcionando
- [ ] Notebooks importados no workspace
- [ ] Conexão com internet estável

### ✅ Durante a Execução
- [ ] Notebook 1 executado sem erros
- [ ] Tabela `main.nyc_taxi.trips_delta` criada
- [ ] Dados validados (contagem de registros)
- [ ] Notebook 2 executado com respostas
- [ ] Notebook 3 executado com análises

### ✅ Após a Execução
- [ ] Todas as células executadas com sucesso
- [ ] Respostas às perguntas documentadas
- [ ] Insights identificados e documentados
- [ ] Tabela Delta otimizada

## 🔧 Solução de Problemas

### Erro: "Table not found"
**Causa**: Notebook 1 não foi executado ou falhou
**Solução**: 
1. Execute completamente o Notebook 1
2. Verifique se a tabela foi criada: `SHOW TABLES IN main.nyc_taxi`

### Erro: "RETRIES_EXCEEDED"
**Causa**: Problemas de conectividade de rede
**Solução**:
1. Aguarde alguns minutos
2. Execute novamente a célula que falhou
3. Se persistir, reinicie o cluster

### Erro: "Memory issues"
**Causa**: Cluster com pouca memória
**Solução**:
1. Reinicie o cluster
2. Execute os notebooks em horários de menor uso
3. Execute uma célula por vez se necessário

### Dados não carregando
**Causa**: URLs dos dados podem estar temporariamente indisponíveis
**Solução**:
1. Verifique conectividade
2. Tente novamente após alguns minutos
3. Verifique se as URLs estão corretas

## 📊 Resultados Esperados

### Após Notebook 1 (Ingestão)
```
✅ Tabela criada: main.nyc_taxi.trips_delta
📊 Registros processados: ~13-15 milhões
🗂️ Particionamento: taxi_type, year, month
```

### Após Notebook 2 (Análises)
```
🎯 Pergunta 1: Média Yellow Taxis = ~$18-20
🎯 Pergunta 2: Tabela com 24 horas detalhadas
📈 Insights adicionais documentados
```

### Após Notebook 3 (Exploratória)
```
💡 Padrões temporais identificados
📊 Distribuições analisadas
🔍 Qualidade dos dados validada
📈 Visualizações geradas
```

## 🎯 Perguntas do Case - Respostas Rápidas

### Pergunta 1: Média Yellow Taxis
**Localização**: Notebook 2, Seção 3
**Resposta**: Média de ~$18-20 por viagem (Jan-Mai 2023)

### Pergunta 2: Passageiros por Hora (Maio)
**Localização**: Notebook 2, Seção 4
**Resposta**: Tabela detalhada com 24 horas, pico entre 18h-19h


## 🏁 Conclusão

Após executar todos os notebooks, você terá:

- ✅ Data Lake completo com dados NYC Taxi
- ✅ Respostas às perguntas do case técnico
- ✅ Análise exploratória detalhada
- ✅ Insights de negócio documentados
- ✅ Código PySpark estruturado e documentado

**Tempo total estimado**: 30-45 minutos
