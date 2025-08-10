# Case Técnico Data Architect - iFood

## Objetivo

Este desafio permitirá que você demonstre suas habilidades em Engenharia de Dados/Software, Análise e Modelagem de Dados.

Neste case técnico, você deverá fazer a ingestão de alguns dados em nosso Data Lake e pensar em uma forma de disponibilizá-los para os consumidores. Para finalizar, você deverá realizar análises sobre os dados disponibilizados.

## Dados Disponíveis

Os dados estão disponíveis no site da agência responsável por licenciar e regular os táxis na cidade de NY: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Num primeiro momento, precisamos que sejam armazenados e disponibilizados os dados de Janeiro a Maio de 2023.

## Estrutura do Repositório

```
ifood-case/
├── src/                    # Código fonte da solução
├── analysis/              # Scripts/Notebooks com as respostas das perguntas
├── README.md              # Este arquivo
└── requirements.txt       # Dependências do projeto
```

## Considerações

- Você pode considerar inicialmente armazenar todos os arquivos originais em uma landing zone (que pode ser um bucket do S3, por exemplo, ou qualquer outra tecnologia de sua escolha);

- Você pode considerar manipular/limpar os dados que julgar necessário;

- Você precisa garantir que as colunas **VendorID**, **passenger_count**, **total_amount**, **tpep_pickup_datetime** e **tpep_dropoff_datetime** estejam presentes na camada de consumo. As outras colunas podem ser ignoradas;

- Você pode considerar que no Data Lake não existe nenhuma tabela criada, portanto, precisam ser modeladas e criadas.

## O Desafio

Você deverá entregar:

1. **Solução para ler os dados originais, fazer a ingestão no Data Lake e disponibilizar aos usuários finais**
   - Deve utilizar PySpark em alguma etapa;
   - Recomendamos usar Databricks Community Edition (https://community.cloud.databricks.com/);
   - A escolha da tecnologia de metadados fica a seu critério;
   - A escolha da linguagem de consulta (SQL, PySpark e etc) para os usuários finais fica a seu critério;

2. **Código SQL ou PySpark estruturado da forma que preferir com as respostas para as seguintes perguntas:**
   - Qual a média de valor total (total_amount) recebido em um mês considerando todos os yellow taxis da frota?
   - Qual a média de passageiros (passenger_count) por cada hora do dia que pegaram táxi no mês de maio considerando todos os táxis da frota?

## Critérios de Avaliação

Serão avaliados:
- Qualidade e organização do código
- Processo de análise exploratória
- Justificativa das escolhas técnicas
- Criatividade na solução proposta
- Clareza na comunicação dos resultados

## Instruções de Execução

1. Crie um repositório público ou privado no GitHub
2. Desenvolva sua solução
3. Atualize o README com instruções de execução
4. Envie o link do seu repositório

## Estrutura da Solução V2

### 🏗️ Arquitetura Implementada
```
Python (Extração) → PySpark (Consolidação) → SQL (Análises)
```

### 1. Extração Python (src/)
- Download robusto via requests
- Upload para DBFS
- Validação de integridade

### 2. Consolidação PySpark (src/)
- **Raw Layer**: Dados brutos
- **Bronze Layer**: Padronizados e limpos
- **Silver Layer**: Enriquecidos e validados
- **Gold Layer**: Agregados para análises

### 3. Análises SQL (sql/)
- Consultas otimizadas na camada Gold
- Respostas às perguntas do case
- Insights de negócio

## Tecnologias Utilizadas V2

- **Python**: Extração de dados (requests, os, time)
- **PySpark**: Consolidação em 4 camadas Delta Lake
- **SQL**: Análises finais otimizadas
- **Delta Lake**: Armazenamento ACID com particionamento
- **Databricks Community Edition**: Ambiente de execução

## Como Executar

### Pré-requisitos
- Conta no Databricks Community Edition (https://community.cloud.databricks.com/)
- Acesso à internet para download dos dados
- Cluster Databricks com runtime 13.3 LTS ou superior

### Opção 1: Execução via Notebooks V2 (Recomendado)

1. **Clone este repositório**
   ```bash
   git clone <seu-repositorio>
   ```

2. **Importe os notebooks V2 no Databricks**
   - Acesse seu workspace Databricks
   - Vá em "Workspace" → "Import"
   - Faça upload dos arquivos da pasta `notebooks/`
   - Ou importe diretamente do GitHub

3. **Execute os notebooks V2 na ordem:**
   - `01_Data_Extraction_Python.py` - Extração via Python
   - `02_Data_Consolidation_PySpark.py` - Consolidação 4 camadas
   - `03_Business_Analysis_SQL.py` - Análises SQL finais

### Opção 2: Execução via Pipeline V2

1. **Configure o ambiente**
   ```bash
   pip install -r requirements.txt
   ```

2. **Execute o pipeline completo V2**
   ```python
   # No Databricks
   %run src/main_pipeline_v2.py
   ```

3. **Execute consultas SQL**
   ```sql
   -- Carregue e execute
   %run sql/business_questions.sql
   ```

### Estrutura de Execução

```
1. Extração Python (src/data_extraction.py)
   ├── Download via requests
   ├── Upload para DBFS
   ├── Validação de integridade
   └── Preparação para PySpark

2. Consolidação PySpark (src/data_consolidation.py)
   ├── Raw Layer (dados brutos)
   ├── Bronze Layer (padronizados)
   ├── Silver Layer (enriquecidos)
   ├── Gold Layer (agregados)
   └── Otimização Delta Lake

3. Análises SQL (sql/business_questions.sql)
   ├── Pergunta 1: Média Yellow Taxis
   ├── Pergunta 2: Passageiros por hora em Maio
   ├── Análises complementares
   └── Insights de negócio
```

