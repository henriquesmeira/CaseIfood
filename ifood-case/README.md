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

## Estrutura da Solução

### 1. Ingestão de Dados (src/)
- Scripts PySpark para download e processamento dos dados
- Criação de tabelas Delta Lake
- Validação e limpeza dos dados

### 2. Análises (analysis/)
- Notebooks com análises exploratórias
- Respostas às perguntas do desafio
- Visualizações e insights

## Tecnologias Utilizadas

- **PySpark**: Processamento distribuído dos dados
- **Delta Lake**: Armazenamento e versionamento
- **Databricks Community Edition**: Ambiente de execução
- **Python**: Linguagem principal

## Como Executar

### Pré-requisitos
- Conta no Databricks Community Edition (https://community.cloud.databricks.com/)
- Acesso à internet para download dos dados
- Cluster Databricks com runtime 13.3 LTS ou superior

### Opção 1: Execução via Notebooks (Recomendado)

1. **Clone este repositório**
   ```bash
   git clone <seu-repositorio>
   ```

2. **Importe os notebooks no Databricks**
   - Acesse seu workspace Databricks
   - Vá em "Workspace" → "Import"
   - Faça upload dos arquivos da pasta `notebooks/`
   - Ou importe diretamente do GitHub

3. **Execute os notebooks na ordem:**
   - `01_Data_Ingestion.py` - Ingestão completa dos dados
   - `02_Business_Analysis.py` - Respostas às perguntas do case
   - `03_Exploratory_Analysis.py` - Análise exploratória detalhada

### Opção 2: Execução via Scripts Python

1. **Configure o ambiente**
   ```bash
   pip install -r requirements.txt
   ```

2. **Execute o pipeline completo**
   ```python
   # No Databricks
   %run src/main_pipeline.py
   ```

3. **Execute análises específicas**
   ```python
   # Análises de negócio
   %run analysis/business_questions.py

   # Análise exploratória
   %run analysis/exploratory_analysis.py
   ```

### Estrutura de Execução

```
1. Ingestão de Dados (src/main_pipeline.py)
   ├── Download dos arquivos Parquet
   ├── Processamento e padronização
   ├── Criação da tabela Delta Lake
   └── Validação de qualidade

2. Análises de Negócio (analysis/business_questions.py)
   ├── Pergunta 1: Média Yellow Taxis
   ├── Pergunta 2: Passageiros por hora em Maio
   └── Insights adicionais

3. Análise Exploratória (analysis/exploratory_analysis.py)
   ├── Padrões temporais
   ├── Distribuições de tarifas
   ├── Qualidade dos dados
   └── Visualizações
```

