# Arquitetura do Data Lake — NYC Yellow Taxi 2025

## Visão geral

Data lake batch construído sobre HDFS, processado com Spark e consultável
via Hive, usando como fonte os dados públicos de corridas de táxi de Nova
York (yellow taxi, ano completo de 2025).

```
┌─────────────┐     ┌──────────────┐     ┌───────────────┐     ┌──────────┐
│  NYC TLC    │────▶│  Camada RAW   │────▶│ Camada         │────▶│  Hive    │
│  (fonte)    │     │  (HDFS)       │     │ PROCESSED      │     │ (SQL)    │
└─────────────┘     └──────────────┘     │ (HDFS)         │     └──────────┘
                                          └───────────────┘
```

## Camadas do data lake

### Camada RAW

- **Localização (HDFS):** `/data-lake/raw/2025`
- **Origem:** download direto do CDN da NYC TLC (`d37ci6vzurychx.cloudfront.net`)
- **Formato:** Parquet, comprimido em **snappy** (convertido a partir do
  zstd original — ver seção "Decisões técnicas" abaixo)
- **Escopo:** yellow taxi, jan/2025 a dez/2025 (12 arquivos mensais)
- **Sem transformação:** os dados aqui são mantidos exatamente como
  vieram da fonte, sem limpeza nem enriquecimento

### Camada PROCESSED

- **Localização (HDFS):** `/data-lake/processed/yellow`
- **Particionamento:** `pickup_year` / `pickup_month`
- **Formato:** Parquet
- **Transformações aplicadas:**
  - Remoção de linhas com `trip_distance <= 0`
  - Remoção de linhas com `fare_amount < 0`
  - Remoção de linhas com dropoff anterior ao pickup
  - Remoção de linhas com `passenger_count == 0` explícito (nulos foram
    mantidos — ver justificativa abaixo)
  - Remoção de duplicadas (por `tpep_pickup_datetime`,
    `tpep_dropoff_datetime`, `PULocationID`, `DOLocationID`)
- **Colunas enriquecidas:**
  - `trip_duration.minutes`: duração da corrida em minutos
  - `pickup_time_of_day`: faixa do dia (madrugada/manhã/tarde/noite)
  - `pickup_year`, `pickup_month`: extraídas do pickup, usadas só para
    particionamento físico

### Camada analítica (Hive)

- **Tabela:** `yellow_taxi_processed` (externa, sobre a camada processed)
- **Consultas:** ver `hive/analytical_queries.hql`

## Decisões técnicas e por quê

### Por que remover passenger_count == 0, mas manter os nulos?

Na exploração (passo 5), ~24% das linhas tinham `passenger_count` nulo ou
zero — volume alto demais para ser tudo erro de registro. A contagem de
nulos da coluna batia quase exatamente com esse número, confirmando que a
maior parte é um campo **opcional não preenchido** pela fonte, não um erro
de fato. Descartar essas linhas jogaria fora dados válidos de distância,
valor e horário só por falta de um campo secundário. Só o valor `0`
explícito (diferente de nulo) foi tratado como erro de registro e removido.

### Por que reconverter de zstd para snappy?

Os parquets da NYC TLC vêm comprimidos em zstd. A build do `libhadoop`
usada pelas imagens Docker deste projeto (Hadoop 3.2, via `bde2020`) não
tem suporte nativo a zstd — essa é uma limitação conhecida do próprio
Apache Hadoop, não específica dessa imagem. A solução foi reconverter os
arquivos localmente com `pyarrow` (que não depende do `libhadoop`) antes
da ingestão no HDFS.

### Por que particionar por pickup_year/pickup_month?

Consultas analíticas comuns sobre dados de corridas costumam filtrar por
período (ex.: "corridas de março"). Particionar fisicamente por
ano/mês permite ao Hive fazer **partition pruning** — ler só as pastas
relevantes no HDFS em vez da tabela inteira, acelerando consultas que
filtram por data.

## Estrutura do repositório

```
data-lake-nyc-taxi-hdfs-spark-hive/
├── infra/                     # docker-compose.yml, hadoop.env
├── data/raw/                  # amostra local dos dados brutos (fora do Git)
├── jobs/spark/                # jobs PySpark (exploração, transformação)
├── hive/                      # DDL e consultas HiveQL
├── scripts/                   # scripts Bash de orquestração
└── docs/                      # esta documentação
```

## Como reproduzir o ambiente do zero

1. `cd infra && docker compose up -d` — sobe HDFS, Spark, Hive
   (aguardar ~40s para o Hive Metastore inicializar completamente)
2. `./scripts/run_pipeline.sh` — roda o pipeline completo:
   download → conversão de compressão → ingestão HDFS → transformação
   Spark → criação da tabela Hive → consultas analíticas

## Problemas conhecidos / pontos de atenção

- `depends_on` no `docker-compose.yml` não garante que um serviço esteja
  *pronto*, só que foi *iniciado* — por isso os serviços do Hive têm
  healthcheck configurado, evitando erro de conexão recusada ao subir o
  ambiente do zero.
- Consultas Hive rodam sobre o motor **MapReduce** por padrão (mais lento
  que Spark/Tez) — aceitável para o volume de dados deste projeto, mas
  vale considerar migrar para Hive-on-Spark em um projeto de maior escala.