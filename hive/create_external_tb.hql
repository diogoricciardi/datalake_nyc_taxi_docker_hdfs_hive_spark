
-- "External" (EXTERNAL TABLE) significa que o Hive só aponta para os dados que já existem no HDFS, ele não copia nem move nada. Se essa tabela for apagada (DROP TABLE), 
-- os arquivos parquet continuam intactos no HDFS; só o mapeamento lógico do Hive é removido.

-- Como rodar (dentro do container hive-server, via beeline):
-- docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -f /hive/create_external_table.sql

CREATE EXTERNAL TABLE IF NOT EXISTS processed_nyc_taxi_rides (
    VendorID               INT,
    tpep_pickup_datetime   TIMESTAMP,
    tpep_dropoff_datetime  TIMESTAMP,
    passenger_count        BIGINT,
    trip_distance          DOUBLE,
    RatecodeID             BIGINT,
    store_and_fwd_flag     STRING,
    PULocationID           INT,
    DOLocationID           INT,
    payment_type           BIGINT,
    fare_amount            DOUBLE,
    extra                  DOUBLE,
    mta_tax                DOUBLE,
    tip_amount             DOUBLE,
    tolls_amount           DOUBLE,
    improvement_surcharge  DOUBLE,
    total_amount           DOUBLE,
    congestion_surcharge   DOUBLE,
    Airport_fee            DOUBLE,
    cbd_congestion_fee     DOUBLE,
    trip_duration_minutes  DOUBLE, 
    pickup_time_of_day     STRING
) 

PARTITIONED BY (
    pickup_year INT,
    pickup_month INT
)

STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/data-lake/processed/yellow';

-- O Hive não descobre partições existentes sozinho, precisa ser avisado explicitamente para "escanear" a estrutura de pastas do HDFS e registrar
-- cada combinação de pickup_year/pickup_month como uma partição válida.

MSCK REPAIR TABLE processed_nyc_taxi_rides;

SHOW PARTITIONS processed_nyc_taxi_rides;