'''
Regras de limpeza aplicadas:
  - trip_distance <= 0                          -> remove (erro de registro)
  - fare_amount < 0                             -> remove (estornos/cancelamentos)
  - dropoff antes do pickup                     -> remove (erro de registro)
  - passenger_count == 0 (explícito, não nulo)  -> remove (valor inválido)
  - passenger_count nulo                        -> MANTÉM (campo opcional, não é erro)
  - linhas duplicadas (por colunas-chave)        -> remove, mantém a primeira ocorrência
 
Enriquecimento adicionado:
  - trip_duration_minutes: duração da corrida em minutos
  - time_of_day: faixa do dia em que a corrida começou (madrugada/manhã/tarde/noite)
  - pickup_year / pickup_month: usados para particionamento no HDFS
 
Como rodar:
  docker exec spark-master /spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --executor-memory 3g \
      /jobs/spark/data_transformation.py
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

HDFS_RAW_PATH = 'hdfs://namenode:8020/data-lake/raw/2025'
HDFS_PROCESSED_PATH = 'hdfs://namenode:8020/data-lake/processed/yellow'

def main():
    
    spark = SparkSession.builder.appName('transform_nyc_yellow_taxi_2025').getOrCreate()   
    spark.sparkContext.setLogLevel('WARN')
    
    print('=' * 80)
    print(f'Reading raw data from: {HDFS_RAW_PATH}')
    print('=' * 80)
    
    df = spark.read.parquet(HDFS_RAW_PATH)
    print(f'Total rows in raw layer: {df.count()}. Starting transformations...')
    
    df_filtered = df.filter(
        # trip_distance <= 0 -> remove (erro de registro)
        (f.col('trip_distance') > 0) & 
        # fare_amount < 0 -> remove (estornos/cancelamentos)
        (f.col('fare_amount') > 0) & 
        # dropoff antes do pickup -> remove (erro de registro)
        (f.col('tpep_dropoff_datetime') > f.col('tpep_pickup_datetime')) & 
        # passenger_count == 0 (explícito, não nulo) -> remove (valor inválido)
        ~(f.col('passenger_count') == 0)
    )
    
    # linhas duplicadas (por colunas-chave) -> remove, mantém a primeira ocorrência
    key_cols = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID"]
    df_distinct = df_filtered.dropDuplicates(subset=key_cols)
    
    # enriquecimento
    df_enriched = (
        df_distinct
            # + trip_duration_minutes: duração da corrida em minutos
            .withColumn(
                colName = 'trip_duration.minutes',
                col = f.round(
                    (f.unix_timestamp('tpep_dropoff_datetime') - f.unix_timestamp('tpep_pickup_datetime')) / 60
                    , 2
                )
            )
            # + time_of_day: faixa do dia em que a corrida começou (madrugada/manhã/tarde/noite)
            .withColumn(
                colName = 'pickup_time_of_day',
                col = f.when(f.hour('tpep_pickup_datetime').between(0, 5), 'night')
                .when(f.hour('tpep_pickup_datetime').between(6, 11), 'morning')
                .when(f.hour('tpep_pickup_datetime').between(12, 17), 'afternoon')
                .otherwise('evening')
            )
            # pickup_year
            .withColumn(
                colName = 'pickup_year',
                col = f.year('tpep_pickup_datetime')
            )
            # pickup month
            .withColumn(
                colName = 'pickup_month',
                col = f.month('tpep_pickup_datetime')
            )
    )
    
    print(f'Transformations completed. Total rows after transformations: {df_enriched.count()}')
    print(f'Removed rows: {df.count() - df_enriched.count()}')
    
    print(f'\n--- FINAL SCHEMA ---')
    df_enriched.printSchema()
    
    '''
    Persistência particionada no HDFS
    partitionBy grava os dados em subpastas físicas por ano/mês (ex.: .../pickup_year=2025/pickup_month=3/...), o que acelera muito
    consultas futuras no Hive que filtrem por período.
    '''
    
    print(f'Writting processed data in: {HDFS_PROCESSED_PATH}')
    df_enriched.write.mode('overwrite').partitionBy('pickup_year', 'pickup_month').parquet(HDFS_PROCESSED_PATH)
    
    print("\n" + "=" * 80)
    print("Data written to HDFS successfully.")
    print("=" * 80)
    
    spark.stop
    
if __name__ == '__main__':
    main()