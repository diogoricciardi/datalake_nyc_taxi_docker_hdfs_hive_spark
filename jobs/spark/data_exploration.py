from pyspark.sql import SparkSession
from pyspark.sql import functions as f

HDFS_RAW_PATH = "hdfs://namenode:8020/data-lake/raw/2025"

def main():
    # appName é só um rótulo pra identificar o job na UI do Spark (http://localhost:8080)
    # getOrCreate() reaproveita uma sessão existente ou cria uma nova.
    spark = SparkSession.builder.appName("exp_nyc_yellow_taxi_2025").getOrCreate()

    # reduz o volume de logs técnicos do Spark no terminal, deixando só avisos e erros
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 80)
    print(f"Reading data from {HDFS_RAW_PATH}")
    print("=" * 80)

    # lê todos os arquivos no diretório do hdfs
    df = spark.read.parquet(HDFS_RAW_PATH)
    
    # imprime o schema atribuido pelo Spark
    print("\n--- SCHEMA ---")
    df.printSchema()
    
    ''' 
    contagem total de linhas
    .count() é uma "ação" (action) — dispara o processamento distribuído de fato. Operações como .select() ou .filter() são "transformações"
    (transformations) e só são executadas de forma preguiçosa (lazy evaluation) quando uma ação como count(), show() ou write() é chamada. 
    '''
    total_rows = df.count()
    print(f"--- Total quantity of rows: {total_rows} ---")
    
    '''
    contagem de nulos por coluna
    cast("int") transforma o retorno booleano em 0/1
    tecnicamente, toda transformação gera uma nova coluna que o Spark nomeia com uma representação textual da operação. O .alias(c) resolve isso
    atribuindo o nome original da coluna
    '''
    print("\n--- Null values by column ---")
    null_counts = df.select(
        [f.sum(f.col(c).isNull().cast("int")).alias(c) for c in df.columns]
    )
    # por padrão o Spart corta o conteúdo de cada célula em 20 caracteres. truncate=False remove isso
    # vertical=True inverte a exibição: em vez de colunas lado a lado, ele imprime uma linha por campo
    null_counts.show(truncate=False, vertical=True)
    
    
    print("\n--- Descriptive Statistics ---")
    num_cols = [
        'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax','tip_amount', 'tolls_amount', 'improvement_surcharge', 
        'total_amount', 'congestion_surcharge', 'Airport_fee', 'cbd_congestion_fee'
    ]
    existing_cols = [c for c in num_cols if c in df.columns]
    # convertendo em dataframe para melhorar a leitura
    stats = df.select(existing_cols).describe()
    # * é o operador de unpacking (desempacotamento) do Python. .select() espera receber os argumentos separados por vírgula, 
    # não uma lista única. Sem o *, você estaria passando a lista inteira como um único argumento
    rounded_stats = stats.select("Summary", *[f.round(f.col(c).cast("double"), 2).alias(c) for c in existing_cols])
    rounded_stats.show(truncate=False)
    
    '''
    checagem de qualidade específicas do domínio
    '''
    print("\n--- DATA QUALITY CHECKS ---")
    
    # conta distância inválida
    if 'trip_distance' in df.columns:
        invalid_distance = df.filter(f.col('trip_distance') <= 0).count()
        print(f"Number of registers with trip distance lower than 0: {invalid_distance}")
    
    # conta valor da corrida menor que zero
    if 'fare_amount' in df.columns:
        negative_fare_amount = df.filter(f.col('fare_amount') < 0).count()
        print(f"Number of registers with negative fare amount: {negative_fare_amount}")
        
    # conta número registros com número de passageiros nulo ou igual a 0
    if 'passenger_count' in df.columns:
        no_passenger = df.filter(
            (f.col('passenger_count').isNull()) | (f.col('passenger_count') == 0) 
        ).count()
        print(f"Number of registers with no passengers: {no_passenger}")
        
    # dropoff antes de pickup
    if all(c in df.columns for c in ['tpep_pickup_datetime', 'tpep_dropoff_datetime']):
        drop_before_pick = df.filter(f.col('tpep_dropoff_datetime') < f.col('tpep_pickup_datetime')).count()
        print(f"Rides with dropoff before pickup: {drop_before_pick}")
    
    '''
    linhas duplicadas
    '''
    key_cols = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID"]
    existing_key_cols = [c for c in key_cols if c in df.columns]
    distinct_rows = df.select(existing_key_cols).distinct().count()
    duplicated_rows = total_rows - distinct_rows
    print(f"Count of duplicated rows: {duplicated_rows}")
    
    print("\n" + "=" * 80)
    print(f"Exploration completed")
    print("=" * 80)
    
    spark.stop
    
if __name__ == "__main__":
    main()
    
    