-- Consultas analiticas em HiveQL sobre a tabela
-- processed_nyc_taxi_rides, respondendo perguntas de negocio definidas no
-- escopo do projeto (horarios de pico, bairros com maior demanda, relacao entre
-- distancia/duracao/valor da corrida).
--
-- Como rodar:
--   docker exec hive-server beeline -u jdbc:hive2://localhost:10000 \
--       -f /hive/analytical_queries.hql

USE default;

-- Horarios de pico: quantidade de corridas por faixa do dia
SELECT
    pickup_time_of_day,
    COUNT(*) AS total_rides,
    ROUND(AVG(fare_amount), 2) AS avg_amount,
    ROUND(AVG(trip_distance), 2) AS avg_distance
FROM processed_nyc_taxi_rides
GROUP BY pickup_time_of_day
ORDER BY total_rides DESC;

-- Horarios de pico, em granularidade de hora do dia (0 a 23)
SELECT
    HOUR(tpep_pickup_datetime) AS pickup_hour,
    COUNT(*) AS total_rides
FROM processed_nyc_taxi_rides
GROUP BY HOUR(tpep_pickup_datetime)
ORDER BY pickup_hour;

-- Zonas com maior demanda de embarque
SELECT
    PULocationID,
    COUNT(*) AS total_pickups
FROM processed_nyc_taxi_rides
GROUP BY PULocationID
ORDER BY total_pickups DESC

-- Relacao entre distancia, duracao e valor da corrida
SELECT
    CASE
        WHEN trip_distance < 1 THEN '0-1 miles'
        WHEN trip_distance < 3 THEN '1-3 miles'
        WHEN trip_distance < 6 THEN '3-6 miles'
        WHEN trip_distance < 10 THEN '6-10 miles'
        ELSE '10+ miles'
    END AS distance_range,
    COUNT(*) AS total_rides,
    ROUND(AVG(`trip_duration.minutes`), 2) AS avg_trip_duration,
    ROUND(AVG(fare_amount), 2) AS avg_amount,
    ROUND(AVG(fare_amount / NULLIF(trip_distance, 0)), 2) AS avg_amount_by_mile
FROM processed_nyc_taxi_rides
GROUP BY
    CASE
        WHEN trip_distance < 1 THEN '0-1 miles'
        WHEN trip_distance < 3 THEN '1-3 miles'
        WHEN trip_distance < 6 THEN '3-6 miles'
        WHEN trip_distance < 10 THEN '6-10 miles'
        ELSE '10+ miles'
    END
ORDER BY total_rides DESC;

-- Sazonalidade: total de corridas e receita por mes
SELECT
    pickup_month,
    COUNT(*) AS total_rides,
    ROUND(SUM(total_amount), 2) AS total_amount,
    ROUND(AVG(total_amount), 2) AS avg_amount
FROM processed_nyc_taxi_rides
WHERE pickup_year = 2025
GROUP BY pickup_month
ORDER BY pickup_month;
