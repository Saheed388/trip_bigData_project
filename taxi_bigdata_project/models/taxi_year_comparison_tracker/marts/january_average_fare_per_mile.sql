{{ config(materialized='table') }}

-- Average Fare per Mile in January (2019–2021) using joins to date tables

SELECT 
    '2019' AS year, 
    'January' AS month, 
    AVG(f.fare_amount / NULLIF(f.trip_distance, 0)) AS avg_fare_per_mile
FROM {{ ref('fact_table_2019_stg') }} f
JOIN {{ ref('date_2019_data') }} d
    ON f.vendorid = d.vendorid
WHERE EXTRACT(MONTH FROM d.tpep_pickup_datetime) = 1
GROUP BY 1

UNION ALL

SELECT 
    '2020' AS year, 
    'January' AS month, 
    AVG(f.fare_amount / NULLIF(f.trip_distance, 0)) AS avg_fare_per_mile
FROM {{ ref('fact_table_2020_stg') }} f
JOIN {{ ref('date_2020_data') }} d
    ON f.vendorid = d.vendorid
WHERE EXTRACT(MONTH FROM d.tpep_pickup_datetime) = 1
GROUP BY 1

UNION ALL

SELECT 
    '2021' AS year, 
    'January' AS month, 
    AVG(f.fare_amount / NULLIF(f.trip_distance, 0)) AS avg_fare_per_mile
FROM {{ ref('fact_table_2021_stg') }} f
JOIN {{ ref('date_2021_data') }} d
    ON f.vendorid = d.vendorid
WHERE EXTRACT(MONTH FROM d.tpep_pickup_datetime) = 1
GROUP BY 1