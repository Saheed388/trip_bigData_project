{{ config(materialized='table') }}

select '2019' as year,
    avg(fare_amount / nullif(trip_distance, 0)) as avg_fare_per_mile
from {{ ref('fact_table_2019_stg') }}

union all

select '2020' as year,
    avg(fare_amount / nullif(trip_distance, 0)) as avg_fare_per_mile
from {{ ref('fact_table_2020_stg') }}

union all


select '2021' as year,
    avg(fare_amount / nullif(trip_distance, 0)) as avg_fare_per_mile
from {{ ref('fact_table_2021_stg') }}