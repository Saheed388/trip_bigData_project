{{ config(materialized='table') }}

select '2019' as year, avg(trip_distance) as avg_trip_distance
from {{ ref( 'fact_table_2019_stg') }}

union all

select '2020' as year, avg(trip_distance) as avg_trip_distance
from {{ ref( 'fact_table_2020_stg') }}

union all

select '2021' as year, avg(trip_distance) as avg_trip_distance
from {{ ref('fact_table_2021_stg') }}
