{{ config(materialized='table') }}

select
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    pickup_date,
    dropoff_date,
    cast(pickup_time as time)   as pickup_time,
    cast(dropoff_time as time)  as dropoff_time
from {{ source('taxi_project', 'YELLOW_TRIP_2020_DATA') }}
