{{ config(materialized='table') }}

select
    VENDORID,
    STORE_AND_FWD_FLAG,
from {{ source('taxi_project', 'YELLOW_TRIP_2021_DATA') }}
