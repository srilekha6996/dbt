{{ config(materialized='table') }}
select
  pickup_date,
  pickup_ts
from {{ ref('vw_trips_clean') }}
limit 10