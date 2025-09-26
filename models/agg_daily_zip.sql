{{ config(materialized='table') }}

select
  pickup_date,
  pickup_zip,
  count(*)                as trips,
  round(avg(fare_usd), 2) as avg_fare,
  round(avg(fare_per_mile), 2) as avg_fare_per_mile
from {{ ref('fct_trips_inc') }}
group by 1,2