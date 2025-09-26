
{{ config(materialized='view') }}

with base as (
  select
    tpep_pickup_datetime  as pickup_ts,
    tpep_dropoff_datetime as dropoff_ts,
    cast(trip_distance as double) as trip_miles,
    cast(fare_amount as double) as fare_usd,
    cast(pickup_zip as int) as pickup_zip,
    cast(dropoff_zip as int) as dropoff_zip
  from {{ source('raw_taxi','sample_taxis_nyc') }}
  where tpep_pickup_datetime is not null
    and tpep_dropoff_datetime is not null
    and trip_distance > 0
    and fare_amount >= 0
)
select
  pickup_ts,
  dropoff_ts,
  date(pickup_ts) as pickup_date,
  pickup_zip,
  dropoff_zip,
  trip_miles,
  fare_usd,
  case when trip_miles > 0 then fare_usd / trip_miles end as fare_per_mile
from base