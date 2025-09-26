{{ config(
  materialized='incremental',
  unique_key=['pickup_ts','dropoff_ts','pickup_zip','dropoff_zip','trip_miles','fare_usd'],
  partition_by=['pickup_date']         
) }}

select
  pickup_date,
  pickup_ts,
  dropoff_ts,
  pickup_zip,
  dropoff_zip,
  trip_miles,
  fare_usd,
  fare_per_mile
from {{ ref('vw_trips_clean') }}
{% if is_incremental() %}
  where pickup_date > (select coalesce(max(pickup_date), date('1900-01-01')) from {{ this }})
{% endif %}