{{ config(materialized='view') }}

with

source as (

    select * from {{ source('ecommerce_raw','orders') }}

)

SELECT 
    order_id,
    customer_id,
    coalesce(
    date(safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*S', order_date)),
    safe.parse_date('%Y-%m-%d', order_date),
    safe.parse_date('%d/%m/%Y', order_date),
    safe.parse_date('%m-%d-%Y', order_date)
            ) as order_date,
    has_promo,
    promo_code,
    discount,
    status,
   {{ normalize_status('status') }} AS normalized_status,
    channel
FROM source
