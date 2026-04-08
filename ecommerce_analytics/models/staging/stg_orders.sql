{{
  config(
    materialized = "view"
  )
}}

WITH

source AS (

    SELECT * FROM {{ source('ecommerce_raw','orders') }}

)

SELECT
    order_id,
    customer_id,
    COALESCE(
        DATE(safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*S', order_date)),
        safe.parse_date('%Y-%m-%d', order_date),
        safe.parse_date('%d/%m/%Y', order_date),
        safe.parse_date('%m-%d-%Y', order_date)
    ) AS order_date,
    has_promo,
    promo_code,
    discount,
    source.status,
    {{ normalize_status('status') }} AS normalized_status,
    channel
FROM source
