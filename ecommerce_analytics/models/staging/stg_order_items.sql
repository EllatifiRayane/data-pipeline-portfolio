{{ config(materialized='view') }}

WITH

source AS (

    SELECT * FROM {{ source('ecommerce_raw','order_items') }}

)

SELECT 
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    total_price
FROM source
