{{ config(materialized='view') }}

WITH

source AS (

    SELECT * FROM {{ source('ecommerce_raw','customers') }}

)

SELECT 
    customer_id,
    first_name,
    last_name,
    gender,
    city,
    country,
    email,
    CAST(birth_date AS DATE) AS birth_date,
    CAST(registration_date AS DATE) AS registration_date,
    customer_segment
FROM source
