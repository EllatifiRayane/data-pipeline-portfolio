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
    customer_segment,
    CAST(birth_date AS DATE) AS birth_date,
    CAST(registration_date AS DATE) AS registration_date
FROM source
