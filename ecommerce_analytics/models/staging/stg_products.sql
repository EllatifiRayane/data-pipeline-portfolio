{{ config(materialized='view') }}

WITH

source AS (

    SELECT * FROM {{ source('ecommerce_raw','products') }}

),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY last_modified_t DESC
        ) AS row_num
    FROM source
),

renamed AS (

    SELECT
        id AS product_id,
        product_name,
        brands,
        categories,
        quantity,
        nutriscore_grade,
        ecoscore_grade,
        countries_tags AS countries_sold,
        stores,
        TIMESTAMP_SECONDS(created_t) AS creation_at,
        TIMESTAMP_SECONDS(last_modified_t) AS last_modified_at,
        TIMESTAMP(ingested_at) AS ingested_at

    FROM deduplicated
    WHERE id IS NOT NULL AND row_num = 1

)

SELECT * FROM renamed
