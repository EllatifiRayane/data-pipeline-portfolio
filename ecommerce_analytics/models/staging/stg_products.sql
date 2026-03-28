with

source as (

    select * from {{ source('ecommerce_raw','products') }}

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

    FROM source

)

SELECT * FROM renamed
