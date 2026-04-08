{{
  config(
    materialized = "table"
  )
}}


SELECT
    product_id,
    product_name,
    brands,
    -- Extract the first product category
    nutriscore_grade,
    ecoscore_grade,
    SPLIT(categories, ",")[SAFE_OFFSET(0)] AS primary_category,
    COALESCE(
        REGEXP_REPLACE(countries_sold.list[SAFE_OFFSET(0)].element, r"en:", ""),
        "unknown"
    ) AS primary_country, -- Extract and clean the first country (removes 'en:' prefix)
    ARRAY_LENGTH(countries_sold.list) AS nb_countries

FROM {{ ref('stg_products') }}
