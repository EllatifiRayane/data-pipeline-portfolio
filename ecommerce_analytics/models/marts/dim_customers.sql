{{
  config(
    materialized = "table"
  )
}}

WITH enriched AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        gender,
        city,
        country,
        email,
        customer_segment,
        birth_date,
        registration_date,
        DATE_DIFF(CURRENT_DATE(), birth_date, YEAR) AS age,
        DATE_DIFF(CURRENT_DATE(), registration_date, DAY) AS customer_tenure_days
    FROM {{ ref('stg_customers') }}
)

SELECT
    *,
    CASE
        WHEN age BETWEEN 18 AND 25 THEN '18-25'
        WHEN age BETWEEN 26 AND 35 THEN '26-35'
        WHEN age BETWEEN 36 AND 50 THEN '36-50'
        WHEN age BETWEEN 51 AND 75 THEN '51-75'
        ELSE 'other'
    END AS age_group
FROM enriched
