{{
  config(
    materialized = "table"
  )
}}

SELECT
    order_date,
    EXTRACT(YEAR FROM order_date) AS year,
    EXTRACT(MONTH FROM order_date) AS month,
    CONCAT('Q', CAST(EXTRACT(QUARTER FROM order_date) AS STRING)) AS quarter,
    channel,
    has_promo,
    normalized_status,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(total_amount),2) AS total_revenue,
    ROUND(AVG(total_amount),2) AS avg_basket,
    COUNT(DISTINCT customer_id) AS unique_customers
    
FROM {{ ref('int_orders') }}
GROUP BY 1, 2, 3, 4, 5, 6, 7


