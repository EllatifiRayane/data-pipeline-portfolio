{{
  config(
    materialized = "table"
  )
}}

WITH dedup_orders AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        has_promo,
        discount,
        normalized_status,
        channel,
        ROW_NUMBER() OVER (PARTITION BY order_id) AS row_nb
    FROM {{ ref('stg_orders') }}
    QUALIFY row_nb = 1
),

revenue_per_order AS (
    SELECT
        order_id,
        SUM(quantity * unit_price) AS revenue,
        COUNT(DISTINCT order_item_id) AS order_item_count,
        COUNT(DISTINCT product_id) AS number_of_product
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
)

SELECT
    ddp.order_id,
    ddp.customer_id,
    ddp.order_date,
    ddp.has_promo,
    ddp.discount,
    ddp.normalized_status,
    ddp.channel,
    rpo.order_item_count,
    rpo.number_of_product,
    ROUND(rpo.revenue, 2) AS total_amount
FROM dedup_orders AS ddp
LEFT JOIN revenue_per_order AS rpo ON ddp.order_id = rpo.order_id
