{{ config(
    materialized='table',
    partition_by={
        "field": "order_month",
        "data_type": "date"
    },
    cluster_by=["product_id"]
) }}

WITH product_monthly AS (

    SELECT 
        soi.product_id,
        DATE_TRUNC(io.order_date, MONTH) AS order_month,

        -- Core metrics
        SUM(soi.unit_price * soi.quantity) AS monthly_revenue,
        SUM(soi.quantity)                  AS units_sold,
        COUNT(DISTINCT soi.order_id)       AS nb_orders,

        SAFE_DIVIDE(
            SUM(soi.unit_price * soi.quantity),
            SUM(soi.quantity)
        ) AS avg_unit_price

    FROM {{ ref('stg_order_items') }} soi
    JOIN {{ ref('int_orders') }} io
        USING (order_id)

    WHERE io.normalized_status = 'DELIVERED'
      AND io.order_date IS NOT NULL

    GROUP BY 1, 2

),

product_enriched AS (

    SELECT
        pm.*,

        -- Previous month revenue
        LAG(monthly_revenue) OVER (
            PARTITION BY product_id
            ORDER BY order_month
        ) AS prev_revenue,

        -- Annual revenue (for seasonality)
        SUM(monthly_revenue) OVER (
            PARTITION BY product_id, EXTRACT(YEAR FROM order_month)
        ) AS annual_revenue

    FROM product_monthly pm

),

final AS (

    SELECT
        product_id,
        order_month,

        -- Core metrics
        monthly_revenue,
        units_sold,
        nb_orders,
        avg_unit_price,

        -- Growth
        SAFE_DIVIDE(
            monthly_revenue - prev_revenue,
            prev_revenue
        ) AS mom_growth_rate,

        -- Seasonality
        SAFE_DIVIDE(
            monthly_revenue,
            annual_revenue / 12
        ) AS seasonal_index,

        -- Flags (dashboard ready 💥)
        CASE
            WHEN SAFE_DIVIDE(
                monthly_revenue - prev_revenue,
                prev_revenue
            ) > 0.35
            AND monthly_revenue > 1000
            THEN TRUE
            ELSE FALSE
        END AS is_high_growth

    FROM product_enriched

)

SELECT *
FROM final