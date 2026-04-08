{{
    config(
        materialized='table',
        partition_by={
            "field": "cohort_month",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by=["period_number"]
    )
}}

WITH first_order AS (

    SELECT
        customer_id,
        DATE_TRUNC(MIN(order_date), MONTH) AS cohort_month
    FROM {{ ref('int_orders') }}
    WHERE
        order_date IS NOT NULL
        AND normalized_status = 'DELIVERED'
    GROUP BY customer_id
),

cohort AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_id) AS cohort_nb_customers
    FROM first_order
    GROUP BY cohort_month
),

monthly_revenue AS (
    SELECT
        customer_id,
        DATE_TRUNC(order_date, MONTH) AS order_month,
        SUM(total_amount) AS revenue,
        COUNT(order_id) AS nb_orders
    FROM {{ ref('int_orders') }}
    WHERE
        order_date IS NOT NULL
        AND normalized_status = 'DELIVERED'
    GROUP BY customer_id, DATE_TRUNC(order_date, MONTH)
),

cohort_activity AS (
    SELECT
        fo.cohort_month,
        fo.customer_id,
        mr.order_month,
        mr.revenue,
        mr.nb_orders,
        DATE_DIFF(mr.order_month, fo.cohort_month, MONTH) AS period_number
    FROM first_order AS fo
    INNER JOIN monthly_revenue AS mr ON fo.customer_id = mr.customer_id

),

monthly_cohort_activity AS (

    SELECT
        cohort_month,
        order_month,
        period_number,
        SUM(revenue) AS total_revenue,
        SUM(nb_orders) AS total_orders,
        COUNT(DISTINCT customer_id) AS nb_customers
    FROM cohort_activity
    GROUP BY cohort_month, order_month, period_number
),

final AS (
    SELECT
        mca.cohort_month,
        mca.order_month,
        mca.period_number,
        mca.total_revenue,
        mca.total_orders,
        mca.nb_customers,
        co.cohort_nb_customers,
        ROUND(SAFE_DIVIDE(mca.nb_customers, co.cohort_nb_customers), 2)
            AS retention_rate,
        ROUND(
            SAFE_DIVIDE(
                co.cohort_nb_customers - mca.nb_customers,
                co.cohort_nb_customers
            ),
            2
        ) AS churn_rate,
        SUM(mca.total_revenue)
            OVER (PARTITION BY co.cohort_month ORDER BY mca.period_number ASC)
            AS cumulative_ltv
    FROM monthly_cohort_activity AS mca
    INNER JOIN
        cohort AS co
        ON mca.cohort_month = co.cohort_month
)

SELECT * FROM final
