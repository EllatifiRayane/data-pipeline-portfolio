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
  DATE_TRUNC(MIN(order_date), MONTH) AS cohort_month,
  customer_id
FROM {{ ref('int_orders') }}
WHERE order_date IS NOT NULL
AND normalized_status = 'DELIVERED'
GROUP BY customer_id
ORDER BY MIN(order_date)
),
cohort AS (
  SELECT count(distinct(customer_id)) as cohort_nb_customers,cohort_month
  FROM first_order
  GROUP BY cohort_month
),
monthly_revenue AS (
SELECT customer_id,
DATE_TRUNC(order_date, MONTH) AS order_month,
sum(total_amount) AS revenue,
count(order_id) AS nb_orders
FROM {{ ref('int_orders') }}
WHERE order_date IS NOT NULL
and normalized_status ='DELIVERED'
GROUP BY customer_id,DATE_TRUNC(order_date, MONTH)
),
cohort_activity AS (
SELECT 
fo.cohort_month,
fo.customer_id,
mr.order_month,
mr.revenue,
mr.nb_orders,
DATE_DIFF(mr.order_month,fo.cohort_month,MONTH) AS period_number
FROM first_order fo 
JOIN monthly_revenue mr on mr.customer_id = fo.customer_id

),
monthly_cohort_activity AS (

  SELECT cohort_month,
  order_month,
  period_number,
  SUM(revenue) AS total_revenue,
  SUM(nb_orders) AS total_orders,
  COUNT(DISTINCT(customer_id)) as nb_customers
  FROM cohort_activity
  GROUP BY cohort_month,order_month,period_number
),

final AS (

  SELECT 
  cohort_month,
  order_month,
  period_number,
  total_revenue,
  total_orders,
  nb_customers,
  cohort_nb_customers,
  ROUND(SAFE_DIVIDE(nb_customers,cohort_nb_customers),2) as retention_rate,
  ROUND(SAFE_DIVIDE(cohort_nb_customers-nb_customers,cohort_nb_customers),2) as churn_rate
  FROM monthly_cohort_activity 
  INNER JOIN cohort USING(cohort_month)

)
SELECT * FROM final
