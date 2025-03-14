{{config(
    materialized='table',
    schema='transform'
)}}
WITH filtered_transactions AS (
    SELECT
        t.customer_id,
        t.product_id,
        p.product_sub_family,
        p.product_family,
        t.payment_month,
        t.revenue,
        LAG(t.revenue) OVER (PARTITION BY t.customer_id, p.product_sub_family ORDER BY t.payment_month) AS previous_revenue
    FROM {{ ref('stg_transactions') }} t
    JOIN {{ ref('stg_products') }} p
      ON t.product_id = p.product_id
    WHERE t.revenue_type = 1 -- Only include rows where revenue_type is true 
),
contraction_revenue AS (
    SELECT
        DATE_TRUNC('month', t.payment_month) AS period_month,
        SUM(CASE 
            WHEN t.revenue < t.previous_revenue THEN t.previous_revenue - t.revenue 
            ELSE 0 
        END) AS revenue_lost
    FROM filtered_transactions t
    GROUP BY DATE_TRUNC('month', t.payment_month)
),
contraction AS (
    SELECT
        period_month,
        SUM(revenue_lost) AS total_revenue_lost
    FROM contraction_revenue
    GROUP BY period_month
    ORDER BY period_month
)
SELECT *
FROM contraction