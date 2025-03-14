{{config(
    materialized='table',
    schema='transform'
    )
}}
WITH customer_purchase_history AS (
    SELECT
        t.customer_id,
        t.product_id,
        t.payment_month,
        MIN(t.payment_month) OVER (PARTITION BY t.customer_id) AS first_purchase_month
    FROM {{ ref('stg_transactions') }} t
),
product_purchase_counts AS (
    SELECT
        customer_id,
        product_id,
        COUNT(DISTINCT payment_month) AS purchase_count,
        MAX(payment_month) AS last_purchase_month
    FROM customer_purchase_history
    GROUP BY customer_id, product_id
),
customer_cross_sell AS (
    SELECT
        customer_id,
        COUNT(DISTINCT product_id) AS total_cross_sells
    FROM customer_purchase_history
    WHERE payment_month > first_purchase_month 
    GROUP BY customer_id
),
customer_product_churn AS (
    SELECT
        customer_id,
        COUNT(DISTINCT product_id) AS churned_products
    FROM product_purchase_counts
    WHERE DATEADD(MONTH, 1, last_purchase_month) < (SELECT MAX(payment_month) FROM {{ ref('stg_transactions') }}) 
    GROUP BY customer_id
),
combined_metrics AS (
    SELECT
        c.customer_id,
        c.customer_name,
        COALESCE(cs.total_cross_sells,0) AS total_cross_sells,
        COALESCE(cp.churned_products,0) AS churned_products
    FROM {{ ref('stg_customers') }} c
    LEFT JOIN customer_cross_sell cs
      ON c.customer_id = cs.customer_id
    LEFT JOIN customer_product_churn cp
      ON c.customer_id = cp.customer_id
)
SELECT *
FROM combined_metrics
ORDER BY churned_products DESC, total_cross_sells DESC