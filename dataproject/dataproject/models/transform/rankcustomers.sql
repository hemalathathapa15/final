{{config(
    materialized='table',
    schema='transform'
    )
}}
WITH customer_revenue AS (
    SELECT
        t.customer_id,
        c.customer_name,
        SUM(t.revenue) AS total_revenue
    FROM {{ ref('stg_transactions') }} t
    JOIN {{ ref('stg_customers') }} c
      ON t.customer_id = c.customer_id
    GROUP BY t.customer_id, c.customer_name
),
ranked_customers AS (
    SELECT
        customer_id,
        customer_name,
        total_revenue,
        RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
    FROM customer_revenue
)
SELECT *
FROM ranked_customers
ORDER BY revenue_rank