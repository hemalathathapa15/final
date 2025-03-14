{{config(
    materialized='table',
    schema='transform'
    )
}}
WITH product_revenue AS (
    SELECT
        t.product_id,
        SUM(t.revenue) AS total_revenue
    FROM {{ ref('stg_transactions') }} t
    JOIN {{ ref('stg_products') }} p
      ON t.product_id = p.product_id
    GROUP BY t.product_id
),
ranked_products AS (
    SELECT
        product_id,
        total_revenue,
        RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
    FROM product_revenue
)
SELECT *
FROM ranked_products
ORDER BY revenue_rank
