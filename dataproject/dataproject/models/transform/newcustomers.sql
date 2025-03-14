{{ config(
    materialized='table',
    schema='transform'
) }}

WITH customer_first_purchase AS (
    SELECT
        customer_id,
        MIN(payment_month) AS first_purchase_month,
        -- Assumed fiscal year starts in October
        CASE
            WHEN MONTH(MIN(payment_month)) >= 10 THEN YEAR(MIN(payment_month))
            ELSE YEAR(MIN(payment_month)) - 1
        END AS fiscal_year
    FROM {{ ref('stg_transactions') }}
    GROUP BY customer_id
),
new_logos_per_fiscal_year AS (
    SELECT
        fiscal_year,
        COUNT(DISTINCT customer_id) AS new_logos
    FROM customer_first_purchase
    GROUP BY fiscal_year
)
SELECT *
FROM new_logos_per_fiscal_year
ORDER BY fiscal_year