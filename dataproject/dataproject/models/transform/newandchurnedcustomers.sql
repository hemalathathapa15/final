{{config(
    materialized='table',
    schema='transform'
    )
}}
WITH customers_data AS (
    SELECT
        payment_month,
        customer_id,
        MIN(payment_month) OVER (PARTITION BY customer_id) AS first_purchase_date,
        MAX(payment_month) OVER (PARTITION BY customer_id) AS last_purchase_date
    FROM {{ ref('stg_transactions') }}  
    JOIN {{ ref('stg_customers') }} USING (customer_id)
),
new_and_churned_customers AS (
    SELECT
        payment_month,
        COUNT(DISTINCT CASE WHEN first_purchase_date = payment_month THEN customer_id END) AS new_customers,
        COUNT(DISTINCT CASE WHEN last_purchase_date = payment_month AND first_purchase_date = last_purchase_date THEN customer_id END) AS one_time_customers,
        COUNT(DISTINCT CASE WHEN last_purchase_date = payment_month AND first_purchase_date != last_purchase_date THEN customer_id END) AS churned_customers
    FROM customers_data
    GROUP BY payment_month
    ORDER BY payment_month
)
SELECT * FROM new_and_churned_customers

