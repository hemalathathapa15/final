WITH cleaned_data AS (
    SELECT DISTINCT
        customer_id,
        product_id,
        payment_month,
        revenue_type,
        revenue,
        quantity,
        dimension_1,
        dimension_2,
        dimension_3,
        dimension_4,
        dimension_5,
        dimension_6,
        dimension_7,
        dimension_8,
        dimension_9,
        dimension_10,
        companies
    FROM {{ source('sourcedata', 'transactions') }}
    WHERE customer_id IS NOT NULL
      AND product_id IS NOT NULL
      AND payment_month IS NOT NULL
      AND revenue_type IS NOT NULL
      AND revenue IS NOT NULL
      AND quantity IS NOT NULL
      AND dimension_1 IS NOT NULL
      AND dimension_2 IS NOT NULL
      AND dimension_3 IS NOT NULL
      AND dimension_4 IS NOT NULL
      AND dimension_5 IS NOT NULL
      AND dimension_6 IS NOT NULL
      AND dimension_7 IS NOT NULL
      AND dimension_8 IS NOT NULL
      AND dimension_9 IS NOT NULL
      AND dimension_10 IS NOT NULL
      AND companies IS NOT NULL
)

SELECT *
FROM cleaned_data