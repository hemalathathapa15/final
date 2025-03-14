WITH cleaned_data AS (
    SELECT DISTINCT
        company,
        customer_id,
        customername AS customer_name
    FROM {{ source('sourcedata','customers') }}
    WHERE company IS NOT NULL
      AND customer_id IS NOT NULL
      AND customername IS NOT NULL
)

SELECT *
FROM cleaned_data 