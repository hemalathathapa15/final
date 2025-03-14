WITH cleaned_data AS (
    SELECT DISTINCT
        product_id,
        product_family,
        product_sub_family
    FROM {{ source('sourcedata', 'products') }}
    WHERE product_id IS NOT NULL
      AND product_family IS NOT NULL
      AND product_sub_family IS NOT NULL
)

SELECT *
FROM cleaned_data