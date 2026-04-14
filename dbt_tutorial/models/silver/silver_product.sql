{{ config(materialized='table', schema='silver') }}

WITH base AS (

    SELECT
        product_sk,

        TRIM(product_code) AS product_code,
        TRIM(product_name) AS product_name,

        INITCAP(TRIM(department)) AS department,
        INITCAP(TRIM(category)) AS category,

        supplier_sk,

        -- ensure correct numeric precision
        CAST(list_price AS DECIMAL(10,2)) AS list_price,

        -- standardize unit of measure
        LOWER(TRIM(uom)) AS uom

    FROM {{ source('source', 'dim_product') }}

    WHERE product_sk IS NOT NULL
      AND product_name IS NOT NULL
      AND list_price > 0

)

SELECT *
FROM base
