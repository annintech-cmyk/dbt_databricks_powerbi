{{ config(materialized='table', schema='silver') }}

WITH base AS (

    SELECT
        sales_id,
        date_sk,
        store_sk,
        product_sk,
        returned_qty,
        return_reason,

        -- CLEAN refund_amount (remove invalid characters)
        TRY_CAST(REGEXP_REPLACE(refund_amount, '[^0-9.]', '') AS DECIMAL(10,2)) 
            AS refund_amount

    FROM {{ source('source', 'fact_returns') }}

    WHERE sales_id IS NOT NULL
      AND returned_qty >= 0

)

SELECT *
FROM base
