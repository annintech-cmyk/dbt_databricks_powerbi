{{ config(materialized='table', schema='silver') }}

WITH base AS (

    SELECT
        sales_id,
        date_sk,
        store_sk,
        product_sk,
        customer_sk,

        CAST(quantity AS INT) AS quantity,
        CAST(unit_price AS DECIMAL(10,2)) AS unit_price,

        discount_amount,
        net_amount,
        payment_method

    FROM {{ source('source', 'fact_sales') }}

    WHERE quantity > 0
      AND unit_price IS NOT NULL

)

SELECT
    sales_id,
    date_sk,
    store_sk,
    product_sk,
    customer_sk,

    quantity,
    unit_price,

    CAST(ROUND(quantity * unit_price, 2) AS DECIMAL(10,2)) AS gross_amount_calculated,

    discount_amount,

    CAST(ROUND(net_amount, 2) AS DECIMAL(10,2)) AS net_amount,

    UPPER(payment_method) AS payment_method

FROM base
