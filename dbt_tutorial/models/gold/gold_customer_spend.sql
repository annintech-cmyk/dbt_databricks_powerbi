{{ config(materialized='table', schema='gold') }}

SELECT
    c.gender,
    c.loyalty_tier,
    SUM(s.net_amount) AS total_spend
FROM {{ ref('silver_sales') }} s
JOIN {{ ref('silver_customer') }} c
    ON s.customer_sk = c.customer_sk
GROUP BY c.gender, c.loyalty_tier
