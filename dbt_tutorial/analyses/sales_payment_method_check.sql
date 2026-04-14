SELECT DISTINCT payment_method
FROM {{ source('source', 'fact_sales') }}
ORDER BY payment_method;
