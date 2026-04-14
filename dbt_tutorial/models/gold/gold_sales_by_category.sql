SELECT
    p.category,
    SUM(s.gross_amount_calculated) AS total_sales,
    SUM(s.net_amount) AS net_sales,
    COUNT(*) AS total_orders
FROM {{ ref('silver_sales') }} s
JOIN {{ ref('silver_product') }} p
    ON s.product_sk = p.product_sk
GROUP BY p.category

