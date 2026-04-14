SELECT
    p.category,
    SUM(r.refund_amount) AS total_refund
FROM {{ ref('silver_returns') }} r
JOIN {{ ref('silver_product') }} p
    ON r.product_sk = p.product_sk
GROUP BY p.category
