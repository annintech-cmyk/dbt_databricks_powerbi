WITH sales AS (
    SELECT 
        sales_id, 
        product_sk,
        customer_sk,
        {{ multiply('unit_price', 'quantity') }} AS calculated_gross_amount,
        payment_method
    FROM {{ ref('bronze_sales') }}
), 

products AS (
    SELECT 
        product_sk, 
        category
    FROM {{ ref('bronze_product') }}
),

customers AS (
    SELECT 
        customer_sk, 
        gender
    FROM {{ ref('bronze_customer') }}
),

joined_query AS (   -- ✅ FIXED (comma added above)
    SELECT 
        s.sales_id, 
        s.product_sk,
        s.customer_sk,
        s.calculated_gross_amount,
        s.payment_method, 
        p.category,
        c.gender
    FROM sales s
    JOIN products p 
        ON s.product_sk = p.product_sk   
    JOIN customers c 
        ON s.customer_sk = c.customer_sk
)

SELECT 
    category,
    gender,
    SUM(calculated_gross_amount) AS total_gross_amount
FROM joined_query
GROUP BY category, gender
ORDER BY total_gross_amount DESC
