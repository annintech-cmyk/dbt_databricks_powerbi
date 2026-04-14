SELECT 
*
FROM 
     {{  ref('bronze_sales') }} 
WHERE
     gross_amount < 100 AND net_amount < 100