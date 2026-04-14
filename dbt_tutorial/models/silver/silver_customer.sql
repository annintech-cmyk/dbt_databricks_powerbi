{{ config(materialized='table', schema='silver') }}

WITH base AS (

    SELECT
        customer_sk,
        TRIM(customer_code) AS customer_code,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        TRIM(gender) AS gender,

        -- EMAIL CLEANING
        CASE 
            WHEN email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
            THEN LOWER(TRIM(email))
            ELSE 'support@gmail.com'
        END AS email,

        phone,

        -- LOYALTY CLEANING
        CASE 
            WHEN UPPER(TRIM(loyalty_tier)) IN ('SILVER', 'GOLD', 'PLATINUM')
            THEN INITCAP(TRIM(loyalty_tier))
            ELSE 'None'
        END AS loyalty_tier,

        CAST(signup_date AS DATE) AS signup_date

    FROM {{ source('source', 'dim_customer') }}

    WHERE customer_sk IS NOT NULL

)

SELECT *
FROM base
