{{ config(materialized='table', schema='silver') }}

WITH base AS (

    SELECT
        store_sk,
        TRIM(store_code) AS store_code,
        TRIM(store_name) AS store_name,
        TRIM(city) AS city,
        UPPER(TRIM(state_province)) AS state_province,
        TRIM(region) AS region,

        -- standardize country
        CASE 
            WHEN country = 'USA' THEN 'United States'
            ELSE country
        END AS country,

        CAST(open_date AS DATE) AS open_date,
        CAST(sq_ft AS INT) AS sq_ft

    FROM {{ source('source', 'dim_store') }}

    WHERE store_sk IS NOT NULL
      AND store_name IS NOT NULL
      AND country IS NOT NULL

)

SELECT
    *,
    
    -- optional derived metric
    DATEDIFF(CURRENT_DATE, open_date) AS store_age_days

FROM base
