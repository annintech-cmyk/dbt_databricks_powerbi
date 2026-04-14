{{ config(materialized='table', schema='silver') }}

WITH base AS (

    SELECT
        CAST(date_sk AS INT) AS date_sk,
        CAST(date AS DATE) AS date,

        CAST(day AS INT) AS day,
        CAST(month AS INT) AS month,

        INITCAP(TRIM(month_name)) AS month_name,
        CAST(quarter AS INT) AS quarter,
        CAST(year AS INT) AS year,

        CAST(day_of_week AS INT) AS day_of_week,
        INITCAP(TRIM(day_name)) AS day_name,

        -- normalize boolean fields safely
        CAST(is_weekend AS BOOLEAN) AS is_weekend,
        CAST(is_month_end AS BOOLEAN) AS is_month_end,
        CAST(is_month_start AS BOOLEAN) AS is_month_start,
        CAST(is_quarter_end AS BOOLEAN) AS is_quarter_end,
        CAST(is_quarter_start AS BOOLEAN) AS is_quarter_start

    FROM {{ source('source', 'dim_date') }}

    WHERE date_sk IS NOT NULL
      AND date IS NOT NULL

)

SELECT *
FROM base
