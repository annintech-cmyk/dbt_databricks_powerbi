SELECT DISTINCT country
FROM {{ source('source', 'dim_store') }}
ORDER BY country;

