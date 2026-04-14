SELECT
    st.store_name,
    st.country,
    SUM(s.net_amount) AS revenue
FROM {{ ref('silver_sales') }} s
JOIN {{ ref('silver_store') }} st
    ON s.store_sk = st.store_sk
GROUP BY st.store_name, st.country
