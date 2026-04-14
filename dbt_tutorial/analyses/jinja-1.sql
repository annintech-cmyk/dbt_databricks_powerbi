{% set inc_flag = 1 %}
{% set last_load = 3 %}

{% set cols_list = ["sales_id", "gross_amount", "net_amount"] %}

SELECT
{% for col in cols_list %}
    {{ col }}{% if not loop.last %},{% endif %}
{% endfor %}

FROM {{ ref('bronze_sales') }}

{% if inc_flag == 1 %}
WHERE date_sk > {{ last_load }}
{% endif %}
