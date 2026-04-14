SELECT id, name, created_at FROM
 {{ source('default', 'raw_users') }}
