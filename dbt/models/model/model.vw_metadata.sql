SELECT * FROM {{ source('model', 'metadata') }}
UNION ALL
SELECT * FROM {{ source('z_dev_model', 'metadata') }}
