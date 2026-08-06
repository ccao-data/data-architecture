SELECT
    run_id,
    run_type,
    year
FROM {{ source('model', 'metadata') }}
UNION ALL
SELECT
    run_id,
    run_type,
    year
FROM {{ source('z_dev_model', 'metadata') }}
