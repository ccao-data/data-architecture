SELECT comp.*
FROM {{ source('model', 'comp_version') }} AS comp
INNER JOIN (
    SELECT
        run_id,
        pin,
        card,
        MAX(version) AS version
    FROM {{ source('model', 'comp_version') }}
    GROUP BY run_id, pin, card
) AS latest
    ON comp.run_id = latest.run_id
    AND comp.pin = latest.pin
    AND comp.card = latest.card
    AND comp.version = latest.version
