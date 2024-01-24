-- View that finds the 3 nearest neighbor PINs for every PIN for every year
SELECT * FROM {{ ref('proximity.dist_pin_to_pin_1km') }}
UNION
-- Anti-join is necessary here because the 10km table sometimes contains
-- slightly different distances which don't get de-duped via UNION
SELECT *
FROM {{ ref('proximity.dist_pin_to_pin_10km') }} AS km10
LEFT JOIN {{ ref('proximity.dist_pin_to_pin_1km') }} AS km1
USING (pin10, year)
WHERE km1.pin10 IS NULL
