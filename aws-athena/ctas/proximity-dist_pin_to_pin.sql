-- View that finds the 3 nearest neighbor PINs for every PIN for every year
SELECT * FROM proximity.dist_pin_to_pin_1km
UNION
-- Anti-join is necessary here because the 10km table sometimes contains
-- slightly different distances which don't get de-duped via UNION
SELECT km10.*
FROM proximity.dist_pin_to_pin_10km AS km10
LEFT JOIN proximity.dist_pin_to_pin_1km AS km1
    ON km10.pin10 = km1.pin10
    AND km10.year = km1.year
WHERE km1.pin10 IS NULL
