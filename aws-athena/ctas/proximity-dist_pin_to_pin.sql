-- View that finds the 3 nearest neighbor PINs for every PIN for every year
SELECT * FROM {{ ref('proximity.dist_pin_to_pin_01') }}
UNION
SELECT * FROM {{ ref('proximity.dist_pin_to_pin_02') }}
UNION
SELECT * FROM {{ ref('proximity.dist_pin_to_pin_03') }}
