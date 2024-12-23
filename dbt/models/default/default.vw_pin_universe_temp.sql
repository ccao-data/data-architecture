-- Small, managable version of pin universe for the purporse of updating the
-- Parcel Universe asset on open data.
SELECT * FROM {{ ref('default.vw_pin_universe') }} LIMIT 100000
