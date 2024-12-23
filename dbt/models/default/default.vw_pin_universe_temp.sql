-- Small, managable version of pin universe for the purporse of updating the
-- Parcel Universe asset on open data.
SELECT * FROM {{ ref('location.vw_pin10_location') }} LIMIT 100000
