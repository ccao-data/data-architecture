-- View to collect currently exempt properties
CREATE OR REPLACE VIEW default.vw_pin_exempt AS

SELECT
    p.parid AS pin,
    p.taxyr AS year,
    twn.township_name,
    l.user1 AS township_code,
    o.own1 AS owner_name,
    o.ownnum AS owner_num,
    p.class,
    vpa.prop_address_full AS property_address,
    vpa.prop_address_city_name AS property_city,
    --- Forward fill lat and long
    COALESCE(
        parcel.lon,
        LAST_VALUE(parcel.lon)
            IGNORE NULLS OVER (PARTITION BY p.parid ORDER BY p.taxyr)
    ) AS lon,
    COALESCE(
        parcel.lat,
        LAST_VALUE(parcel.lat)
            IGNORE NULLS OVER (PARTITION BY p.parid ORDER BY p.taxyr)
    ) AS lat
FROM iasworld.pardat AS p
LEFT JOIN iasworld.owndat AS o ON p.parid = o.parid AND p.taxyr = o.taxyr
LEFT JOIN iasworld.legdat AS l ON p.parid = l.parid AND p.taxyr = l.taxyr
LEFT JOIN default.vw_pin_address AS vpa
    ON p.parid = vpa.pin AND p.taxyr = vpa.year
LEFT JOIN spatial.parcel ON vpa.pin10 = parcel.pin10 AND vpa.year = parcel.year
LEFT JOIN spatial.township AS twn
    ON l.user1 = CAST(twn.township_code AS VARCHAR)
WHERE
    --- This condition is how we determine exempt status, not through class
    o.ownnum IS NOT NULL
    AND l.cur = 'Y'