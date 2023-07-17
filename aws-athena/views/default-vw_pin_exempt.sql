-- View to collect currently exempt properties
CREATE OR REPLACE VIEW default.vw_pin_exempt AS

SELECT
    par.parid AS pin,
    par.taxyr AS year,
    twn.township_name,
    leg.user1 AS township_code,
    own.own1 AS owner_name,
    own.ownnum AS owner_num,
    par.class,
    vpa.prop_address_full AS property_address,
    vpa.prop_address_city_name AS property_city,
    --- Forward fill lat and long
    COALESCE(
        parcel.lon,
        LAST_VALUE(parcel.lon)
            IGNORE NULLS OVER (PARTITION BY par.parid ORDER BY par.taxyr)
    ) AS lon,
    COALESCE(
        parcel.lat,
        LAST_VALUE(parcel.lat)
            IGNORE NULLS OVER (PARTITION BY par.parid ORDER BY par.taxyr)
    ) AS lat
FROM iasworld.pardat AS par
LEFT JOIN
    iasworld.owndat AS own
    ON par.parid = own.parid AND par.taxyr = own.taxyr
LEFT JOIN
    iasworld.legdat AS leg
    ON par.parid = leg.parid AND par.taxyr = leg.taxyr
LEFT JOIN default.vw_pin_address AS vpa
    ON par.parid = vpa.pin AND par.taxyr = vpa.year
LEFT JOIN spatial.parcel ON vpa.pin10 = parcel.pin10 AND vpa.year = parcel.year
LEFT JOIN spatial.township AS twn
    ON leg.user1 = CAST(twn.township_code AS VARCHAR)
WHERE
    --- This condition is how we determine exempt status, not through class
    own.ownnum IS NOT NULL
    AND leg.cur = 'Y'
    AND par.taxyr >= '2022'
