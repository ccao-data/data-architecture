-- View to collect currently exempt properties
SELECT
    par.parid AS pin,
    par.taxyr AS year,
    twn.township_name,
    leg.user1 AS township_code,
    own.own1 AS owner_name,
    own.ownnum AS owner_num,
    REGEXP_REPLACE(par.class, '[^[:alnum:]]', '') AS class,
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
FROM {{ source('iasworld', 'pardat') }} AS par
LEFT JOIN {{ source('iasworld', 'owndat') }} AS own
    ON par.parid = own.parid
    AND par.taxyr = own.taxyr
    AND own.cur = 'Y'
    AND own.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'legdat') }} AS leg
    ON par.parid = leg.parid
    AND par.taxyr = leg.taxyr
    AND leg.cur = 'Y'
    AND leg.deactivat IS NULL
LEFT JOIN {{ ref('default.vw_pin_address') }} AS vpa
    ON par.parid = vpa.pin
    AND par.taxyr = vpa.year
LEFT JOIN {{ source('spatial', 'parcel') }} AS parcel
    ON vpa.pin10 = parcel.pin10
    AND vpa.year = parcel.year
LEFT JOIN {{ source('spatial', 'township') }} AS twn
    ON leg.user1 = CAST(twn.township_code AS VARCHAR)
WHERE
    -- This condition is how we determine exempt status, not through class
    own.ownnum IS NOT NULL
    AND par.cur = 'Y'
    AND par.deactivat IS NULL
    AND par.taxyr >= '2022'
    -- Remove any parcels with non-numeric characters
    -- or that are not 14 characters long
    AND REGEXP_COUNT(par.parid, '[a-zA-Z]') = 0
    AND LENGTH(par.parid) = 14
    AND par.class NOT IN ('999')
