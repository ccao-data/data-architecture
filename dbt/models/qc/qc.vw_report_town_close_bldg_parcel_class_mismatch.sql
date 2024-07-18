SELECT
    legdat.parid,
    legdat.taxyr,
    -- legdat and pardat are unique by parid and taxyr, so we can select the
    -- max and have confidence there is only one value
    MAX(legdat.user1) AS township_code,
    MAX(pardat.class) AS parcel_class,
    -- comdat, dweldat, and oby can have multiple cards/lines for
    -- one parid/taxyr combo, so we need to aggregate them into lists
    ARRAY_AGG(DISTINCT comdat.class) AS comdat_classes,
    ARRAY_AGG(DISTINCT dweldat.class) AS dweldat_classes,
    ARRAY_AGG(DISTINCT oby.class) AS oby_classes
FROM {{ source('iasworld', 'pardat') }} AS pardat
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON pardat.parid = legdat.parid
    AND pardat.taxyr = legdat.taxyr
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'comdat') }} AS comdat
    ON pardat.parid = comdat.parid
    AND pardat.taxyr = comdat.taxyr
    AND comdat.cur = 'Y'
    AND comdat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'dweldat') }} AS dweldat
    ON pardat.parid = dweldat.parid
    AND pardat.taxyr = dweldat.taxyr
    AND dweldat.cur = 'Y'
    AND dweldat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'oby') }} AS oby
    ON pardat.parid = oby.parid
    AND pardat.taxyr = oby.taxyr
    AND oby.cur = 'Y'
    AND oby.deactivat IS NULL
WHERE pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
    -- Trick to filter for rows where these four columns are not equal
    AND LEAST(pardat.class, comdat.class, dweldat.class, oby.class)
    != GREATEST(pardat.class, comdat.class, dweldat.class, oby.class)
GROUP BY legdat.parid, legdat.taxyr
