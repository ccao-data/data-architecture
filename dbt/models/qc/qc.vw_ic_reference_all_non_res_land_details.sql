SELECT
    pardat.jur,
    pardat.taxyr,
    pardat.parid,
    {{ insert_hyphens('pardat.class', 1) }} AS pardat_class,
    {{ insert_hyphens('pardat.tieback', 2, 4, 7, 10) }} AS tieback,
    legdat.user1,
    pardat.zoning,
    {{ insert_hyphens('pardat.nbhd', 2) }} AS nbhd,
    land.lline,
    land.acres,
    land.orate,
    land.sf,
    land.brate,
    land.price,
    land.ovrprice,
    {{ insert_hyphens('land.class', 1) }} AS land_class,
    land.influ,
    land.code,
    land.note1,
    land.allocpct,
    land.influ2,
    CONCAT(
        COALESCE(land.infl1, ''),
        CASE WHEN land.infl1 IS NOT NULL THEN ':' ELSE '' END,
        COALESCE(infl1_reascd.description, '')
    ) AS infl1,
    CONCAT(
        COALESCE(land.infl2, ''),
        CASE WHEN land.infl2 IS NOT NULL THEN ':' ELSE '' END,
        COALESCE(infl2_reascd.description, '')
    ) AS infl2,
    land.ltype,
    land.oincr,
    land.odecr,
    land.exmppct,
    land.osize
FROM {{ source('iasworld', 'land') }} AS land
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON land.parid = legdat.parid
    AND land.taxyr = legdat.taxyr
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'pardat') }} AS pardat
    ON land.parid = pardat.parid
    AND land.taxyr = pardat.taxyr
    AND pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
LEFT JOIN {{ ref('ccao.infl_reascd') }} AS infl1_reascd
    ON land.infl1 = infl1_reascd.reascd
LEFT JOIN {{ ref('ccao.infl_reascd') }} AS infl2_reascd
    ON land.infl2 = infl2_reascd.reascd
WHERE land.cur = 'Y'
    AND land.deactivat IS NULL
    AND pardat.class NOT BETWEEN '200' AND '299'
