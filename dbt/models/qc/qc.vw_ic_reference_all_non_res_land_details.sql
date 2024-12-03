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
    land.class AS land_class,
    land.influ,
    land.code,
    land.note1,
    land.allocpct,
    land.influ2,
    land.infl1,
    land.infl2,
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
    and legdat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'pardat') }} AS pardat
    ON land.parid = pardat.parid
    AND land.taxyr = pardat.taxyr
    AND pardat.cur = 'Y'
    and pardat.deactivat IS NULL
WHERE land.cur = 'Y'
    AND land.deactivat IS NULL
    AND pardat.class NOT BETWEEN '200' AND '299'
