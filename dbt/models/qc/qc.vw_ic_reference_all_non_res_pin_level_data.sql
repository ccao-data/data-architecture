SELECT
    pardat.jur,
    pardat.taxyr,
    pardat.parid,
    {{ insert_hyphens("pardat.class", 1) }} AS pardat_class,
    {{ insert_hyphens("pardat.tieback", 2, 4, 7, 10) }} AS tieback,
    aprval.comval,
    aprval.obyval,
    aprval.aprland,
    aprval.aprtot,
    aprval.aprbldg,
    aprval.dwelval,
    legdat.adrno,
    legdat.adrdir,
    legdat.adrstr,
    legdat.user1 AS township_code,
    legdat.cityname,
    legdat.taxdist,
    legdat.zip1,
    {{ insert_hyphens("pardat.nbhd", 2) }} AS nbhd,
    pardat.note1,
    pardat.user1,
    pardat.zoning,
    owndat.own1
FROM {{ source('iasworld', 'aprval') }} AS aprval
LEFT JOIN {{ source('iasworld', 'pardat') }} AS pardat
    ON aprval.parid = pardat.parid
    AND aprval.taxyr = pardat.taxyr
    AND pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON pardat.parid = legdat.parid
    AND pardat.taxyr = legdat.taxyr
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'owndat') }} AS owndat
    ON pardat.parid = owndat.parid
    AND pardat.taxyr = owndat.taxyr
    AND owndat.cur = 'Y'
    AND owndat.deactivat IS NULL
WHERE aprval.cur = 'Y'
    AND aprval.deactivat IS NULL
    AND pardat.class NOT BETWEEN '200' AND '299'
