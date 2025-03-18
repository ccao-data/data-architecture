-- Combine DWELDAT, COMDAT, and OBY and pull calculated fields from each
WITH combined_calc AS (
    SELECT
        'DWELDAT' AS table_name,
        dweldat.parid,
        dweldat.card,
        -- DWELDAT doesn't have lline but we need it for the union with OBY
        NULL AS lline,
        dweldat.taxyr,
        dweldat.external_rcnld,
        dweldat.external_occpct,
        dweldat.external_propct,
        dweldat.external_calc_rcnld,
        dweldat.ovrrcnld,
        dweldat.external_calc_rcnld - dweldat.ovrrcnld AS value_difference,
        dweldat.mktrsn AS card_code,
        dweldat.user16 AS alt_cdu,
        dweldat.who,
        DATE_FORMAT(
            DATE_PARSE(dweldat.wen, '%Y-%m-%d %H:%i:%S.%f'), '%c/%e/%Y %H:%i'
        ) AS wen
    FROM {{ source('iasworld', 'dweldat') }} AS dweldat
    WHERE dweldat.cur = 'Y'
        AND dweldat.deactivat IS NULL
    UNION ALL
    SELECT
        'COMDAT' AS table_name,
        comdat.parid,
        comdat.card,
        NULL AS lline,
        comdat.taxyr,
        comdat.external_rcnld,
        comdat.external_occpct,
        comdat.external_propct,
        comdat.external_calc_rcnld,
        comdat.ovrrcnld,
        comdat.external_calc_rcnld - comdat.ovrrcnld AS value_difference,
        comdat.chgrsn AS card_code,
        comdat.user16 AS alt_cdu,
        comdat.who,
        DATE_FORMAT(
            DATE_PARSE(comdat.wen, '%Y-%m-%d %H:%i:%S.%f'), '%c/%e/%Y %H:%i'
        ) AS wen
    FROM {{ source('iasworld', 'comdat') }} AS comdat
    WHERE comdat.cur = 'Y'
        AND comdat.deactivat IS NULL
    UNION ALL
    SELECT
        'OBY' AS table_name,
        oby.parid,
        oby.card,
        oby.lline,
        oby.taxyr,
        oby.external_rcnld,
        oby.external_occpct,
        oby.external_propct,
        oby.external_calc_rcnld,
        oby.ovrrcnld,
        oby.external_calc_rcnld - oby.ovrrcnld AS value_difference,
        oby.chgrsn AS card_code,
        oby.user16 AS alt_cdu,
        oby.who,
        DATE_FORMAT(
            DATE_PARSE(oby.wen, '%Y-%m-%d %H:%i:%S.%f'), '%c/%e/%Y %H:%i'
        ) AS wen
    FROM {{ source('iasworld', 'oby') }} AS oby
    WHERE oby.cur = 'Y'
        AND oby.deactivat IS NULL
)

SELECT
    calc.parid,
    calc.taxyr,
    legdat.user1 AS township_code,
    pardat.class,
    aprval.reascd AS reason_for_change,
    DATE_FORMAT(DATE_PARSE(aprval.revdt, '%Y-%m-%d %H:%i:%S.%f'), '%c/%e/%Y')
        AS revdt,
    aprval.aprbldg,
    aprval_prev.aprbldg AS aprbldg_prev,
    calc.table_name,
    calc.card,
    calc.lline,
    calc.external_rcnld,
    calc.external_occpct,
    calc.external_propct,
    calc.external_calc_rcnld,
    calc.ovrrcnld,
    calc.value_difference,
    calc.card_code,
    calc.alt_cdu,
    calc.who,
    calc.wen
FROM combined_calc AS calc
LEFT JOIN {{ source('iasworld', 'pardat') }} AS pardat
    ON calc.parid = pardat.parid
    AND calc.taxyr = pardat.taxyr
    AND pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON calc.parid = legdat.parid
    AND calc.taxyr = legdat.taxyr
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'aprval') }} AS aprval
    ON calc.parid = aprval.parid
    AND calc.taxyr = aprval.taxyr
    AND aprval.cur = 'Y'
    AND aprval.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'aprval') }} AS aprval_prev
    ON aprval.parid = aprval_prev.parid
    AND CAST(aprval.taxyr AS INT) = CAST(aprval_prev.taxyr AS INT) + 1
    AND aprval_prev.cur = 'Y'
    AND aprval_prev.deactivat IS NULL
WHERE calc.value_difference != 0
    AND (aprval.reascd IS NULL OR aprval.reascd != '22')
