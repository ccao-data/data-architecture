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
    calc.parid AS "PARID",
    calc.taxyr AS "TAXYR",
    legdat.user1 AS "TOWNSHIP",
    pardat.class AS "Parcel Class",
    aprval.reascd AS "Reason for Change",
    DATE_FORMAT(DATE_PARSE(aprval.revdt, '%Y-%m-%d %H:%i:%S.%f'), '%c/%e/%Y')
        AS "Review Date",
    aprval.aprbldg AS "Curr. Year BMV",
    aprval_prev.aprbldg AS "Prior Year BMV",
    calc.table_name AS "Table",
    calc.card AS "CARD",
    calc.lline AS "Line",
    calc.external_rcnld AS "EXTERNAL_RCNLD",
    calc.external_occpct AS "EXTERNAL_OCCPCT",
    calc.external_propct AS "EXTERNAL_PROPCT",
    calc.external_calc_rcnld AS "EXTERNAL_CALC_RCNLD",
    calc.ovrrcnld AS "OVRRCNLD",
    calc.value_difference AS "Value difference",
    calc.card_code AS "Card Code",
    calc.alt_cdu AS "Alternative CDU",
    calc.who AS "WHO",
    calc.wen AS "WEN"
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
