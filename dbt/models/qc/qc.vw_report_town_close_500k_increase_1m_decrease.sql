WITH card_code_info AS (
    SELECT
        pardat.parid,
        pardat.taxyr,
        CASE
            WHEN
                CONTAINS(
                    ARRAY_AGG(SUBSTRING(COALESCE(dweldat.mktrsn, ''), 1, 1)),
                    '5'
                )
                THEN 'Y'
            ELSE ''
        END AS dweldat_code_5,
        CASE
            WHEN
                CONTAINS(
                    ARRAY_AGG(SUBSTRING(COALESCE(comdat.chgrsn, ''), 1, 1)), '5'
                )
                THEN 'Y'
            ELSE ''
        END AS comdat_code_5,
        CASE
            WHEN
                CONTAINS(
                    ARRAY_AGG(SUBSTRING(COALESCE(oby.chgrsn, ''), 1, 1)), '5'
                )
                THEN 'Y'
            ELSE ''
        END AS oby_code_5
    FROM {{ source('iasworld', 'pardat') }} AS pardat
    LEFT JOIN {{ source('iasworld', 'dweldat') }} AS dweldat
        ON pardat.parid = dweldat.parid
        AND pardat.taxyr = dweldat.taxyr
        AND dweldat.cur = 'Y'
        AND dweldat.deactivat IS NULL
    LEFT JOIN {{ source('iasworld', 'comdat') }} AS comdat
        ON pardat.parid = comdat.parid
        AND pardat.taxyr = comdat.taxyr
        AND comdat.cur = 'Y'
        AND comdat.deactivat IS NULL
    LEFT JOIN {{ source('iasworld', 'oby') }} AS oby
        ON pardat.parid = oby.parid
        AND pardat.taxyr = oby.taxyr
        AND oby.cur = 'Y'
        AND oby.deactivat IS NULL
    WHERE pardat.cur = 'Y'
        AND pardat.deactivat IS NULL
    GROUP BY pardat.parid, pardat.taxyr
)

SELECT
    asmt.parid,
    asmt.taxyr,
    legdat.user1 AS township_code,
    pardat.class,
    aprval.reascd,
    asmt_prev.valapr1 AS valapr1_prev,
    asmt_prev.valapr2 AS valapr2_prev,
    asmt_prev.valapr3 AS valapr3_prev,
    asmt.valapr1,
    asmt.valapr2,
    asmt.valapr3,
    asmt.valapr3 - asmt_prev.valapr3 AS difference,
    CONCAT(
        CAST(
            (
                (asmt.valapr3 - asmt_prev.valapr3)
                / CAST(asmt_prev.valapr3 AS DOUBLE)
            )
            * 100 AS VARCHAR
        ),
        '%'
    ) AS percent_change,
    code.dweldat_code_5,
    code.comdat_code_5,
    code.oby_code_5
FROM {{ source('iasworld', 'asmt_all') }} AS asmt
LEFT JOIN {{ source('iasworld', 'asmt_all') }} AS asmt_prev
    ON asmt.parid = asmt_prev.parid
    AND CAST(asmt.taxyr AS INT) = CAST(asmt_prev.taxyr AS INT) + 1
    AND asmt_prev.cur = 'Y'
    AND asmt_prev.deactivat IS NULL
    AND asmt_prev.valclass IS NULL
LEFT JOIN card_code_info AS code
    ON asmt.parid = code.parid
    AND asmt.taxyr = code.taxyr
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON asmt.parid = legdat.parid
    AND asmt.taxyr = legdat.taxyr
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'pardat') }} AS pardat
    ON asmt.parid = pardat.parid
    AND asmt.taxyr = pardat.taxyr
    AND pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'aprval') }} AS aprval
    ON asmt.parid = aprval.parid
    AND asmt.taxyr = aprval.taxyr
    AND aprval.cur = 'Y'
    AND aprval.deactivat IS NULL
WHERE asmt.cur = 'Y'
    AND asmt.deactivat IS NULL
    AND asmt.valclass IS NULL
    AND (
        asmt.valapr3 - asmt_prev.valapr3 >= 500000
        OR asmt.valapr3 - asmt_prev.valapr3 <= -1000000
    )
