-- Check dweldat, comdat, and oby for card code 5s, i.e. cards that had a
-- one-year reduction in value that might explain a big year-over-year change
WITH card_code_info AS (
    SELECT
        pardat.parid,
        pardat.taxyr,
        CASE
            WHEN
                -- There can be multiple cards per parcel, and there are a
                -- couple different codes starting with 5 that we want to
                -- classify as "card code 5s", so we need to aggregate the card
                -- codes into an array and then check if any of them start
                -- with 5
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
    asmt.township_code,
    asmt.class,
    asmt.reascd,
    asmt.valapr1_prev,
    asmt.valapr2_prev,
    asmt.valapr3_prev,
    asmt.valapr1,
    asmt.valapr2,
    asmt.valapr3,
    asmt.valapr3 - asmt.valapr3_prev AS difference,
    CASE
        WHEN asmt.valapr3_prev != 0
            THEN CONCAT(
                CAST(
                    ROUND(
                        (
                            (asmt.valapr3 - asmt.valapr3_prev)
                            / CAST(asmt.valapr3_prev AS DOUBLE)
                        )
                        * 100,
                        2
                    ) AS VARCHAR
                ),
                '%'
            )
    END AS percent_change,
    code.dweldat_code_5,
    code.comdat_code_5,
    code.oby_code_5
FROM {{ ref('qc.vw_iasworld_asmt_all_with_prior_year_values') }} AS asmt
LEFT JOIN card_code_info AS code
    ON asmt.parid = code.parid
    -- Join to prior year card code info, since we want to know the code value
    -- last year
    AND CAST(CAST(asmt.taxyr AS INT) - 1 AS VARCHAR) = code.taxyr
WHERE (
    asmt.valapr3 - asmt.valapr3_prev >= 500000
    OR asmt.valapr3 - asmt.valapr3_prev <= -1000000
)
