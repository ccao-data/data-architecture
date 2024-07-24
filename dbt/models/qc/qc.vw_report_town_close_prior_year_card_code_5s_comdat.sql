WITH comdat_change AS (
    SELECT
        comdat_prev.parid,
        -- If there is a record for a card in the prior year but not in the
        -- current year, infer the current year based on the prior year
        COALESCE(
            comdat.taxyr,
            CAST(CAST(comdat_prev.taxyr AS INT) + 1 AS VARCHAR)
        ) AS taxyr,
        comdat_prev.card,
        comdat_prev.chgrsn,
        comdat.bldgval,
        comdat_prev.bldgval AS bldgval_prev,
        -- The data switch from mktadj to external_occpct around 2023 but are
        -- not consistent about that switch, so consolidate the two fields
        ROUND(
            COALESCE(comdat_prev.external_occpct, comdat_prev.mktadj),
            1
        ) AS external_occpct_prev,
        comdat.external_occpct,
        CASE
            WHEN comdat.bldgval != 0
                THEN ROUND(
                    (comdat_prev.bldgval / CAST(comdat.bldgval AS DOUBLE))
                    * 100,
                    1
                )
        END AS pct_prev_of_cur_bldgval,
        CASE
            WHEN comdat.bldgval != 0
                THEN ROUND(
                    ROUND(
                        (
                            comdat_prev.bldgval
                            / CAST(comdat.bldgval AS DOUBLE)
                        )
                        * 100,
                        1
                    )
                    - COALESCE(
                        comdat_prev.external_occpct,
                        comdat_prev.mktadj
                    ),
                    1
                )
        END AS difference_in_pct
    -- Select from the prior year of data as the base of the query so that we
    -- can preserve parcels that may have changed in the following year
    FROM {{ source('iasworld', 'comdat') }} AS comdat_prev
    LEFT JOIN {{ source('iasworld', 'comdat') }} AS comdat
        ON comdat_prev.parid = comdat.parid
        AND comdat_prev.card = comdat.card
        AND CAST(comdat.taxyr AS INT) = CAST(comdat_prev.taxyr AS INT) + 1
        AND comdat.cur = 'Y'
        AND comdat.deactivat IS NULL
    WHERE comdat_prev.cur = 'Y'
        AND comdat_prev.deactivat IS NULL
)

SELECT
    comdat_change.parid,
    comdat_change.taxyr,
    asmt.township_code,
    asmt.class,
    asmt.own1,
    asmt.reascd,
    asmt.who,
    comdat_change.card,
    comdat_change.chgrsn,
    comdat_change.external_occpct_prev,
    comdat_change.external_occpct,
    comdat_change.pct_prev_of_cur_bldgval,
    comdat_change.difference_in_pct,
    comdat_change.bldgval_prev,
    comdat_change.bldgval,
    asmt.valapr1_prev,
    asmt.valapr2_prev,
    asmt.valapr3_prev,
    asmt.valasm1_prev,
    asmt.valasm2_prev,
    asmt.valasm3_prev,
    asmt.valapr1,
    asmt.valapr2,
    asmt.valapr3,
    asmt.valasm1,
    asmt.valasm2,
    asmt.valasm3
FROM comdat_change
LEFT JOIN {{ ref('qc.vw_iasworld_asmt_all_with_prior_year_values') }} AS asmt
    ON comdat_change.parid = asmt.parid
    AND comdat_change.taxyr = asmt.taxyr
WHERE comdat_change.chgrsn IN ('5', '5B')
    AND (
        comdat_change.difference_in_pct IS NULL
        OR comdat_change.difference_in_pct != 0
    )
