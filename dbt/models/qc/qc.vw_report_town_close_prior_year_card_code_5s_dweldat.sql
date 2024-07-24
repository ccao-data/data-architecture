WITH dweldat_change AS (
    SELECT
        dweldat_prev.parid,
        -- If there is a record for a card in the prior year but not in the
        -- current year, infer the current year based on the prior year
        COALESCE(
            dweldat.taxyr,
            CAST(CAST(dweldat_prev.taxyr AS INT) + 1 AS VARCHAR)
        ) AS taxyr,
        dweldat_prev.card,
        dweldat_prev.mktrsn,
        dweldat.adjrcnld,
        dweldat_prev.adjrcnld AS adjrcnld_prev,
        -- The data switch from mktadj to external_occpct around 2023 but are
        -- not consistent about that switch, so consolidate the two fields
        ROUND(
            COALESCE(dweldat_prev.external_occpct, dweldat_prev.mktadj),
            1
        ) AS external_occpct_prev,
        dweldat.external_occpct,
        CASE
            WHEN dweldat.adjrcnld != 0
                THEN CAST(
                    ROUND(
                        (
                            dweldat_prev.adjrcnld
                            / CAST(dweldat.adjrcnld AS DOUBLE)
                        )
                        * 100,
                        1
                    )
                    AS VARCHAR
                )
            ELSE 'N/A'
        END AS pct_prev_of_cur_adjrcnld,
        CASE
            WHEN dweldat.adjrcnld != 0
                THEN CAST(
                    ROUND(
                        ROUND(
                            (
                                dweldat_prev.adjrcnld
                                / CAST(dweldat.adjrcnld AS DOUBLE)
                            )
                            * 100,
                            1
                        )
                        - COALESCE(
                            dweldat_prev.external_occpct,
                            dweldat_prev.mktadj
                        ),
                        1
                    )
                    AS VARCHAR
                )
            ELSE 'N/A'
        END AS difference_in_pct
    -- Select from the prior year of data as the base of the query so that we
    -- can preserve parcels that may have changed in the following year
    FROM {{ source('iasworld', 'dweldat') }} AS dweldat_prev
    LEFT JOIN {{ source('iasworld', 'dweldat') }} AS dweldat
        ON dweldat_prev.parid = dweldat.parid
        AND dweldat_prev.card = dweldat.card
        AND CAST(dweldat.taxyr AS INT) = CAST(dweldat_prev.taxyr AS INT) + 1
        AND dweldat.cur = 'Y'
        AND dweldat.deactivat IS NULL
    WHERE dweldat_prev.cur = 'Y'
        AND dweldat_prev.deactivat IS NULL
)

SELECT
    dweldat_change.parid,
    dweldat_change.taxyr,
    asmt.township_code,
    asmt.class,
    asmt.own1,
    asmt.reascd,
    asmt.who,
    dweldat_change.card,
    dweldat_change.mktrsn,
    dweldat_change.external_occpct_prev,
    dweldat_change.external_occpct,
    dweldat_change.pct_prev_of_cur_adjrcnld,
    dweldat_change.difference_in_pct,
    dweldat_change.adjrcnld_prev,
    dweldat_change.adjrcnld,
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
FROM dweldat_change
LEFT JOIN {{ ref('qc.vw_iasworld_asmt_all_with_prior_year_values') }} AS asmt
    ON dweldat_change.parid = asmt.parid
    AND dweldat_change.taxyr = asmt.taxyr
WHERE dweldat_change.mktrsn IN ('5', '5B')
    AND dweldat_change.difference_in_pct != '0.0'
