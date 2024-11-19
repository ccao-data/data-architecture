-- Calculate YoY changes to occupancy percentages in DWELDAT
WITH dweldat_change AS (
    SELECT
        dweldat_prev.parid,
        -- If there is a record for a card in the prior year but not in the
        -- current year, infer the current year based on the prior year.
        -- Even though we use prior year data for the rest of the identifiers
        -- that we select, we want to select the current taxyr here since
        -- that's what we use to filter QC reports
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
        -- Avoid division by zero errors by only computing the percent change
        -- in bldgval if current bldgval is present, otherwise fall back to a
        -- null value
        CASE
            WHEN dweldat.adjrcnld != 0
                THEN ROUND(
                    (
                        dweldat_prev.adjrcnld
                        / CAST(dweldat.adjrcnld AS DOUBLE)
                    )
                    * 100,
                    1
                )
        END AS pct_prev_of_cur_adjrcnld,
        CASE
            WHEN dweldat.adjrcnld != 0
                THEN ROUND(
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
        END AS difference_in_pct
    -- Select from the prior year of data as the base of the query so that we
    -- can preserve parcels that may have changed in the subsequent year
    -- such that they don't appear in DWELDAT anymore
    FROM {{ source('iasworld', 'dweldat') }} AS dweldat_prev
    LEFT JOIN {{ source('iasworld', 'dweldat') }} AS dweldat
        ON dweldat_prev.parid = dweldat.parid
        AND dweldat_prev.card = dweldat.card
        AND CAST(dweldat.taxyr AS INT) = CAST(dweldat_prev.taxyr AS INT) + 1
        AND dweldat.cur = 'Y'
        AND dweldat.deactivat IS NULL
    WHERE dweldat_prev.cur = 'Y'
        AND dweldat_prev.deactivat IS NULL
        -- Filter for prior year cards with one year market value relief
        AND dweldat_prev.mktrsn IN ('5', '5B')
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
-- Filter out DWELDAT rows that have no change. We perform this filtering
-- in the main query rather than the subquery since it needs to operate
-- on difference_in_pct, which is a field that the subquery computes
WHERE (
    dweldat_change.difference_in_pct IS NULL
    OR dweldat_change.difference_in_pct != 0
)
