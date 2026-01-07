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
    dweldat_change.parid AS "PARID",
    dweldat_change.taxyr AS "TAXYR",
    asmt.township_code AS "TOWNSHIP",
    asmt.class AS "CLASS",
    asmt.own1 AS "OWN1",
    asmt.reascd AS "Reason for Change",
    asmt.who AS "WHO",
    dweldat_change.card AS "CARD",
    dweldat_change.mktrsn AS "Card Code",
    dweldat_change.external_occpct_prev AS "Prior Year Occ %",
    dweldat_change.external_occpct AS "Curr. Year Occ %",
    dweldat_change.pct_prev_of_cur_adjrcnld AS "Prior Year % of Curr. Year",
    dweldat_change.difference_in_pct AS "Difference in %",
    dweldat_change.adjrcnld_prev AS "Prior Year DWELDAT VAL",
    dweldat_change.adjrcnld AS "Curr. Year DWELDAT VAL",
    asmt.valapr1_prev AS "Prior Year LMV",
    asmt.valapr2_prev AS "Prior Year BMV",
    asmt.valapr3_prev AS "Prior Year Total MV",
    asmt.valasm1_prev AS "Prior Year LAV",
    asmt.valasm2_prev AS "Prior Year BAV",
    asmt.valasm3_prev AS "Prior Year Total AV",
    asmt.valapr1 AS "Curr. Year LMV",
    asmt.valapr2 AS "Curr. Year BMV",
    asmt.valapr3 AS "Curr. Year Total MV",
    asmt.valasm1 AS "Curr. Year LAV",
    asmt.valasm2 AS "Curr. Year BAV",
    asmt.valasm3 AS "Curr. Year Total AV"
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
