-- Calculate YoY changes to occupancy percentages in OBY
WITH oby_change AS (
    SELECT
        oby_prev.parid,
        -- If there is a record for a card in the prior year but not in the
        -- current year, infer the current year based on the prior year.
        -- Even though we use prior year data for the rest of the identifiers
        -- that we select, we want to select the current taxyr here since
        -- that's what we use to filter QC reports
        COALESCE(
            oby.taxyr,
            CAST(CAST(oby_prev.taxyr AS INT) + 1 AS VARCHAR)
        ) AS taxyr,
        oby_prev.card,
        oby_prev.chgrsn,
        oby_prev.lline,
        oby.adjrcnld,
        oby_prev.adjrcnld AS adjrcnld_prev,
        -- The data switch from mktadj to external_occpct around 2023 but are
        -- not consistent about that switch, so consolidate the two fields
        ROUND(
            COALESCE(oby_prev.external_occpct, oby_prev.mktadj),
            1
        ) AS external_occpct_prev,
        oby.external_occpct,
        -- Avoid division by zero errors by only computing the percent change
        -- in bldgval if current bldgval is present, otherwise fall back to a
        -- null value
        CASE
            WHEN oby.adjrcnld != 0
                THEN ROUND(
                    (oby_prev.adjrcnld / CAST(oby.adjrcnld AS DOUBLE))
                    * 100,
                    1
                )
        END AS pct_prev_of_cur_adjrcnld,
        CASE
            WHEN oby.adjrcnld != 0
                THEN ROUND(
                    ROUND(
                        (
                            oby_prev.adjrcnld
                            / CAST(oby.adjrcnld AS DOUBLE)
                        )
                        * 100,
                        1
                    )
                    - COALESCE(
                        oby_prev.external_occpct,
                        oby_prev.mktadj
                    ),
                    1
                )
        END AS difference_in_pct
    -- Select from the prior year of data as the base of the query so that we
    -- can preserve parcels that may have changed in the subsequent year
    -- such that they don't appear in OBY anymore
    FROM {{ source('iasworld', 'oby') }} AS oby_prev
    LEFT JOIN {{ source('iasworld', 'oby') }} AS oby
        ON oby_prev.parid = oby.parid
        AND oby_prev.card = oby.card
        AND oby_prev.lline = oby.lline
        AND CAST(oby.taxyr AS INT) = CAST(oby_prev.taxyr AS INT) + 1
        AND oby.cur = 'Y'
        AND oby.deactivat IS NULL
    WHERE oby_prev.cur = 'Y'
        AND oby_prev.deactivat IS NULL
        -- Filter for prior year cards with one year market value relief
        AND oby_prev.chgrsn IN ('5', '5B')
)

SELECT
    oby_change.parid,
    oby_change.taxyr,
    asmt.township_code,
    asmt.class,
    asmt.own1,
    asmt.reascd,
    asmt.who,
    oby_change.card,
    oby_change.lline,
    oby_change.chgrsn,
    oby_change.external_occpct_prev,
    oby_change.external_occpct,
    oby_change.pct_prev_of_cur_adjrcnld,
    oby_change.difference_in_pct,
    oby_change.adjrcnld_prev,
    oby_change.adjrcnld,
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
FROM oby_change
LEFT JOIN {{ ref('qc.vw_iasworld_asmt_all_with_prior_year_values') }} AS asmt
    ON oby_change.parid = asmt.parid
    AND oby_change.taxyr = asmt.taxyr
-- Filter out OBY rows that have no change. We perform this filtering
-- in the main query rather than the subquery since it needs to operate
-- on difference_in_pct, which is a field that the subquery computes
WHERE (
    oby_change.difference_in_pct IS NULL
    OR oby_change.difference_in_pct != 0
)
