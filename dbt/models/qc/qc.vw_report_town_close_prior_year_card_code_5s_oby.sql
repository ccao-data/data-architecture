SELECT
    asmt.parid,
    -- If there is a record for a card in the prior year but not in the
    -- current year, infer the current year based on the prior year
    COALESCE(
        oby.taxyr,
        CAST(CAST(oby_prev.taxyr AS INT) + 1 AS VARCHAR)
    ) AS taxyr,
    asmt.township_code,
    asmt.class,
    asmt.own1,
    asmt.reascd,
    asmt.who,
    oby_prev.card,
    oby_prev.lline,
    oby_prev.chgrsn,
    -- The data switch from mktadj to external_occpct around 2023 but are
    -- not consistent about that switch, so consolidate the two fields
    COALESCE(oby_prev.external_occpct, oby_prev.mktadj)
        AS external_occpct_prev,
    oby.external_occpct,
    oby.external_occpct
    - COALESCE(oby_prev.external_occpct, oby_prev.mktadj, 0)
        AS external_occpct_difference,
    oby_prev.adjrcnld AS adjrcnld_prev,
    oby.adjrcnld,
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
-- Select from the prior year of data as the base of the query so that we
-- can preserve parcels that may have changed in the following year
FROM {{ source('iasworld', 'oby') }} AS oby_prev
LEFT JOIN {{ source('iasworld', 'oby') }} AS oby
    ON oby.parid = oby_prev.parid
    AND oby.card = oby_prev.card
    AND oby.lline = oby_prev.lline
    AND CAST(oby.taxyr AS INT) = CAST(oby_prev.taxyr AS INT) + 1
    AND oby.cur = 'Y'
    AND oby.deactivat IS NULL
LEFT JOIN {{ ref('qc.vw_iasworld_asmt_all_with_prior_year_values') }} AS asmt
    ON asmt.parid = oby_prev.parid
    AND asmt.taxyr = oby_prev.taxyr
WHERE oby_prev.cur = 'Y'
    AND oby_prev.deactivat IS NULL
    AND oby_prev.chgrsn IN ('5', '5B')
