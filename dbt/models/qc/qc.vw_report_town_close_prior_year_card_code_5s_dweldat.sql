SELECT
    asmt.parid,
    -- If there is a record for a card in the prior year but not in the
    -- current year, infer the current year based on the prior year
    COALESCE(
        dweldat.taxyr,
        CAST(CAST(dweldat_prev.taxyr AS INT) + 1 AS VARCHAR)
    ) AS taxyr,
    asmt.township_code,
    asmt.class,
    asmt.own1,
    asmt.reascd,
    asmt.who,
    dweldat_prev.card,
    dweldat_prev.mktrsn,
    -- The data switch from mktadj to external_occpct around 2023 but are
    -- not consistent about that switch, so consolidate the two fields
    COALESCE(dweldat_prev.external_occpct, dweldat_prev.mktadj)
        AS external_occpct_prev,
    dweldat.external_occpct,
    dweldat.external_occpct
    - COALESCE(dweldat_prev.external_occpct, dweldat_prev.mktadj, 0)
        AS external_occpct_difference,
    dweldat_prev.adjrcnld AS adjrcnld_prev,
    dweldat.adjrcnld,
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
FROM {{ source('iasworld', 'dweldat') }} AS dweldat_prev
LEFT JOIN {{ source('iasworld', 'dweldat') }} AS dweldat
    ON dweldat.parid = dweldat_prev.parid
    AND dweldat.card = dweldat_prev.card
    AND CAST(dweldat.taxyr AS INT) = CAST(dweldat_prev.taxyr AS INT) + 1
    AND dweldat.cur = 'Y'
    AND dweldat.deactivat IS NULL
LEFT JOIN {{ ref('qc.vw_iasworld_asmt_all_with_prior_year_values') }} AS asmt
    ON asmt.parid = dweldat_prev.parid
    AND asmt.taxyr = dweldat_prev.taxyr
WHERE dweldat_prev.cur = 'Y'
    AND dweldat_prev.deactivat IS NULL
    AND dweldat_prev.mktrsn IN ('5', '5B')
