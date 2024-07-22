SELECT
    asmt.parid,
    -- If there is a record for a card in the prior year but not in the
    -- current year, infer the current year based on the prior year
    COALESCE(
        comdat.taxyr,
        CAST(CAST(comdat_prev.taxyr AS INT) + 1 AS VARCHAR)
    ) AS taxyr,
    asmt.township_code,
    asmt.class,
    asmt.own1,
    asmt.reascd,
    asmt.who,
    comdat_prev.card,
    comdat_prev.chgrsn,
    -- The data switch from mktadj to external_occpct around 2023 but are
    -- not consistent about that switch, so consolidate the two fields
    COALESCE(comdat_prev.external_occpct, comdat_prev.mktadj)
        AS external_occpct_prev,
    comdat.external_occpct,
    comdat.external_occpct
    - COALESCE(comdat_prev.external_occpct, comdat_prev.mktadj, 0)
        AS external_occpct_difference,
    comdat_prev.bldgval AS bldgval_prev,
    comdat.bldgval,
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
FROM {{ source('iasworld', 'comdat') }} AS comdat_prev
LEFT JOIN {{ source('iasworld', 'comdat') }} AS comdat
    ON comdat.parid = comdat_prev.parid
    AND comdat.card = comdat_prev.card
    AND CAST(comdat.taxyr AS INT) = CAST(comdat_prev.taxyr AS INT) + 1
    AND comdat.cur = 'Y'
    AND comdat.deactivat IS NULL
LEFT JOIN {{ ref('qc.vw_iasworld_asmt_all_with_prior_year_values') }} AS asmt
    ON asmt.parid = comdat_prev.parid
    AND asmt.taxyr = comdat_prev.taxyr
WHERE comdat_prev.cur = 'Y'
    AND comdat_prev.deactivat IS NULL
    AND comdat_prev.chgrsn IN ('5', '5B')
