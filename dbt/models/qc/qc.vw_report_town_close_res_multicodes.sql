SELECT
    asmt.parid,
    asmt.taxyr,
    asmt.township_code,
    asmt.class AS parcel_class,
    asmt.nbhd,
    dweldat.card,
    dweldat.class AS dweldat_class,
    dweldat.sfla,
    dweldat.adjrcnld,
    dweldat_prev.adjrcnld AS adjrcnld_prev,
    asmt.valapr1,
    asmt.valapr2,
    asmt.valapr3,
    asmt.valapr1_prev,
    asmt.valapr2_prev,
    asmt.valapr3_prev,
    sale.saledt_fmt,
    sale.price
FROM {{ source('iasworld', 'dweldat') }} AS dweldat
-- Filter for only dwellings on multicard parcels
INNER JOIN (
    SELECT
        parid,
        taxyr
    FROM {{ source('iasworld', 'dweldat') }}
    WHERE cur = 'Y'
        AND deactivat IS NULL
    GROUP BY parid, taxyr
    HAVING COUNT(*) > 1
) AS multicard_parcel
    ON dweldat.parid = multicard_parcel.parid
    AND dweldat.taxyr = multicard_parcel.taxyr
-- Join to the prior year of dweldat data to pull the prior building value
LEFT JOIN {{ source('iasworld', 'dweldat') }} AS dweldat_prev
    ON dweldat.parid = dweldat_prev.parid
    AND dweldat.card = dweldat_prev.card
    AND CAST(dweldat.taxyr AS INT) = CAST(dweldat_prev.taxyr AS INT) + 1
    AND dweldat_prev.cur = 'Y'
    AND dweldat_prev.deactivat IS NULL
LEFT JOIN {{ ref('qc.vw_iasworld_asmt_all_with_prior_year_values') }} AS asmt
    ON dweldat.parid = asmt.parid
    AND dweldat.taxyr = asmt.taxyr
LEFT JOIN {{ ref('qc.vw_iasworld_sales_latest_sale') }} AS sale
    ON dweldat.parid = sale.parid
    -- Filter for only sales starting in 2021
    AND sale.saledt >= '2021-01-01'
WHERE dweldat.cur = 'Y'
    AND dweldat.deactivat IS NULL
