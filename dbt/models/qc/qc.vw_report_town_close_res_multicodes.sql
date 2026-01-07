SELECT
    asmt.parid AS "PARID",
    asmt.taxyr AS "TAXYR",
    asmt.township_code AS "TOWNSHIP",
    asmt.class AS "Parcel Class",
    asmt.nbhd AS "NBHD",
    dweldat.card AS "CARD",
    dweldat.class AS "DWELDAT Class",
    dweldat.sfla AS "Dwelling SF",
    dweldat.adjrcnld AS "Curr. Year Card BMV",
    dweldat_prev.adjrcnld AS "Prior Year Card BMV",
    asmt.valapr1 AS "Curr. Year LMV",
    asmt.valapr2 AS "Curr. Year BMV",
    asmt.valapr3 AS "Curr. Year Total MV",
    asmt.valapr1_prev AS "Prior Year LMV",
    asmt.valapr2_prev AS "Prior Year BMV",
    asmt.valapr3_prev AS "Prior Year Total MV",
    sale.saledt_fmt AS "Sale Date",
    sale.price AS "Sale Price"
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
