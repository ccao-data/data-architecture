SELECT
    asmt.parid,
    asmt.taxyr,
    legdat.user1 AS township_code,
    pardat.class AS parcel_class,
    pardat.nbhd,
    dweldat.card,
    dweldat.class AS dweldat_class,
    dweldat.sfla,
    dweldat.adjrcnld,
    dweldat_prev.adjrcnld AS adjrcnld_prev,
    asmt.valapr1,
    asmt.valapr2,
    asmt.valapr3,
    asmt_prev.valapr1 AS valapr1_prev,
    asmt_prev.valapr2 AS valapr2_prev,
    asmt_prev.valapr3 AS valapr3_prev,
    latest_sale.saledt,
    latest_sale.price
FROM {{ source('iasworld', 'dweldat') }} AS dweldat
-- Filter for only dwellings on multicard parcels
INNER JOIN (
    SELECT
        parid,
        taxyr
    FROM {{ source('iasworld', 'dweldat') }}
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
LEFT JOIN {{ source('iasworld', 'asmt_all') }} AS asmt
    ON dweldat.parid = asmt.parid
    AND dweldat.taxyr = asmt.taxyr
    AND asmt.cur = 'Y'
    AND asmt.deactivat IS NULL
    AND asmt.valclass IS NULL
-- Join to the prior year of asmt data to pull the prior assessed values
LEFT JOIN {{ source('iasworld', 'asmt_all') }} AS asmt_prev
    ON asmt.parid = asmt_prev.parid
    AND CAST(asmt.taxyr AS INT) = CAST(asmt_prev.taxyr AS INT) + 1
    AND asmt_prev.cur = 'Y'
    AND asmt_prev.deactivat IS NULL
    AND asmt_prev.valclass IS NULL
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON dweldat.taxyr = legdat.taxyr
    AND dweldat.parid = legdat.parid
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'pardat') }} AS pardat
    ON dweldat.taxyr = pardat.taxyr
    AND dweldat.parid = pardat.parid
    AND pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
-- Get the most recent sale for each parcel, if one exists
LEFT JOIN (
    SELECT
        parid,
        saledt,
        price
    FROM (
        SELECT
            parid,
            saledt,
            price,
            ROW_NUMBER()
                OVER (PARTITION BY parid ORDER BY saledt DESC)
                AS row_num
        FROM {{ source('iasworld', 'sales') }}
        WHERE saledt >= '2021-01-1'
            AND price > 1
    ) AS ranked_sales
    WHERE row_num = 1
) AS latest_sale
    ON dweldat.parid = latest_sale.parid
WHERE dweldat.cur = 'Y'
    AND dweldat.deactivat IS NULL
