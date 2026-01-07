-- Get up to the first five DWELDAT cards for each parcel
WITH first_five_dwellings AS (
    SELECT
        parid,
        taxyr,
        card,
        yrblt,
        sfla,
        stories,
        rmbed,
        fixbath,
        fixhalf,
        bsmt,
        user12 AS bsmt_fin,
        external_propct,
        user16 AS alt_cdu,
        row_num
    FROM (
        SELECT
            *,
            ROW_NUMBER()
                OVER (PARTITION BY parid, taxyr ORDER BY card ASC)
                AS row_num
        FROM {{ source('iasworld', 'dweldat') }}
        WHERE cur = 'Y'
            AND deactivat IS NULL
    ) AS ranked_dwellings
    WHERE row_num <= 5
),

-- Pivot the dwelling chars using the MAP_AGG function and key access
-- as recommended here:
-- https://gist.github.com/shotahorii/6b710c902a8a6ef184987ca787d329d9
first_five_dwellings_pivoted AS (
    SELECT
        parid,
        taxyr,
    {% for idx in range(1, 6) %}
        yrblt_kv[{{ idx }}] AS yrblt_{{ idx }},
        sfla_kv[{{ idx }}] AS sfla_{{ idx }},
        stories_kv[{{ idx }}] AS stories_{{ idx }},
        rmbed_kv[{{ idx }}] AS rmbed_{{ idx }},
        fixbath_kv[{{ idx }}] AS fixbath_{{ idx }},
        fixhalf_kv[{{ idx }}] AS fixhalf_{{ idx }},
        bsmt_kv[{{ idx }}] AS bsmt_{{ idx }},
        bsmt_fin_kv[{{ idx }}] AS bsmt_fin_{{ idx }},
        external_propct_kv[{{ idx }}] AS external_propct_{{ idx }},
        alt_cdu_kv[{{ idx }}] AS alt_cdu_{{ idx }}{% if not loop.last %}
            ,
        {% endif %}
    {% endfor %}
    FROM (
        SELECT
            parid,
            taxyr,
            MAP_AGG(row_num, yrblt) AS yrblt_kv,
            MAP_AGG(row_num, sfla) AS sfla_kv,
            MAP_AGG(row_num, stories) AS stories_kv,
            MAP_AGG(row_num, rmbed) AS rmbed_kv,
            MAP_AGG(row_num, fixbath) AS fixbath_kv,
            MAP_AGG(row_num, fixhalf) AS fixhalf_kv,
            MAP_AGG(row_num, bsmt) AS bsmt_kv,
            MAP_AGG(row_num, bsmt_fin) AS bsmt_fin_kv,
            MAP_AGG(row_num, external_propct) AS external_propct_kv,
            MAP_AGG(row_num, alt_cdu) AS alt_cdu_kv
        FROM first_five_dwellings
        GROUP BY parid, taxyr
    ) AS agg_dwellings
),

num_dwellings AS (
    SELECT
        parid,
        taxyr,
        COUNT(*) AS num_dwellings
    FROM {{ source('iasworld', 'dweldat') }}
    WHERE cur = 'Y'
        AND deactivat IS NULL
    GROUP BY parid, taxyr
)

SELECT
    pardat.parid AS "PARID",
    pardat.taxyr AS "TAXYR",
    legdat.user1 AS "TOWNSHIP",
    pardat.nbhd AS "Nbhd",
    pardat.class AS "Class",
    legdat.adrno AS "ADRNO",
    legdat.adradd AS "ADRADD",
    legdat.adrdir AS "ADRDIR",
    legdat.adrstr AS "ADRSTR",
    legdat.adrsuf AS "ADRSUF",
    legdat.adrsuf2 AS "ADRSUF2",
    legdat.unitdesc AS "UNITDESC",
    legdat.unitno AS "UNITNO",
    legdat.cityname AS "City",
    aprval.aprland AS "Curr. Year LMV",
    aprval.aprbldg AS "Crr. Year BMV",
    aprval.aprtot AS "Curr. Year TMV",
    aprval_prev.aprland AS "Prior Year LMV",
    aprval_prev.aprbldg AS "Prior Year BMV",
    aprval_prev.aprtot AS "Prior Year TMV",
    ROUND(
        (
            (aprval.aprtot - aprval_prev.aprtot)
            / CAST(aprval_prev.aprtot AS DOUBLE)
        ),
        2
    ) AS "Total % Change",
    aprval.dwelval AS "Curr. Year Dwelling MV",
    aprval.dwelval + aprval.aprland AS "Curr. Year Dwelling Total",
    aprval_prev.dwelval AS "Prior Year Dwelling MV",
    aprval_prev.dwelval + aprval_prev.aprland AS "Prior Year Dwelling Total",
    ROUND(
        (
            (
                (aprval.dwelval + aprval.aprland)
                - (aprval_prev.dwelval + aprval_prev.aprland)
            )
            / CAST(
                (aprval_prev.dwelval + aprval_prev.aprland) AS DOUBLE
            )
        ),
        2
    ) AS "Dwelling % Change",
    sale.saledt_fmt AS "Sale Date",
    sale.price AS "Sale Price",
    sale.instruno AS "Instrument No",
    COALESCE(pardat.tiebldgpct, 0) AS "PARDAT Proration",
    aprval.reascd AS "Reason for Change",
    aprval.obyval AS "OBY Value",
    land.sf AS "LAND_SF",
    num_dwellings.num_dwellings AS "Dwelling Count",
{% for idx in range(1, 6) %}
    dweldat.yrblt_{{ idx }} AS "Yr Built Card {{ idx }}",
    dweldat.sfla_{{ idx }} AS "SFLA Card {{ idx }}",
    dweldat.stories_{{ idx }} AS "Story Height Card {{ idx }}",
    dweldat.rmbed_{{ idx }} AS "Bedrooms Card {{ idx }}",
    dweldat.fixbath_{{ idx }} AS "Full Baths Card {{ idx }}",
    dweldat.fixhalf_{{ idx }} AS "Half Baths Card {{ idx }}",
    dweldat.bsmt_{{ idx }} AS "Basement Card {{ idx }}",
    dweldat.bsmt_fin_{{ idx }} AS "Bsmt finish Card {{ idx }}",
    dweldat.external_propct_{{ idx }} AS "Proration Card {{ idx }}",
    dweldat.alt_cdu_{{ idx }} AS "Alt CDU Card {{ idx }}"{% if not loop.last %},{% endif %}
{% endfor %}
FROM {{ source('iasworld', 'pardat') }} AS pardat
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON pardat.parid = legdat.parid
    AND pardat.taxyr = legdat.taxyr
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'aprval') }} AS aprval
    ON pardat.parid = aprval.parid
    AND pardat.taxyr = aprval.taxyr
    AND aprval.cur = 'Y'
    AND aprval.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'aprval') }} AS aprval_prev
    ON aprval.parid = aprval_prev.parid
    AND CAST(aprval.taxyr AS INT) = CAST(aprval_prev.taxyr AS INT) + 1
    AND aprval_prev.cur = 'Y'
    AND aprval_prev.deactivat IS NULL
LEFT JOIN num_dwellings
    ON pardat.parid = num_dwellings.parid
    AND pardat.taxyr = num_dwellings.taxyr
LEFT JOIN first_five_dwellings_pivoted AS dweldat
    ON pardat.parid = dweldat.parid
    AND pardat.taxyr = dweldat.taxyr
-- Pull land SF from vw_pin_land rather than the source iasworld.land table
-- since SF aggregation is complicated
LEFT JOIN {{ ref('default.vw_pin_land') }} AS land
    ON pardat.parid = land.pin
    AND pardat.taxyr = land.year
LEFT JOIN {{ ref('qc.vw_iasworld_sales_latest_sale') }} AS sale
    ON pardat.parid = sale.parid
    -- Filter for only sales starting in 2021
    AND sale.saledt >= '2021-01-01'
WHERE pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
    -- Filter for only residential parcels
    AND SUBSTR(pardat.class, 1, 1) = '2'
