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
    pardat.parid,
    pardat.taxyr,
    legdat.user1 AS township_code,
    pardat.nbhd,
    pardat.class,
    legdat.adrno,
    legdat.adradd,
    legdat.adrdir,
    legdat.adrstr,
    legdat.adrsuf,
    legdat.adrsuf2,
    legdat.unitdesc,
    legdat.unitno,
    legdat.cityname,
    aprval.aprland,
    aprval.aprbldg,
    aprval.aprtot,
    aprval_prev.aprland AS aprland_prev,
    aprval_prev.aprbldg AS aprbldg_prev,
    aprval_prev.aprtot AS aprtot_prev,
    ROUND(
        (
            (aprval.aprtot - aprval_prev.aprtot)
            / CAST(aprval_prev.aprtot AS DOUBLE)
        ),
        2
    ) AS aprtot_percent_change,
    aprval.dwelval,
    aprval.dwelval + aprval.aprland AS dweltot,
    aprval_prev.dwelval AS dwelval_prev,
    aprval_prev.dwelval + aprval_prev.aprland AS dweltot_prev,
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
    ) AS dweltot_percent_change,
    sale.saledt_fmt,
    sale.price,
    sale.instruno,
    COALESCE(pardat.tiebldgpct, 0) AS tiebldgpct,
    aprval.reascd,
    aprval.obyval,
    land.sf,
    num_dwellings.num_dwellings,
{% for idx in range(1, 6) %}
    dweldat.yrblt_{{ idx }},
    dweldat.sfla_{{ idx }},
    dweldat.stories_{{ idx }},
    dweldat.rmbed_{{ idx }},
    dweldat.fixbath_{{ idx }},
    dweldat.fixhalf_{{ idx }},
    dweldat.bsmt_{{ idx }},
    dweldat.bsmt_fin_{{ idx }},
    dweldat.external_propct_{{ idx }},
    dweldat.alt_cdu_{{ idx }}{% if not loop.last %},{% endif %}
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
