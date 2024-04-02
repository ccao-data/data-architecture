--- A view to generate the top 5 parcels in a given township and year by AV

--- Choose most recent assessor value
WITH most_recent_values AS (
    SELECT
        pin AS parid,
        year AS taxyr,
        COALESCE(certified_tot, mailed_tot) AS total_av,
        CASE
            WHEN certified_tot IS NULL THEN 'mailed'
            ELSE 'certified'
        END AS stage_used
    FROM {{ ref('default.vw_pin_value') }}
    WHERE certified_tot IS NOT NULL
        OR mailed_tot IS NOT NULL
),

-- Add townships, valuation classes, mailing names
townships AS (
    SELECT
        legdat.parid AS pin,
        legdat.taxyr AS year,
        REGEXP_REPLACE(pardat.class, '([^0-9EXR])', '') AS class,
        township.triad_name AS triad,
        township.township_name,
        NULLIF(CONCAT_WS(
            ' ',
            owndat.own1, owndat.own2
        ), '') AS owner_name,
        NULLIF(CONCAT_WS(
            ' ',
            pardat.adrpre, CAST(pardat.adrno AS VARCHAR),
            pardat.adrdir, pardat.adrstr, pardat.adrsuf,
            pardat.unitdesc, pardat.unitno
        ), '') AS address,
        pardat.cityname AS city
    FROM {{ source('iasworld', 'legdat') }} AS legdat
    LEFT JOIN {{ source('spatial', 'township') }} AS township
        ON legdat.user1 = township.township_code
    LEFT JOIN {{ source('iasworld', 'pardat') }} AS pardat
        ON legdat.parid = pardat.parid
        AND legdat.taxyr = pardat.taxyr
    LEFT JOIN {{ source('iasworld', 'owndat') }} AS owndat
        ON legdat.parid = owndat.parid
        AND legdat.taxyr = owndat.taxyr
    WHERE legdat.cur = 'Y'
        AND pardat.deactivat IS NULL
        AND pardat.cur = 'Y'
        AND owndat.deactivat IS NULL
        AND owndat.cur = 'Y'
),

-- Create ranks
top_5 AS (
    SELECT
        mrv.taxyr AS year,
        townships.township_name AS township,
        townships.triad,
        townships.class,
        RANK() OVER (
            PARTITION BY townships.township_code, mrv.taxyr
            ORDER BY mrv.total_av DESC
        ) AS rank,
        mrv.parid,
        mrv.total_av,
        townships.address,
        townships.city,
        townships.owner_name,
        mrv.stage_used
    FROM most_recent_values AS mrv
    LEFT JOIN townships
        ON mrv.parid = townships.pin
        AND mrv.taxyr = townships.year
    WHERE townships.township_name IS NOT NULL
)

-- Only keep top 5
SELECT *
FROM top_5
WHERE rank <= 5
ORDER BY township, year, class, rank
