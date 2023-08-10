--- A view to generate the top 5 parcels in a given township and year by AV
{{ config(materialized='view') }}

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
    FROM default.vw_pin_value
    WHERE certified_tot IS NOT NULL
        OR mailed_tot IS NOT NULL
),

-- Add valuation class
classes AS (
    SELECT
        parid,
        taxyr,
        class,
        NULLIF(CONCAT_WS(
            ' ',
            adrpre, CAST(adrno AS VARCHAR),
            adrdir, adrstr, adrsuf,
            unitdesc, unitno
        ), '') AS address,
        cityname AS city
    FROM iasworld.pardat
),

-- Add townships
townships AS (
    SELECT
        parid,
        taxyr,
        user1 AS township_code
    FROM iasworld.legdat
),

-- Add township name
town_names AS (
    SELECT
        triad_name AS triad,
        township_name,
        township_code
    FROM spatial.township
),

--- Mailing name from owndat
taxpayers AS (
    SELECT
        parid,
        taxyr,
        NULLIF(CONCAT_WS(
            ' ',
            own1, own2
        ), '') AS owner_name
    FROM iasworld.owndat
),

-- Create ranks
top_5 AS (
    SELECT
        mrv.taxyr AS year,
        town_names.township_name AS township,
        town_names.triad,
        classes.class,
        RANK() OVER (
            PARTITION BY townships.township_code, mrv.taxyr
            ORDER BY mrv.total_av DESC
        ) AS rank,
        mrv.parid,
        mrv.total_av,
        classes.address,
        classes.city,
        taxpayers.owner_name,
        mrv.stage_used
    FROM most_recent_values AS mrv
    LEFT JOIN classes
        ON mrv.parid = classes.parid
        AND mrv.taxyr = classes.taxyr
    LEFT JOIN townships
        ON mrv.parid = townships.parid
        AND mrv.taxyr = townships.taxyr
    LEFT JOIN town_names
        ON townships.township_code = town_names.township_code
    LEFT JOIN taxpayers
        ON mrv.parid = taxpayers.parid
        AND mrv.taxyr = taxpayers.taxyr
    WHERE town_names.township_name IS NOT NULL
)

-- Only keep top 5
SELECT *
FROM top_5
WHERE rank <= 5
ORDER BY township, year, class, rank
