--- A view to generate the top 5 parcels in a given township and year by AV
CREATE OR REPLACE VIEW reporting.vw_top_5
AS

WITH values_by_year AS (
    SELECT
        parid,
        taxyr,
        Max(CASE
                WHEN procname = 'CCAOVALUE'
                        AND taxyr < '2020' THEN ovrvalasm3
                WHEN procname = 'CCAOVALUE'
                        AND taxyr >= '2020'
                        AND valclass IS NULL THEN valasm3
                ELSE NULL
            END) AS mailed_tot,
        Max(CASE
                WHEN procname = 'CCAOFINAL'
                        AND taxyr < '2020' THEN ovrvalasm3
                WHEN procname = 'CCAOFINAL'
                        AND taxyr >= '2020'
                        AND valclass IS NULL THEN valasm3
                ELSE NULL
            END) AS certified_tot
    FROM iasworld.asmt_all

    WHERE (valclass IS null OR taxyr < '2020')

    GROUP BY parid, taxyr
        ),
    --- Choose most recent assessor value
    most_recent_values AS (
        SELECT
            parid,
            taxyr,
            CASE
                WHEN certified_tot IS NULL THEN mailed_tot ELSE certified_tot END AS total_av,
            CASE
                WHEN certified_tot IS NULL THEN 'mailed' ELSE 'certified' END AS stage_used
        FROM values_by_year
        WHERE certified_tot IS NOT NULL OR mailed_tot IS NOT NULL
    ),
    -- Add valuation class
    classes AS (
        SELECT
            parid,
            taxyr,
            class,
            NULLIF(CONCAT_WS(
                ' ',
                adrpre, CAST(adrno AS varchar),
                adrdir, adrstr, adrsuf,
                unitdesc, unitno
            ), '') AS "address",
            cityname AS city
        FROM iasworld.pardat
        ),
    -- Add townships
    townships AS (
        SELECT
            parid,
            taxyr,
            substr(TAXDIST, 1, 2) AS township_code
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
            most_recent_values.taxyr AS year,
            town_names.township_name as township,
            triad,
            classes.class,
            rank() over(
             PARTITION BY townships.township_code, most_recent_values.taxyr
             ORDER BY total_av DESC
             ) AS rank,
             most_recent_values.parid,
             total_av,
             "address",
             city,
             owner_name,
             stage_used
        FROM most_recent_values

        LEFT JOIN classes
            ON most_recent_values.parid = classes.parid
                AND most_recent_values.taxyr = classes.taxyr
        LEFT JOIN townships
            ON most_recent_values.parid = townships.parid
                AND most_recent_values.taxyr = townships.taxyr
        LEFT JOIN town_names
            ON townships.township_code = town_names.township_code
        LEFT JOIN taxpayers
            ON most_recent_values.parid = taxpayers.parid
                AND most_recent_values.taxyr = taxpayers.taxyr

        WHERE town_names.township_name IS NOT NULL
        )
-- Only keep top 5
SELECT
    *
FROM top_5
WHERE rank <= 5
ORDER BY township, year, class, rank