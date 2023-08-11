-- Gathers AVs by year, major class, assessment stage, and
-- township for reporting
WITH values_by_year AS (
    SELECT
        parid,
        taxyr,
        CASE
            WHEN procname = 'CCAOVALUE' THEN 'mailed'
            WHEN procname = 'CCAOFINAL' THEN 'assessor certified'
            WHEN procname = 'BORVALUE' THEN 'bor certified'
        END AS stage,
        MAX(
            CASE
                WHEN taxyr < '2020' THEN ovrvalasm2
                WHEN taxyr >= '2020' THEN valasm2
            END
        ) AS bldg,
        MAX(
            CASE
                WHEN taxyr < '2020' THEN ovrvalasm1
                WHEN taxyr >= '2020' THEN valasm1
            END
        ) AS land,
        MAX(
            CASE
                WHEN taxyr < '2020' THEN ovrvalasm3
                WHEN taxyr >= '2020' THEN valasm3
            END
        ) AS total
    FROM iasworld.asmt_all
    WHERE procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
        AND rolltype != 'RR'
        AND deactivat IS NULL
        AND valclass IS NULL
    GROUP BY parid, taxyr, procname
),

-- Add valuation class
classes AS (
    SELECT
        parid,
        taxyr,
        CASE
            WHEN class IN ('EX', 'RR') THEN class
            WHEN class IN (
                    '500', '535', '501', '516', '517', '522', '523',
                    '526', '527', '528', '529', '530', '531', '532',
                    '533', '535', '590', '591', '592', '597', '599'
                ) THEN '5A'
            WHEN class IN (
                    '550', '580', '581', '583', '587', '589', '593'
                ) THEN '5B'
            ELSE SUBSTR(class, 1, 1)
        END AS class
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
)

-- Add total and median values by township
SELECT
    values_by_year.taxyr AS year,
    values_by_year.stage,
    town_names.township_name,
    town_names.triad,
    classes.class,
    CASE
        WHEN
            MOD(CAST(values_by_year.taxyr AS INT), 3) = 0
            AND town_names.triad = 'North'
            THEN TRUE
        WHEN
            MOD(CAST(values_by_year.taxyr AS INT), 3) = 1
            AND town_names.triad = 'South'
            THEN TRUE
        WHEN
            MOD(CAST(values_by_year.taxyr AS INT), 3) = 2
            AND town_names.triad = 'City'
            THEN TRUE
        ELSE FALSE
    END AS reassessment_year,
    COUNT(*) AS n,
    SUM(values_by_year.bldg) AS bldg_sum,
    CAST(APPROX_PERCENTILE(values_by_year.bldg, 0.5) AS INT) AS bldg_median,
    SUM(values_by_year.land) AS land_sum,
    CAST(APPROX_PERCENTILE(values_by_year.land, 0.5) AS INT) AS land_median,
    SUM(values_by_year.total) AS tot_sum,
    CAST(APPROX_PERCENTILE(values_by_year.total, 0.5) AS INT) AS tot_median
FROM values_by_year
LEFT JOIN classes
    ON values_by_year.parid = classes.parid
    AND values_by_year.taxyr = classes.taxyr
LEFT JOIN townships
    ON values_by_year.parid = townships.parid
    AND values_by_year.taxyr = townships.taxyr
LEFT JOIN town_names
    ON townships.township_code = town_names.township_code
GROUP BY
    townships.township_code,
    town_names.township_name,
    values_by_year.taxyr,
    classes.class,
    town_names.triad,
    values_by_year.stage
ORDER BY
    town_names.township_name,
    values_by_year.taxyr,
    values_by_year.stage,
    classes.class
