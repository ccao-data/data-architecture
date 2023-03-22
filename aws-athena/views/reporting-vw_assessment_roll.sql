-- Gathers AVs by year, major class, assessment stage, and township for reporting
CREATE OR REPLACE VIEW reporting.vw_assessment_roll
AS

WITH values_by_year AS (
    SELECT
        parid,
        taxyr,
        CASE
            WHEN procname = 'CCAOVALUE' THEN 'mailed'
            WHEN procname = 'CCAOFINAL' THEN 'assessor certified'
            WHEN procname = 'BORVALUE'  THEN 'bor certified'
            ELSE NULL END AS stage,
        max(
            CASE
                WHEN taxyr < '2020' THEN ovrvalasm2
                WHEN taxyr >= '2020' THEN valasm2
                ELSE NULL END
            ) AS bldg,
        max(
            CASE
                WHEN taxyr < '2020' THEN ovrvalasm1
                WHEN taxyr >= '2020' THEN valasm1
                ELSE NULL END
            ) AS land,
        max(
            CASE
                WHEN taxyr < '2020' THEN ovrvalasm3
                WHEN taxyr >= '2020' THEN valasm3
                ELSE NULL END
            ) AS total
    FROM iasworld.asmt_all

    WHERE (valclass IS null OR taxyr < '2020')
    AND procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')

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
                ELSE substr(class, 1, 1) END AS class
        FROM   iasworld.pardat
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
        )
-- Add total and median values by township
SELECT
    values_by_year.taxyr AS year,
    stage,
    town_names.township_name,
    triad,
    classes.class,
    CASE
        WHEN mod(cast(values_by_year.taxyr AS INT), 3) = 0 and triad = 'North' THEN TRUE
        WHEN mod(cast(values_by_year.taxyr AS INT), 3) = 1 and triad = 'South' THEN TRUE
        WHEN mod(cast(values_by_year.taxyr AS INT), 3) = 2 and triad = 'City' THEN TRUE
        ELSE FALSE END AS reassessment_year,
    count(*) AS n,
    sum(bldg) AS bldg_sum,
    cast(approx_percentile(bldg, 0.5) AS INT) AS bldg_median,
    sum(land) AS land_sum,
    cast(approx_percentile(land, 0.5) AS INT) AS land_median,
    sum(total) AS tot_sum,
    cast(approx_percentile(total, 0.5) AS INT) AS tot_median
FROM values_by_year

LEFT JOIN classes
    ON values_by_year.parid = classes.parid
        AND values_by_year.taxyr = classes.taxyr
LEFT JOIN townships
    ON values_by_year.parid = townships.parid
        AND values_by_year.taxyr = townships.taxyr
LEFT JOIN town_names
    ON townships.township_code = town_names.township_code

GROUP BY townships.township_code, town_names.township_name, values_by_year.taxyr, class, triad, stage
ORDER BY town_names.township_name, values_by_year.taxyr, stage, class