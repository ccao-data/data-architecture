CREATE OR REPLACE VIEW reporting.vw_assessment_roll
AS

WITH values_by_year AS (
    SELECT
        parid,
        taxyr,
        -- Mailed values
        max(
            CASE
                WHEN procname = 'CCAOVALUE' AND taxyr < '2020' THEN ovrvalasm2
                WHEN procname = 'CCAOVALUE' AND taxyr >= '2020' THEN valasm2
                ELSE NULL END
            ) AS mailed_bldg,
        max(
            CASE
                WHEN procname = 'CCAOVALUE' AND taxyr < '2020' THEN ovrvalasm1
                WHEN procname = 'CCAOVALUE' AND taxyr >= '2020' THEN valasm1
                ELSE NULL END
            ) AS mailed_land,
        max(
            CASE
                WHEN procname = 'CCAOVALUE' AND taxyr < '2020' THEN ovrvalasm3
                WHEN procname = 'CCAOVALUE' AND taxyr >= '2020' THEN valasm3
                ELSE NULL END
            ) AS mailed_tot,
        -- Assessor certified values
        max(
            CASE
                WHEN procname = 'CCAOFINAL' AND taxyr < '2020' THEN ovrvalasm2
                WHEN procname = 'CCAOFINAL' AND taxyr >= '2020' THEN valasm2
                ELSE NULL END
            ) AS certified_bldg,
        max(
            CASE
                WHEN procname = 'CCAOFINAL' AND taxyr < '2020' THEN ovrvalasm1
                WHEN procname = 'CCAOFINAL' AND taxyr >= '2020' THEN valasm1
                ELSE NULL END
            ) AS certified_land,
        max(
            CASE
                WHEN procname = 'CCAOFINAL' AND taxyr < '2020' THEN ovrvalasm3
                WHEN procname = 'CCAOFINAL' AND taxyr >= '2020' THEN valasm3
                ELSE NULL END
            ) AS certified_tot,
        -- Board certified values
        max(
                CASE WHEN procname = 'BORVALUE' AND taxyr < '2020' THEN ovrvalasm2
                WHEN procname = 'BORVALUE' AND taxyr >= '2020' THEN valasm2
                ELSE NULL END
            ) AS board_bldg,
        max(
            CASE
                WHEN procname = 'BORVALUE' AND taxyr < '2020' THEN ovrvalasm1
                WHEN procname = 'BORVALUE' AND taxyr >= '2020' THEN valasm1
                ELSE NULL END
            ) AS board_lanD,
        max(
            CASE
                WHEN procname = 'BORVALUE' AND taxyr < '2020' THEN ovrvalasm3
                WHEN procname = 'BORVALUE' AND taxyr >= '2020' THEN valasm3
                ELSE NULL END
            ) AS board_tot
        FROM iasworld.asmt_all

        WHERE ((taxyr >= '2020' and valclass IS null) OR taxyr < '2020')
        GROUP BY parid, taxyr
        ORDER BY parid, taxyr
        ),
    -- Add township number and valuation class
    townships AS (
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
                ELSE substr(class, 1, 1) END AS class,
            substr(nbhd, 1, 2) AS township_code
        FROM   iasworld.pardat
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
    town_names.township_name,
    triad,
    CASE
        WHEN mod(cast(values_by_year.taxyr AS INT), 3) = 0 and triad = 'North' THEN TRUE
        WHEN mod(cast(values_by_year.taxyr AS INT), 3) = 1 and triad = 'South' THEN TRUE
        WHEN mod(cast(values_by_year.taxyr AS INT), 3) = 2 and triad = 'City' THEN TRUE
        ELSE FALSE END AS reassessment_year,
    townships.class,
    count(*) AS n,
    sum(mailed_bldg) AS mailed_bldg_sum,
    cast(approx_percentile(mailed_bldg, 0.5) AS INT) AS mailed_bldg_median,
    sum(mailed_land) AS mailed_land_sum,
    cast(approx_percentile(mailed_land, 0.5) AS INT) AS mailed_land_median,
    sum(mailed_tot) AS mailed_tot_sum,
    cast(approx_percentile(mailed_tot, 0.5) AS INT) AS mailed_tot_median,
    sum(certified_bldg) AS certified_bldg_sum,
    cast(approx_percentile(certified_bldg, 0.5) AS INT) AS certified_bldg_median,
    sum(certified_land) AS certified_land_sum,
    cast(approx_percentile(certified_land, 0.5) AS INT) AS certified_land_median,
    sum(certified_tot) AS certified_tot_sum,
    cast(approx_percentile(certified_tot, 0.5) AS INT) AS certified_tot_median,
    sum(board_bldg) AS board_bldg_sum,
    cast(approx_percentile(board_bldg, 0.5) AS INT) AS board_bldg_median,
    sum(board_land) AS board_land_sum,
    cast(approx_percentile(board_land, 0.5) AS INT) AS board_land_median,
    sum(board_tot) AS board_tot_sum,
    cast(approx_percentile(board_tot, 0.5) AS INT) AS board_tot_median
FROM values_by_year

LEFT JOIN townships
    ON values_by_year.parid = townships.parid
        AND values_by_year.taxyr = townships.taxyr
LEFT JOIN town_names
    ON townships.township_code = town_names.township_code

GROUP BY townships.township_code, town_names.township_name, values_by_year.taxyr, class, triad