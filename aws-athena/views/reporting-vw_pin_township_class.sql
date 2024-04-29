-- This view provides common grouping columns used across many other reporting
-- views and CTAS.
WITH correct_class AS (
    SELECT
        parid,
        taxyr,
        REGEXP_REPLACE(class, '[^[:alnum:]]', '') AS class,
        nbhd,
        deactivat,
        cur
    FROM {{ source('iasworld', 'pardat') }}
)

SELECT
    correct.parid AS pin,
    correct.taxyr AS year,
    town.triad_name,
    town.triad_code,
    town.township_name,
    leg.user1 AS township_code,
    SUBSTR(correct.nbhd, 3, 3) AS nbhd,
    CASE WHEN
            UPPER(TRIM(leg.cityname)) IN (
                'ARLNGTN HTS', 'ARLNGTN HGTS', 'ARLNGTON HGT', 'ARLINGTN HTS'
            )
            THEN 'ARLINGTON HEIGHTS'
        WHEN UPPER(TRIM(leg.cityname)) = 'BERKLEY' THEN 'BERKELEY'
        WHEN
            UPPER(TRIM(leg.cityname)) IN (
                'ELK GR VILL', 'ELK GROVE VL', 'ELK GROVE VILLAGE'
            )
            THEN 'ELK GROVE'
        WHEN UPPER(TRIM(leg.cityname)) = 'HICKORY HLS' THEN 'HICKORY HILLS'
        WHEN UPPER(TRIM(leg.cityname)) = 'CHICAGO HTS' THEN 'CHICAGO HEIGHTS'
        WHEN UPPER(TRIM(leg.cityname)) = 'STONE PK' THEN 'STONE PARK'
        WHEN UPPER(TRIM(leg.cityname)) = 'CHGO' THEN 'CHICAGO'
        WHEN UPPER(TRIM(leg.cityname)) = 'HOFFMAN ESTS' THEN 'HOFFMAN ESTATES'
        WHEN UPPER(TRIM(leg.cityname)) = 'OLYMPIA FLDS' THEN 'OLYMPIA FIELDS'
        WHEN UPPER(TRIM(leg.cityname)) = 'ORLAND PK' THEN 'ORLAND PARK'
        WHEN UPPER(TRIM(leg.cityname)) = 'S. BARRINGTON' THEN 'SOUTH BARRINGTON'
        WHEN UPPER(TRIM(leg.cityname)) = 'SO HOLLAND' THEN 'SOUTH HOLLAND'
        WHEN
            UPPER(TRIM(leg.cityname)) IN ('WESTERN SPGS', 'WESTERN SPRG')
            THEN 'WESTERN SPRINGS'
        WHEN UPPER(TRIM(leg.cityname)) = 'BEDFORD PK' THEN 'BEDFORD PARK'
        WHEN UPPER(TRIM(leg.cityname)) = 'EVERGREEN PK' THEN 'EVERGREEN PARK'
        WHEN UPPER(TRIM(leg.cityname)) = 'LA GRANGE PK' THEN 'LA GRANGE PARK'
        WHEN UPPER(TRIM(leg.cityname)) = 'PALTINE' THEN 'PALATINE'
        WHEN UPPER(TRIM(leg.cityname)) = 'TINLEY PK' THEN 'TINLEY PARK'
    END AS municipality_name,
    correct.class,
    groups.reporting_class_code AS major_class,
    groups.modeling_group AS property_group,
    CASE
        WHEN
            MOD(CAST(correct.taxyr AS INT), 3) = 0
            AND town.triad_name = 'North'
            THEN TRUE
        WHEN
            MOD(CAST(correct.taxyr AS INT), 3) = 1
            AND town.triad_name = 'South'
            THEN TRUE
        WHEN
            MOD(CAST(correct.taxyr AS INT), 3) = 2
            AND town.triad_name = 'City'
            THEN TRUE
        ELSE FALSE
    END AS reassessment_year
FROM correct_class AS correct
LEFT JOIN {{ source('iasworld', 'legdat') }} AS leg
    ON correct.parid = leg.parid
    AND correct.taxyr = leg.taxyr
    AND leg.cur = 'Y'
    AND leg.deactivat IS NULL
LEFT JOIN {{ source('spatial', 'township') }} AS town
    ON leg.user1 = town.township_code
-- Exclude classes without a reporting class
INNER JOIN {{ ref('ccao.class_dict') }} AS groups
    ON correct.class = groups.class_code
WHERE correct.cur = 'Y'
    AND correct.deactivat IS NULL
