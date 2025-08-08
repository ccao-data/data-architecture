{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['pin10'],
        bucket_count=1
    )
}}

WITH long AS (
    SELECT
        pcl.pin10,
        pcl.year,
        pcl.tax_code,
        tc.agency_num,
        ai.agency_name,
        ai.major_type,
        ai.minor_type,
        tc.year AS tax_data_year
    FROM {{ source('spatial', 'parcel') }} AS pcl
    INNER JOIN {{ source('tax', 'tax_code') }} AS tc
        ON pcl.tax_code = tc.tax_code_num
        AND pcl.year = tc.year
    LEFT JOIN {{ source('tax', 'agency_info') }} AS ai
        ON tc.agency_num = ai.agency_num
    WHERE ai.minor_type IN (
            'MUNI', 'ELEMENTARY', 'SECONDARY', 'UNIFIED', 'COMM COLL',
            'FIRE', 'LIBRARY', 'PARK', 'SANITARY', 'SSA', 'TIF', 'TOWNSHIP'
        )
),

wide AS (
    SELECT
        pin10,
        -- Cicero is both a municipality and a township, so we include it in the
        -- municipality fields.
        FILTER(
            ARRAY_AGG(
                CASE
                    WHEN
                        minor_type = 'MUNI'
                        OR (
                            minor_type = 'TOWNSHIP'
                            AND agency_name = 'TOWN CICERO'
                        )
                        THEN agency_num
                END
            ),
            x -> x IS NOT NULL
        ) AS tax_municipality_num,
        FILTER(
            ARRAY_AGG(
                CASE
                    WHEN
                        minor_type = 'MUNI'
                        THEN agency_name
                    WHEN minor_type = 'TOWNSHIP' AND agency_name = 'TOWN CICERO'
                        THEN 'TOWN OF CICERO'
                END
            ),
            x -> x IS NOT NULL
        ) AS tax_municipality_name,
        FILTER(
            ARRAY_AGG(
                CASE
                    WHEN minor_type = 'ELEMENTARY' THEN agency_num
                END
            ),
            x -> x IS NOT NULL
        ) AS tax_school_elementary_district_num,
        FILTER(
            ARRAY_AGG(
                CASE
                    WHEN minor_type = 'ELEMENTARY' THEN agency_name
                END
            ),
            x -> x IS NOT NULL
        ) AS tax_school_elementary_district_name,
        FILTER(
            ARRAY_AGG(
                CASE
                    WHEN minor_type = 'SECONDARY' THEN agency_num
                END
            ),
            x -> x IS NOT NULL
        ) AS tax_school_secondary_district_num,
        FILTER(
            ARRAY_AGG(
                CASE
                    WHEN minor_type = 'SECONDARY' THEN agency_name
                END
            ),
            x -> x IS NOT NULL
        ) AS tax_school_secondary_district_name,
        FILTER(
            ARRAY_AGG(
                CASE
                    WHEN minor_type = 'UNIFIED' THEN agency_num
                END
            ),
            x -> x IS NOT NULL
        ) AS tax_school_unified_district_num,
        FILTER(
            ARRAY_AGG(
                CASE
                    WHEN minor_type = 'UNIFIED' THEN agency_name
                END
            ),
            x -> x IS NOT NULL
        ) AS tax_school_unified_district_name,
        FILTER(
            ARRAY_AGG(
                CASE
                    WHEN minor_type = 'COMM COLL' THEN agency_num
                END
            ),
            x -> x IS NOT NULL
        ) AS tax_community_college_district_num,
        FILTER(
            ARRAY_AGG(
                CASE
                    WHEN minor_type = 'COMM COLL' THEN agency_name
                END
            ),
            x -> x IS NOT NULL
        ) AS tax_community_college_district_name,
        FILTER(
            ARRAY_AGG(
                CASE WHEN minor_type = 'FIRE' THEN agency_num END
            ),
            x -> x IS NOT NULL
        ) AS tax_fire_protection_district_num,
        FILTER(
            ARRAY_AGG(
                CASE WHEN minor_type = 'FIRE' THEN agency_name END
            ),
            x -> x IS NOT NULL
        ) AS tax_fire_protection_district_name,
        FILTER(
            ARRAY_AGG(
                CASE
                    WHEN minor_type = 'LIBRARY' THEN agency_num
                END
            ),
            x -> x IS NOT NULL
        ) AS tax_library_district_num,
        FILTER(
            ARRAY_AGG(
                CASE
                    WHEN minor_type = 'LIBRARY' THEN agency_name
                END
            ),
            x -> x IS NOT NULL
        ) AS tax_library_district_name,
        FILTER(
            ARRAY_AGG(
                CASE WHEN minor_type = 'PARK' THEN agency_num END
            ),
            x -> x IS NOT NULL
        ) AS tax_park_district_num,
        FILTER(
            ARRAY_AGG(
                CASE WHEN minor_type = 'PARK' THEN agency_name END
            ),
            x -> x IS NOT NULL
        ) AS tax_park_district_name,
        FILTER(
            ARRAY_AGG(
                CASE
                    WHEN minor_type = 'SANITARY' THEN agency_num
                END
            ),
            x -> x IS NOT NULL
        ) AS tax_sanitation_district_num,
        FILTER(
            ARRAY_AGG(
                CASE
                    WHEN minor_type = 'SANITARY' THEN agency_name
                END
            ),
            x -> x IS NOT NULL
        ) AS tax_sanitation_district_name,
        FILTER(
            ARRAY_AGG(
                CASE WHEN minor_type = 'SSA' THEN agency_num END
            ),
            x -> x IS NOT NULL
        ) AS tax_special_service_area_num,
        FILTER(
            ARRAY_AGG(
                CASE WHEN minor_type = 'SSA' THEN agency_name END
            ),
            x -> x IS NOT NULL
        ) AS tax_special_service_area_name,
        FILTER(
            ARRAY_AGG(
                CASE WHEN minor_type = 'TIF' THEN agency_num END
            ),
            x -> x IS NOT NULL
        ) AS tax_tif_district_num,
        FILTER(
            ARRAY_AGG(
                CASE WHEN minor_type = 'TIF' THEN agency_name END
            ),
            x -> x IS NOT NULL
        ) AS tax_tif_district_name,
        tax_data_year,
        year
    FROM long
    GROUP BY pin10, year, tax_data_year
)

SELECT
    pcl.pin10,
    wide.tax_municipality_num,
    wide.tax_municipality_name,
    wide.tax_school_elementary_district_num,
    wide.tax_school_elementary_district_name,
    wide.tax_school_secondary_district_num,
    wide.tax_school_secondary_district_name,
    wide.tax_school_unified_district_num,
    wide.tax_school_unified_district_name,
    wide.tax_community_college_district_num,
    wide.tax_community_college_district_name,
    wide.tax_fire_protection_district_num,
    wide.tax_fire_protection_district_name,
    wide.tax_library_district_num,
    wide.tax_library_district_name,
    wide.tax_park_district_num,
    wide.tax_park_district_name,
    wide.tax_sanitation_district_num,
    wide.tax_sanitation_district_name,
    wide.tax_special_service_area_num,
    wide.tax_special_service_area_name,
    wide.tax_tif_district_num,
    wide.tax_tif_district_name,
    wide.tax_data_year,
    pcl.year
FROM {{ source('spatial', 'parcel') }} AS pcl
LEFT JOIN wide
    ON pcl.pin10 = wide.pin10
    -- Join syntax here forward fills with most recent non-null value.
    AND (
        CASE WHEN pcl.year > (SELECT MAX(year) FROM wide)
                THEN (SELECT MAX(year) FROM wide)
            ELSE pcl.year
        END = wide.year
    )
WHERE pcl.year >= (SELECT MIN(year) FROM wide)
