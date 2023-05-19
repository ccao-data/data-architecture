CREATE TABLE IF NOT EXISTS location.tax
WITH (
    format='Parquet',
    write_compression = 'SNAPPY',
    external_location='s3://ccao-athena-ctas-us-east-1/location/tax',
    partitioned_by = ARRAY['year'],
    bucketed_by = ARRAY['pin10'],
    bucket_count = 1
) AS (
    WITH long AS (
    SELECT
        p.pin10,
        p.year,
        p.tax_code,
        tc.agency_num,
        ai.agency_name,
        ai.major_type,
        ai.minor_type,
        tc.year AS tax_data_year
    FROM spatial.parcel p
    INNER JOIN tax.tax_code tc
        ON p.tax_code = tc.tax_code_num
        AND p.year = tc.year
    LEFT JOIN tax.agency_info ai
        ON tc.agency_num = ai.agency_num
    WHERE minor_type IN (
    'MUNI',
    'ELEMENTARY',
    'SECONDARY',
    'UNIFIED',
    'COMM COLL',
    'FIRE',
    'LIBRARY',
    'PARK',
    'SANITARY',
    'SSA',
    'TIF'
    )
  ),
  wide AS (
      SELECT
          pin10,
          filter(
              array_agg(CASE WHEN minor_type = 'MUNI' THEN agency_num ELSE NULL END),
              x -> x IS NOT NULL
              ) AS tax_cook_municipality_num,
          filter(
              array_agg(CASE WHEN minor_type = 'MUNI' THEN agency_name ELSE NULL END),
              x -> x IS NOT NULL
              ) AS tax_cook_municipality_name,
          filter(
              array_agg(CASE WHEN minor_type = 'ELEMENTARY' THEN agency_num ELSE NULL END),
              x -> x IS NOT NULL
              ) AS tax_school_elementary_district_num,
          filter(
              array_agg(CASE WHEN minor_type = 'ELEMENTARY' THEN agency_name ELSE NULL END),
              x -> x IS NOT NULL
              ) AS tax_school_elementary_district_name,
          filter(
              array_agg(CASE WHEN minor_type = 'SECONDARY' THEN agency_num ELSE NULL END),
              x -> x IS NOT NULL
              ) AS tax_school_secondary_district_num,
          filter(
              array_agg(CASE WHEN minor_type = 'SECONDARY' THEN agency_name ELSE NULL END),
              x -> x IS NOT NULL
              ) AS tax_school_secondary_district_name,
          filter(
              array_agg(CASE WHEN minor_type = 'UNIFIED' THEN agency_num ELSE NULL END),
              x -> x IS NOT NULL
              ) AS tax_school_unified_district_num,
          filter(
              array_agg(CASE WHEN minor_type = 'UNIFIED' THEN agency_name ELSE NULL END),
              x -> x IS NOT NULL
              ) AS tax_school_unified_district_name,
          filter(
              array_agg(CASE WHEN minor_type = 'COMM COLL' THEN agency_num ELSE NULL END),
              x -> x IS NOT NULL
              ) AS tax_community_college_district_num,
          filter(
              array_agg(CASE WHEN minor_type = 'COMM COLL' THEN agency_name ELSE NULL END),
              x -> x IS NOT NULL
              ) AS tax_community_college_district_name,
          filter(
              array_agg(CASE WHEN minor_type = 'FIRE' THEN agency_num ELSE NULL END),
              x -> x IS NOT NULL
              ) AS tax_fire_protection_district_num,
          filter(
              array_agg(CASE WHEN minor_type = 'FIRE' THEN agency_name ELSE NULL END),
              x -> x IS NOT NULL
              ) AS tax_fire_protection_district_name,
          filter(
              array_agg(CASE WHEN minor_type = 'LIBRARY' THEN agency_num ELSE NULL END),
              x -> x IS NOT NULL
              ) AS tax_library_district_num,
          filter(
              array_agg(CASE WHEN minor_type = 'LIBRARY' THEN agency_name ELSE NULL END),
              x -> x IS NOT NULL
              ) AS tax_library_district_name,
          filter(
              array_agg(CASE WHEN minor_type = 'PARK' THEN agency_num ELSE NULL END),
              x -> x IS NOT NULL
              ) AS tax_park_district_num,
          filter(
              array_agg(CASE WHEN minor_type = 'PARK' THEN agency_name ELSE NULL END),
              x -> x IS NOT NULL
              ) AS tax_park_district_name,
          filter(
              array_agg(CASE WHEN minor_type = 'SANITARY' THEN agency_num ELSE NULL END),
              x -> x IS NOT NULL
              ) AS tax_sanitation_district_num,
          filter(
              array_agg(CASE WHEN minor_type = 'SANITARY' THEN agency_name ELSE NULL END),
              x -> x IS NOT NULL
              ) AS tax_sanitation_district_name,
          filter(
              array_agg(CASE WHEN minor_type = 'SSA' THEN agency_num ELSE NULL END),
              x -> x IS NOT NULL
              ) AS tax_special_service_area_num,
          filter(
              array_agg(CASE WHEN minor_type = 'SSA' THEN agency_name ELSE NULL END),
              x -> x IS NOT NULL
              ) AS tax_special_service_area_name,
          filter(
              array_agg(CASE WHEN minor_type = 'TIF' THEN agency_num ELSE NULL END),
              x -> x IS NOT NULL
              ) AS tax_tif_district_num,
          filter(
              array_agg(CASE WHEN minor_type = 'TIF' THEN agency_name ELSE NULL END),
              x -> x IS NOT NULL
              ) AS tax_tif_district_name,
          tax_data_year, year
      FROM long
      GROUP BY pin10, year, tax_data_year
  )

  SELECT
      p.pin10,
      wide.tax_cook_municipality_num,
      wide.tax_cook_municipality_name,
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
      p.year
  FROM spatial.parcel p
  LEFT JOIN wide
      ON p.pin10 = wide.pin10
      -- Join syntax here forward fills with most recent non-null value.
      AND (
          CASE WHEN p.year > (SELECT Max(year) FROM wide)
              THEN (SELECT Max(year) FROM wide)
              ELSE p.year END = wide.year
                  )
)