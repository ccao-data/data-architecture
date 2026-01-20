-- View that always contains the most recent political boundaries the
-- Data Department has ingested, by 10-digit parcel

WITH parcel AS (
    SELECT
        parcel.pin10,
        parcel.town_code AS township_code,
        parcel.year AS parcel_year,
        ST_POINT(parcel.x_3435, parcel.y_3435) AS geom
    FROM {{ source('spatial', 'parcel') }} AS parcel
    WHERE parcel.year
        = (
            SELECT MAX(parcel.year)
            FROM {{ source('spatial', 'parcel') }} AS parcel
        )
        AND parcel.town_code IS NOT NULL
),

municipality AS (
    SELECT
        municipality.municipality_name,
        municipality.municipality_num,
        municipality.year AS municipality_year,
        ST_GEOMFROMBINARY(municipality.geometry_3435) AS geom
    FROM {{ source('spatial', 'municipality') }} AS municipality
    WHERE municipality.year
        = (
            SELECT MAX(municipality.year)
            FROM {{ source('spatial', 'municipality') }} AS municipality
        )
),

ward_chicago AS (
    SELECT
        ward_chicago.ward_name AS ward_chicago_name,
        ward_chicago.ward_num AS ward_chicago_num,
        ward_chicago.year AS ward_chicago_year,
        ST_GEOMFROMBINARY(ward_chicago.geometry_3435) AS geom
    FROM {{ source('spatial', 'ward') }} AS ward_chicago
    WHERE ward_chicago.ward_name LIKE '%chicago%'
        AND ward_chicago.year
        = (
            SELECT MAX(ward_chicago.year)
            FROM {{ source('spatial', 'ward') }} AS ward_chicago
            WHERE ward_chicago.ward_name LIKE '%chicago%'
        )
),

ward_evanston AS (
    SELECT
        ward_evanston.ward_name AS ward_evanston_name,
        ward_evanston.ward_num AS ward_evanston_num,
        ward_evanston.year AS ward_evanston_year,
        ST_GEOMFROMBINARY(ward_evanston.geometry_3435) AS geom
    FROM {{ source('spatial', 'ward') }} AS ward_evanston
    WHERE ward_evanston.ward_name LIKE '%evanston%'
        AND ward_evanston.year
        = (
            SELECT MAX(ward_evanston.year)
            FROM {{ source('spatial', 'ward') }} AS ward_evanston
            WHERE ward_evanston.ward_name LIKE '%evanston%'
        )
),

community_area AS (
    SELECT
        community_area.community AS community_area_name,
        community_area.area_number AS community_area_num,
        community_area.year AS community_area_year,
        ST_GEOMFROMBINARY(community_area.geometry_3435) AS geom
    FROM {{ source('spatial', 'community_area') }} AS community_area
    WHERE community_area.year
        = (
            SELECT MAX(community_area.year)
            FROM {{ source('spatial', 'community_area') }} AS community_area
        )
),

commissioner_district AS (
    SELECT
        commissioner_district.commissioner_district_name,
        commissioner_district.commissioner_district_num,
        commissioner_district.year AS commissioner_district_year,
        ST_GEOMFROMBINARY(commissioner_district.geometry_3435) AS geom
    FROM
        {{ source('spatial', 'commissioner_district') }}
            AS commissioner_district
    WHERE commissioner_district.year
        = (
            SELECT MAX(commissioner_district.year)
            FROM
                {{ source('spatial', 'commissioner_district') }}
                    AS commissioner_district
        )
),

state_representative_district AS (
    SELECT
        state_representative_district.state_representative_district_name,
        state_representative_district.state_representative_district_num,
        state_representative_district.year
            AS state_representative_district_year,
        ST_GEOMFROMBINARY(state_representative_district.geometry_3435) AS geom
    FROM
        {{ source('spatial', 'state_representative_district') }}
            AS state_representative_district
    WHERE state_representative_district.year
        = (
            SELECT MAX(state_representative_district.year)
            FROM
                {{ source('spatial', 'state_representative_district') }}
                    AS state_representative_district
        )
),

state_senate_district AS (
    SELECT
        state_senate_district.state_senate_district_name,
        state_senate_district.state_senate_district_num,
        state_senate_district.year AS state_senate_district_year,
        ST_GEOMFROMBINARY(state_senate_district.geometry_3435) AS geom
    FROM
        {{ source('spatial', 'state_senate_district') }}
            AS state_senate_district
    WHERE state_senate_district.year
        = (
            SELECT MAX(state_senate_district.year)
            FROM
                {{ source('spatial', 'state_senate_district') }}
                    AS state_senate_district
        )
)

SELECT
    parcel.pin10,
    parcel.township_code,
    parcel.parcel_year,
    municipality.municipality_name,
    municipality.municipality_num,
    municipality.municipality_year,
    ward_chicago.ward_chicago_name,
    ward_chicago.ward_chicago_num,
    ward_chicago.ward_chicago_year,
    ward_evanston.ward_evanston_name,
    ward_evanston.ward_evanston_num,
    ward_evanston.ward_evanston_year,
    community_area.community_area_name,
    community_area.community_area_num,
    community_area.community_area_year,
    commissioner_district.commissioner_district_name,
    commissioner_district.commissioner_district_num,
    commissioner_district.commissioner_district_year,
    state_representative_district.state_representative_district_name,
    state_representative_district.state_representative_district_num,
    state_representative_district.state_representative_district_year,
    state_senate_district.state_senate_district_name,
    state_senate_district.state_senate_district_num,
    state_senate_district.state_senate_district_year
FROM parcel
LEFT JOIN municipality ON ST_WITHIN(parcel.geom, municipality.geom)
LEFT JOIN ward_chicago ON ST_WITHIN(parcel.geom, ward_chicago.geom)
LEFT JOIN ward_evanston ON ST_WITHIN(parcel.geom, ward_evanston.geom)
LEFT JOIN community_area ON ST_WITHIN(parcel.geom, community_area.geom)
LEFT JOIN
    commissioner_district
    ON ST_WITHIN(parcel.geom, commissioner_district.geom)
LEFT JOIN
    state_representative_district
    ON ST_WITHIN(parcel.geom, state_representative_district.geom)
LEFT JOIN
    state_senate_district
    ON ST_WITHIN(parcel.geom, state_senate_district.geom)
