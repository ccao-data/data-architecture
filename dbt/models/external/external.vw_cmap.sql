/* For this query we keep every table unique to pin/year except
dweldat, since we want card-level characteristics for residential parcels. So
CTEs will aggregate oby and comdat to pin/year level. Other tables that don't
have CTEs are already unique by pin/year. We assume yrblt should be maxed
within PIN for this request.

The basic idea here is that we're using asmt_all as the universe for parcels and
joining on residential characteristics from dweldat, condo characteristics from
oby, commercial characteristics from comdat, and land sf from our land view. We
also join on appeals and exemptions. */

-- This is our universe of pins. We're using BOR values since CMAP wants final
-- values and select arbitrary rows within PIN/year since there are unfixable
-- duplicates in asmt_all.
WITH asmt AS (
    SELECT
        parid,
        taxyr,
        ARBITRARY(class) AS class,
        ARBITRARY(valasm1) AS land,
        ARBITRARY(valasm2) AS bldg,
        ARBITRARY(valasm3) AS tot
    FROM {{ source("iasworld", "asmt_all") }}
    WHERE deactivat IS NULL
        AND procname = 'BORVALUE'
        AND valclass IS NULL
        -- Class 999 are test pins
        AND class NOT IN ('999')

    GROUP BY
        parid,
        taxyr
),

-- Aggregate commercial characteristics to pin/year level
com AS (
    SELECT
        parid,
        taxyr,
        COUNT(*) AS multi_imp_num,
        -- Assuming gross building area should be summed within PIN
        SUM(CAST(user20 AS DOUBLE)) AS gross_building_area,
        -- Assuming net rentable area should be summed within PIN
        SUM(CAST(user28 AS DOUBLE)) AS net_rentable_area,
        MAX(yrblt) AS yrblt
    FROM {{ source("iasworld", "comdat") }}
    WHERE deactivat IS NULL
        AND cur = 'Y'
    GROUP BY
        parid,
        taxyr
),

-- Aggregate condo characteristics to pin/year level
oby AS (
    SELECT
        parid,
        taxyr,
        COUNT(*) AS multi_imp_num,
        MAX(yrblt) AS yrblt
    FROM {{ source("iasworld", "oby") }}
    WHERE deactivat IS NULL
        AND cur = 'Y'
    GROUP BY
        parid,
        taxyr
),

-- Card-level residential characteristics
dwel AS (
    SELECT
        pin AS parid,
        year AS taxyr,
        card,
        pin_num_cards AS multi_imp_num,
        char_bldg_sf AS building_sf,
        char_beds AS beds,
        char_rooms AS rooms,
        char_fbath AS full_bath,
        char_hbath AS half_bath,
        CASE WHEN char_frpl = 1 THEN 'Full'
            WHEN char_frpl = 2 THEN 'Partial'
            WHEN char_frpl = 3 THEN 'None'
        END AS fireplace,
        char_attic_type AS attic,
        CASE WHEN char_bsmt = '1' THEN 'Full'
            WHEN char_bsmt = '2' THEN 'Slab'
            WHEN char_bsmt = '3' THEN 'Partial'
            WHEN char_bsmt = '4' THEN 'Crawl'
        END AS basement,
        CASE WHEN char_ext_wall = '1' THEN 'Frame'
            WHEN char_ext_wall = '2' THEN 'Masonry'
            WHEN char_ext_wall = '3' THEN 'Frame + Masonry'
            WHEN char_ext_wall = '4' THEN 'Stucco'
        END AS wall_construction,
        CASE WHEN char_roof_cnst = '1' THEN 'Shingle + Asphalt'
            WHEN char_roof_cnst = '2' THEN 'Tar + Gravel'
            WHEN char_roof_cnst = '3' THEN 'Slate'
            WHEN char_roof_cnst = '4' THEN 'Shake'
            WHEN char_roof_cnst = '5' THEN 'Tile'
            WHEN char_roof_cnst = '6' THEN 'Other'
        END AS roof_construction,
        char_ncu AS num_apts,
        char_yrblt AS yrblt
    FROM {{ ref('default.vw_card_res_char') }}
),

appeals AS (
    SELECT
        pin,
        year
    FROM {{ ref('default.vw_pin_appeal') }}
    GROUP BY pin, year
)

SELECT
    -- keys
    asmt.parid AS pin,
    dwel.card,
    COALESCE(dwel.multi_imp_num, com.multi_imp_num, oby.multi_imp_num)
        AS multi_imp_num,
    asmt.taxyr AS year,

    -- address
    vpa.prop_address_full,
    vpa.prop_address_city_name,
    vpa.prop_address_zipcode_1,
    vpa.mail_address_name,
    vpa.mail_address_full,
    vpa.mail_address_city_name,
    vpa.mail_address_state,
    vpa.mail_address_zipcode_1,

    -- class
    asmt.class AS class_code,
    class_dict.class_desc AS class_description,

    -- town, muni, tax code
    vpu.township_name AS township,
    vpu.nbhd_code AS neighborhood,
    -- Combined municipality name is an array. Empty arrays indicate
    -- unincorporated parcels. Null values indicate missing data.
    CASE
        WHEN CARDINALITY(vpu.combined_municipality_name) = 0
            THEN 'UNINCORPORATED'
        ELSE ARRAY_JOIN(vpu.combined_municipality_name, ', ')
    END AS municipality,
    vpu.tax_code,

    -- av
    asmt.land AS land_av,
    asmt.bldg AS building_av,
    asmt.tot AS total_av,
    vptr.final_eav AS equalized_total_av,

    -- exempt status
    exempt.owner_name AS exempt_agency_name,
    exempt.owner_num AS exempt_agency_num,

    -- exemptions
    vptr.exe_disabled,
    vptr.exe_freeze,
    vptr.exe_homeowner,
    vptr.exe_longtime_homeowner,
    vptr.exe_senior,
    vptr.exe_muni_built,
    vptr.exe_vet_dis_lt50,
    vptr.exe_vet_dis_50_69,
    vptr.exe_vet_dis_ge70,
    vptr.exe_vet_dis_100,
    vptr.exe_vet_returning,
    vptr.exe_wwii,

    -- characteristics
    land.sf AS land_sf,
    COALESCE(dwel.yrblt, com.yrblt, oby.yrblt) AS year_built,
    CAST(asmt.taxyr AS INT) - COALESCE(dwel.yrblt, com.yrblt, oby.yrblt) AS age,

    com.gross_building_area,
    com.net_rentable_area,

    dwel.building_sf,
    dwel.beds,
    dwel.rooms,
    dwel.full_bath,
    dwel.half_bath,
    dwel.fireplace,
    dwel.attic,
    dwel.basement,
    dwel.wall_construction,
    dwel.roof_construction,
    dwel.num_apts,
    -- appeals
    CASE WHEN appeals.pin IS NOT NULL THEN 'Yes' ELSE 'No' END AS appealed
FROM asmt
LEFT JOIN default.vw_pin_address AS vpa
    ON asmt.parid = vpa.pin
    AND asmt.taxyr = vpa.year
LEFT JOIN ccao.class_dict -- Join for class descriptions
    ON asmt.class = class_dict.class_code
LEFT JOIN default.vw_pin_universe AS vpu
    ON asmt.parid = vpu.pin
    AND asmt.taxyr = vpu.year
LEFT JOIN default.vw_pin_exempt AS exempt
    ON asmt.parid = exempt.pin
    AND asmt.taxyr = exempt.year
LEFT JOIN default.vw_pin_tax_roll AS vptr
    ON asmt.parid = vptr.pin
    AND asmt.taxyr = vptr.year
LEFT JOIN default.vw_pin_land AS land
    ON asmt.parid = land.pin
    AND asmt.taxyr = land.year
LEFT JOIN oby
    ON asmt.parid = oby.parid
    AND asmt.taxyr = oby.taxyr
LEFT JOIN com
    ON asmt.parid = com.parid
    AND asmt.taxyr = com.taxyr
LEFT JOIN dwel
    ON asmt.parid = dwel.parid
    AND asmt.taxyr = dwel.taxyr
LEFT JOIN appeals
    ON asmt.parid = appeals.pin
    AND asmt.taxyr = appeals.year
