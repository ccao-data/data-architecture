-- This view feeds our Market Tracker™ Tableau dashboard. It combines sales
-- data with characteristics and geographic information.

SELECT
    vps.pin,
    vps.doc_no,
    vps.sale_price,
    vps.sale_date,
    vps.class,
    vps.year,
    vps.sale_filter_is_outlier,
    vps.sv_is_outlier,
    vps.is_multisale,
    vps.sale_filter_same_sale_within_365,
    vps.sale_filter_less_than_10k,
    vps.sale_filter_deed_type,
    vps.sv_outlier_reason1,
    vps.sv_outlier_reason2,
    vps.sv_outlier_reason3,
    vpu.chicago_community_area_name,
    ARRAY_JOIN(vpu.combined_municipality_name, ', ')
        AS combined_municipality_name,
    -- This ugly case when lets us report on both Chicago community areas and
    -- suburban municipalities
    CASE WHEN vpu.chicago_community_area_name IS NULL
            AND CARDINALITY(vpu.combined_municipality_name) = 0
            THEN CONCAT('UNINCORPORATED ', UPPER(vpu.township_name))
        ELSE
            COALESCE(
                vpu.chicago_community_area_name,
                ARRAY_JOIN(vpu.combined_municipality_name, ', ')
            )
    END AS geo_name,
    CASE
        WHEN
            vpu.chicago_community_area_name IS NOT NULL
            THEN 'community area'
        ELSE 'municipality'
    END AS geo_type,
    vpu.township_name,
    vpu.triad_name,
    vpu.lat,
    vpu.lon,
    vpa.prop_address_full,
    COALESCE(vrc.char_yrblt, vcr.char_yrblt) AS yr_built,
    vrc.char_bldg_sf AS bldg_sf,
    vrc.char_land_sf AS land_sf,
    vcr.char_unit_sf AS unit_sf,
    COALESCE(vrc.char_fbath, vcr.char_full_baths) AS fbath,
    COALESCE(vrc.char_hbath, vcr.char_half_baths) AS hbath,
    COALESCE(vrc.char_beds, vcr.char_bedrooms) AS bedrooms,
    vrc.pin_is_multicard,
    vrc.card,
    vrc.pin_num_cards,
    cls.modeling_group
FROM default.vw_pin_sale AS vps
LEFT JOIN
    default.vw_pin_universe AS vpu
    ON vps.pin = vpu.pin AND vps.year = vpu.year
LEFT JOIN ccao.class_dict AS cls ON vps.class = cls.class_code
LEFT JOIN
    default.vw_pin_address AS vpa
    ON vps.pin = vpa.pin AND vps.year = vpa.year
LEFT JOIN
    default.vw_card_res_char AS vrc
    ON vps.pin = vrc.pin AND vps.year = vrc.year
LEFT JOIN
    default.vw_pin_condo_char AS vcr
    ON vps.pin = vcr.pin AND vps.year = vcr.year
WHERE vps.year BETWEEN '2020' AND '2024'
    AND cls.modeling_group IN ('SF', 'CONDO', 'MF')
