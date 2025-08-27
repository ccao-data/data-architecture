/*
XXXXXXXXXX
*/

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
    vrc.char_yrblt AS res_yr_built,
    vrc.char_bldg_sf AS res_bldg_sf,
    vrc.char_land_sf AS res_land_sf,
    vrc.char_fbath AS res_fbath,
    vrc.char_hbath AS res_hbath,
    vrc.char_beds AS res_bedrooms,
    vrc.pin_is_multicard,
    vrc.card,
    vrc.pin_num_cards,
    vcr.char_yrblt AS condo_yr_built,
    vcr.char_unit_sf AS condo_unit_sf,
    vcr.char_full_baths AS condo_fbath,
    vcr.char_half_baths AS condo_hbath,
    vcr.char_bedrooms AS condo_bedrooms,
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
