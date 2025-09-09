-- This view feeds our Market Tracker™ Tableau dashboard. It combines sales
-- data with characteristics and geographic information.

-- CTE that lets us grab only the first two residential cards from
-- vw_card_res_char
WITH res_chars AS (
    SELECT
        *,
        RANK() OVER (
            PARTITION BY pin, year
            ORDER BY char_bldg_sf DESC, card ASC
        ) AS rnk
    FROM {{ ref('default.vw_card_res_char') }}
),

-- CTE to determine which models a given sale has been used for
models AS (
    SELECT
        train.meta_sale_document_num AS doc_no,
        ARRAY_JOIN(
            ARRAY_DISTINCT(
                ARRAY_AGG(CONCAT_WS(' ', fin_mod.type, fin_mod.year))
            ),
            ', '
        ) AS models_used
    FROM {{ ref('model.training_data') }} AS train
    INNER JOIN {{ ref('model.final_model') }} AS fin_mod
        ON train.run_id = fin_mod.run_id
        AND fin_mod.is_final
    GROUP BY train.meta_sale_document_num
)

SELECT
    vps.pin,
    vrc1.pin_is_multicard,
    vrc1.pin_num_cards,
    vps.class,
    cls.modeling_group,
    vps.year,
    vpu.triad_name AS triad,
    vpu.township_name AS township,
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
    vpa.prop_address_full,
    vpu.lat,
    vpu.lon,
    vps.doc_no,
    deeds.deed_name AS deed_type,
    vps.sale_price,
    vps.sale_date,
    vps.sale_filter_is_outlier,
    vps.sv_is_outlier,
    vps.is_multisale,
    vps.sale_filter_same_sale_within_365,
    vps.sale_filter_less_than_10k,
    vps.sale_filter_deed_type,
    vps.sv_outlier_reason1,
    vps.sv_outlier_reason2,
    vps.sv_outlier_reason3,
    -- Cards 1 and 2 are card numbers for the first two residential cards for a
    -- parcel. They will not always be values of 1 and 2.
    vrc1.card AS card1,
    COALESCE(vrc1.char_yrblt, vcr.char_yrblt) AS year_built1,
    vrc1.char_bldg_sf AS bldg_sf1,
    vrc1.char_land_sf AS land_sf1,
    vcr.char_unit_sf AS unit_sf1,
    vcr.is_common_area,
    vcr.is_parking_space,
    COALESCE(vrc1.char_fbath, vcr.char_full_baths) AS full_baths1,
    COALESCE(vrc1.char_hbath, vcr.char_half_baths) AS half_baths1,
    COALESCE(vrc1.char_beds, vcr.char_bedrooms) AS bedrooms1,
    vrc2.card AS card2,
    vrc2.char_yrblt AS year_built2,
    vrc2.char_bldg_sf AS bldg_sf2,
    vrc2.char_land_sf AS land_sf2,
    vrc2.char_fbath AS full_baths2,
    vrc2.char_hbath AS half_baths2,
    vrc2.char_beds AS bedrooms2,
    models.models_used
FROM {{ ref('default.vw_pin_sale') }} AS vps
LEFT JOIN
    {{ ref('default.vw_pin_universe') }} AS vpu
    ON vps.pin = vpu.pin AND vps.year = vpu.year
LEFT JOIN
    {{ ref('ccao.class_dict') }} AS cls
    ON vps.class = cls.class_code
LEFT JOIN
    {{ ref('default.vw_pin_address') }} AS vpa
    ON vps.pin = vpa.pin AND vps.year = vpa.year
-- We join res_chars twice so that we can get characteristics from up to two
-- residential cards with out creating duplicate sales
LEFT JOIN
    res_chars AS vrc1
    ON vps.pin = vrc1.pin AND vps.year = vrc1.year
    AND vrc1.rnk = 1
LEFT JOIN
    res_chars AS vrc2
    ON vps.pin = vrc2.pin AND vps.year = vrc2.year
    AND vrc2.rnk = 2
LEFT JOIN
    {{ ref('default.vw_pin_condo_char') }} AS vcr
    ON vps.pin = vcr.pin AND vps.year = vcr.year
LEFT JOIN models ON vps.doc_no = models.doc_no
LEFT JOIN {{ ref('sale.deed_type') }} AS deeds
    ON vps.deed_type = deeds.deed_num
WHERE vps.year BETWEEN '2020' AND '2024'
    AND cls.modeling_group IN ('SF', 'CONDO', 'MF')
