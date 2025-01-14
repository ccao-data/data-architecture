-- Copy of default.vw_card_res_char that feeds the "Single and Multi-Family
-- Improvement Characteristics" open data asset.

/* The following columns are not included in the open data asset, or are
currently hidden:
    pin10
    seq
    updated_by
    updated_at
    char_tp_dsgn
    char_class
    char_age
*/

SELECT
    pin || CAST(card AS VARCHAR) || year AS row_id,
    pin,
    year,
    card,
    class,
    township_code,
    cdu,
    tieback_key_pin,
    tieback_proration_rate,
    card_proration_rate,
    pin_is_multicard,
    pin_num_cards,
    pin_is_multiland,
    pin_num_landlines,
    char_yrblt,
    char_bldg_sf,
    char_land_sf,
    char_beds,
    char_rooms,
    char_fbath,
    char_hbath,
    char_frpl,
    char_type_resd,
    char_cnst_qlty,
    char_apts,
    char_attic_fnsh,
    char_gar1_att,
    char_gar1_area,
    char_gar1_size,
    char_gar1_cnst,
    char_attic_type,
    char_bsmt,
    char_ext_wall,
    char_heat,
    char_repair_cnd,
    char_bsmt_fin,
    char_roof_cnst,
    char_use,
    char_site,
    char_ncu,
    char_renovation,
    char_recent_renovation AS recent_renovation,
    char_porch,
    char_air,
    char_tp_plan
FROM {{ ref('default.vw_card_res_char') }}
