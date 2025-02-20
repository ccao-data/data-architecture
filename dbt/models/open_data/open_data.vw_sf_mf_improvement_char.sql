-- Copy of default.vw_card_res_char that feeds the "Single and Multi-Family
-- Improvement Characteristics" open data asset.
-- Some columns from the feeder view may not be present in this view.

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
    CASE
        WHEN char_type_resd = 1 THEN '1 Story'
        WHEN char_type_resd = 2 THEN '2 Story'
        WHEN char_type_resd = 3 THEN '3 Story +'
        WHEN char_type_resd = 4 THEN 'Split Level'
        WHEN char_type_resd = 5 THEN '1.5 Story'
        WHEN char_type_resd = 6 THEN '1.5 Story'
        WHEN char_type_resd = 7 THEN '1.5 Story'
        WHEN char_type_resd = 8 THEN '1.5 Story'
        WHEN char_type_resd = 9 THEN '1.5 Story'
    END AS char_type_resd,
    CASE
        WHEN char_cnst_qlty = '1' THEN 'Deluxe'
        WHEN char_cnst_qlty = '2' THEN 'Average'
        WHEN char_cnst_qlty = '3' THEN 'Poor'
    END AS char_cnst_qlty,
    CASE
        WHEN char_apts = '1' THEN 'Two'
        WHEN char_apts = '2' THEN 'Three'
        WHEN char_apts = '3' THEN 'Four'
        WHEN char_apts = '4' THEN 'Five'
        WHEN char_apts = '5' THEN 'Six'
        WHEN char_apts = '6' THEN 'None'
    END AS char_apts,
    CASE
        WHEN char_attic_fnsh = '1' THEN 'Living Area'
        WHEN char_attic_fnsh = '2' THEN 'Partial'
        WHEN char_attic_fnsh = '3' THEN 'None'
    END AS char_attic_fnsh,
    CASE
        WHEN char_gar1_att = '1' THEN 'Yes'
        WHEN char_gar1_att = '2' THEN 'No'
    END AS char_gar1_att,
    CASE
        WHEN char_gar1_area = '1' THEN 'Yes'
        WHEN char_gar1_area = '2' THEN 'No'
    END AS char_gar1_area,
    CASE
        WHEN char_gar1_size = '1' THEN '1 cars'
        WHEN char_gar1_size = '2' THEN '1.5 cars'
        WHEN char_gar1_size = '3' THEN '2 cars'
        WHEN char_gar1_size = '4' THEN '2.5 cars'
        WHEN char_gar1_size = '5' THEN '3 cars'
        WHEN char_gar1_size = '6' THEN '3.5 cars'
        WHEN char_gar1_size = '7' THEN '0 cars'
        WHEN char_gar1_size = '8' THEN '4 cars'
    END AS char_gar1_size,
    CASE
        WHEN char_gar1_cnst = '1' THEN 'Frame'
        WHEN char_gar1_cnst = '2' THEN 'Masonry'
        WHEN char_gar1_cnst = '3' THEN 'Frame + Masonry'
        WHEN char_gar1_cnst = '4' THEN 'Stucco'
    END AS char_gar1_cnst,
    CASE
        WHEN char_attic_type = '1' THEN 'Full'
        WHEN char_attic_type = '2' THEN 'Partial'
        WHEN char_attic_type = '3' THEN 'None'
    END AS char_attic_type,
    CASE
        WHEN char_bsmt = '1' THEN 'Full'
        WHEN char_bsmt = '2' THEN 'Slab'
        WHEN char_bsmt = '3' THEN 'Partial'
        WHEN char_bsmt = '4' THEN 'Crawl'
    END AS char_bsmt,
    CASE
        WHEN char_ext_wall = '1' THEN 'Frame'
        WHEN char_ext_wall = '2' THEN 'Masonry'
        WHEN char_ext_wall = '3' THEN 'Frame + Masonry'
        WHEN char_ext_wall = '4' THEN 'Stucco'
    END AS char_ext_wall,
    CASE
        WHEN char_heat = '1' THEN 'Warm Air Furnace'
        WHEN char_heat = '2' THEN 'Hot Water Steam'
        WHEN char_heat = '3' THEN 'Electric Heater'
        WHEN char_heat = '4' THEN 'None'
    END AS char_heat,
    CASE
        WHEN char_repair_cnd = '1' THEN 'Above Average'
        WHEN char_repair_cnd = '2' THEN 'Average'
        WHEN char_repair_cnd = '3' THEN 'Below Average'
    END AS char_repair_cnd,
    CASE
        WHEN char_bsmt_fin = '1' THEN 'Formal Rec Room'
        WHEN char_bsmt_fin = '2' THEN 'Apartment'
        WHEN char_bsmt_fin = '3' THEN 'Unfinished'
    END AS char_bsmt_fin,
    CASE
        WHEN char_roof_cnst = '1' THEN 'Shingle + Asphalt'
        WHEN char_roof_cnst = '2' THEN 'Tar + Gravel'
        WHEN char_roof_cnst = '3' THEN 'Slate'
        WHEN char_roof_cnst = '4' THEN 'Shake'
        WHEN char_roof_cnst = '5' THEN 'Tile'
        WHEN char_roof_cnst = '6' THEN 'Other'
    END AS char_roof_cnst,
    CASE
        WHEN char_use = '1' THEN 'Single-Family'
        WHEN char_use = '2' THEN 'Multi-Family'
    END AS char_use,
    CASE
        WHEN char_site = '1' THEN 'Beneficial To Value'
        WHEN char_site = '2' THEN 'Not Relevant To Value'
        WHEN char_site = '3' THEN 'Detracts From Value'
    END AS char_site,
    char_ncu,
    CASE
        WHEN char_porch = '0' THEN 'None'
        WHEN char_porch = '1' THEN 'Frame Enclosed'
        WHEN char_porch = '2' THEN 'Masonry Enclosed'
    END AS char_porch,
    CASE
        WHEN char_air = '1' THEN 'Central A/C'
        WHEN char_air = '2' THEN 'No Central A/C'
    END AS char_air,
    CASE
        WHEN char_tp_plan = '0' THEN 'None'
        WHEN char_tp_plan = '1' THEN 'Architect'
        WHEN char_tp_plan = '2' THEN 'Stock Plan'
    END AS char_tp_plan
FROM {{ ref('default.vw_card_res_char') }}
