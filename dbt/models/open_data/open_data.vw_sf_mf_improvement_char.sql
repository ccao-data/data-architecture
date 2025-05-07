-- Copy of default.vw_card_res_char that feeds the "Single and Multi-Family
-- Improvement Characteristics" open data asset.
-- Some columns from the feeder view may not be present in this view.

SELECT
    feeder.pin,
    feeder.card,
    feeder.class,
    feeder.township_code,
    feeder.cdu,
    feeder.tieback_key_pin,
    feeder.tieback_proration_rate,
    feeder.card_proration_rate,
    feeder.pin_is_multicard,
    feeder.pin_num_cards,
    feeder.pin_is_multiland,
    feeder.pin_num_landlines,
    feeder.char_yrblt,
    feeder.char_bldg_sf,
    feeder.char_land_sf,
    feeder.char_beds,
    feeder.char_rooms,
    feeder.char_fbath,
    feeder.char_hbath,
    feeder.char_frpl,
    CASE
        WHEN feeder.char_type_resd = 1 THEN '1 Story'
        WHEN feeder.char_type_resd = 2 THEN '2 Story'
        WHEN feeder.char_type_resd = 3 THEN '3 Story +'
        WHEN feeder.char_type_resd = 4 THEN 'Split Level'
        WHEN feeder.char_type_resd = 5 THEN '1.5 Story'
        WHEN feeder.char_type_resd = 6 THEN '1.5 Story'
        WHEN feeder.char_type_resd = 7 THEN '1.5 Story'
        WHEN feeder.char_type_resd = 8 THEN '1.5 Story'
        WHEN feeder.char_type_resd = 9 THEN '1.5 Story'
    END AS char_type_resd,
    CASE
        WHEN feeder.char_cnst_qlty = '1' THEN 'Deluxe'
        WHEN feeder.char_cnst_qlty = '2' THEN 'Average'
        WHEN feeder.char_cnst_qlty = '3' THEN 'Poor'
    END AS char_cnst_qlty,
    CASE
        WHEN feeder.char_apts = '1' THEN 'Two'
        WHEN feeder.char_apts = '2' THEN 'Three'
        WHEN feeder.char_apts = '3' THEN 'Four'
        WHEN feeder.char_apts = '4' THEN 'Five'
        WHEN feeder.char_apts = '5' THEN 'Six'
        WHEN feeder.char_apts = '6' THEN 'None'
    END AS char_apts,
    CASE
        WHEN feeder.char_attic_fnsh = '1' THEN 'Living Area'
        WHEN feeder.char_attic_fnsh = '2' THEN 'Partial'
        WHEN feeder.char_attic_fnsh = '3' THEN 'None'
    END AS char_attic_fnsh,
    CASE
        WHEN feeder.char_gar1_att = '1' THEN 'Yes'
        WHEN feeder.char_gar1_att = '2' THEN 'No'
    END AS char_gar1_att,
    CASE
        WHEN feeder.char_gar1_area = '1' THEN 'Yes'
        WHEN feeder.char_gar1_area = '2' THEN 'No'
    END AS char_gar1_area,
    CASE
        WHEN feeder.char_gar1_size = '1' THEN '1 cars'
        WHEN feeder.char_gar1_size = '2' THEN '1.5 cars'
        WHEN feeder.char_gar1_size = '3' THEN '2 cars'
        WHEN feeder.char_gar1_size = '4' THEN '2.5 cars'
        WHEN feeder.char_gar1_size = '5' THEN '3 cars'
        WHEN feeder.char_gar1_size = '6' THEN '3.5 cars'
        WHEN feeder.char_gar1_size = '7' THEN '0 cars'
        WHEN feeder.char_gar1_size = '8' THEN '4 cars'
    END AS char_gar1_size,
    CASE
        WHEN feeder.char_gar1_cnst = '1' THEN 'Frame'
        WHEN feeder.char_gar1_cnst = '2' THEN 'Masonry'
        WHEN feeder.char_gar1_cnst = '3' THEN 'Frame + Masonry'
        WHEN feeder.char_gar1_cnst = '4' THEN 'Stucco'
    END AS char_gar1_cnst,
    CASE
        WHEN feeder.char_attic_type = '1' THEN 'Full'
        WHEN feeder.char_attic_type = '2' THEN 'Partial'
        WHEN feeder.char_attic_type = '3' THEN 'None'
    END AS char_attic_type,
    CASE
        WHEN feeder.char_bsmt = '1' THEN 'Full'
        WHEN feeder.char_bsmt = '2' THEN 'Slab'
        WHEN feeder.char_bsmt = '3' THEN 'Partial'
        WHEN feeder.char_bsmt = '4' THEN 'Crawl'
    END AS char_bsmt,
    CASE
        WHEN feeder.char_ext_wall = '1' THEN 'Frame'
        WHEN feeder.char_ext_wall = '2' THEN 'Masonry'
        WHEN feeder.char_ext_wall = '3' THEN 'Frame + Masonry'
        WHEN feeder.char_ext_wall = '4' THEN 'Stucco'
    END AS char_ext_wall,
    CASE
        WHEN feeder.char_heat = '1' THEN 'Warm Air Furnace'
        WHEN feeder.char_heat = '2' THEN 'Hot Water Steam'
        WHEN feeder.char_heat = '3' THEN 'Electric Heater'
        WHEN feeder.char_heat = '4' THEN 'None'
    END AS char_heat,
    CASE
        WHEN feeder.char_repair_cnd = '1' THEN 'Above Average'
        WHEN feeder.char_repair_cnd = '2' THEN 'Average'
        WHEN feeder.char_repair_cnd = '3' THEN 'Below Average'
    END AS char_repair_cnd,
    CASE
        WHEN feeder.char_bsmt_fin = '1' THEN 'Formal Rec Room'
        WHEN feeder.char_bsmt_fin = '2' THEN 'Apartment'
        WHEN feeder.char_bsmt_fin = '3' THEN 'Unfinished'
    END AS char_bsmt_fin,
    CASE
        WHEN feeder.char_roof_cnst = '1' THEN 'Shingle + Asphalt'
        WHEN feeder.char_roof_cnst = '2' THEN 'Tar + Gravel'
        WHEN feeder.char_roof_cnst = '3' THEN 'Slate'
        WHEN feeder.char_roof_cnst = '4' THEN 'Shake'
        WHEN feeder.char_roof_cnst = '5' THEN 'Tile'
        WHEN feeder.char_roof_cnst = '6' THEN 'Other'
    END AS char_roof_cnst,
    CASE
        WHEN feeder.char_use = '1' THEN 'Single-Family'
        WHEN feeder.char_use = '2' THEN 'Multi-Family'
    END AS char_use,
    CASE
        WHEN feeder.char_site = '1' THEN 'Beneficial To Value'
        WHEN feeder.char_site = '2' THEN 'Not Relevant To Value'
        WHEN feeder.char_site = '3' THEN 'Detracts From Value'
    END AS char_site,
    feeder.char_ncu,
    CASE
        WHEN feeder.char_renovation = '1' THEN 'Yes'
        WHEN feeder.char_renovation = '2' THEN 'No'
    END AS char_renovation,
    CASE
        WHEN feeder.char_porch = '0' THEN 'None'
        WHEN feeder.char_porch = '1' THEN 'Frame Enclosed'
        WHEN feeder.char_porch = '2' THEN 'Masonry Enclosed'
    END AS char_porch,
    CASE
        WHEN feeder.char_air = '1' THEN 'Central A/C'
        WHEN feeder.char_air = '2' THEN 'No Central A/C'
    END AS char_air,
    CASE
        WHEN feeder.char_tp_plan = '0' THEN 'None'
        WHEN feeder.char_tp_plan = '1' THEN 'Architect'
        WHEN feeder.char_tp_plan = '2' THEN 'Stock Plan'
    END AS char_tp_plan,
    {{ open_data_columns(card=true) }}
FROM {{ ref('default.vw_card_res_char') }} AS feeder
{{ open_data_rows_to_delete(card=true) }}
