left_pad(`pin`, 14, '0')

case
    when `char_type_resd` = 1 then '1 Story'
    when `char_type_resd` = 2 then '2 Story'
    when `char_type_resd` = 3 then '3 Story +'
    when `char_type_resd` = 4 then 'Split Level'
    when `char_type_resd` = 5 then '1.5 Story'
    when `char_type_resd` = 6 then '1.5 Story'
    when `char_type_resd` = 7 then '1.5 Story'
    when `char_type_resd` = 8 then '1.5 Story'
    when `char_type_resd` = 9 then '1.5 Story'
    else null end

case
    when `char_cnst_qlty` = '1' then 'Deluxe'
    when `char_cnst_qlty` = '2' then 'Average'
    when `char_cnst_qlty` = '3' then 'Poor'
    else null end

case
    when `char_apts` = '1' then 'Two'
    when `char_apts` = '2' then 'Three'
    when `char_apts` = '3' then 'Four'
    when `char_apts` = '4' then 'Five'
    when `char_apts` = '5' then 'Six'
    when `char_apts` = '6' then 'None'
    else null end

case
    when `char_tp_dsgn` = '1' then 'Yes'
    when `char_tp_dsgn` = '2' then 'No'
    else null end

case
    when `char_gar1_att` = '1' then 'Yes'
    when `char_gar1_att` = '2' then 'No'
    else null end

case
    when `char_gar1_area` = '1' then 'Yes'
    when `char_gar1_area` = '2' then 'No'
    else null end

case
    when `char_gar1_size` = '1' then '1 cars'
    when `char_gar1_size` = '2' then '1.5 cars'
    when `char_gar1_size` = '3' then '2 cars'
    when `char_gar1_size` = '4' then '2.5 cars'
    when `char_gar1_size` = '5' then '3 cars'
    when `char_gar1_size` = '6' then '3.5 cars'
    when `char_gar1_size` = '7' then '0 cars'
    when `char_gar1_size` = '8' then '4 cars'
    else null end

case
    when `char_gar1_cnst` = '1' then 'Frame'
    when `char_gar1_cnst` = '2' then 'Masonry'
    when `char_gar1_cnst` = '3' then 'Frame + Masonry'
    when `char_gar1_cnst` = '4' then 'Stucco'
    else null end

case
    when `char_attic_type` = '1' then 'Full'
    when `char_attic_type` = '2' then 'Partial'
    when `char_attic_type` = '3' then 'None'
    else null end

case
    when `char_bsmt` = '1' then 'Full'
    when `char_bsmt` = '2' then 'Slab'
    when `char_bsmt` = '3' then 'Partial'
    when `char_bsmt` = '4' then 'Crawl'
    else null end

case
    when `char_ext_wall` = '1' then 'Frame'
    when `char_ext_wall` = '2' then 'Masonry'
    when `char_ext_wall` = '3' then 'Frame + Masonry'
    when `char_ext_wall` = '4' then 'Stucco'
    else null end
case
    when `char_heat` = '1' then 'Warm Air Furnace'
    when `char_heat` = '2' then 'Hot Water Steam'
    when `char_heat` = '3' then 'Electric Heater'
    when `char_heat` = '4' then 'None'
    else null end

case
    when `char_repair_cnd` = '1' then 'Above Average'
    when `char_repair_cnd` = '2' then 'Average'
    when `char_repair_cnd` = '3' then 'Below Average'
    else null end

case
    when `char_bsmt_fin` = '1' then 'Formal Rec Room'
    when `char_bsmt_fin` = '2' then 'Apartment'
    when `char_bsmt_fin` = '3' then 'Unfinished'
    else null end

case
    when `char_roof_cnst` = '1' then 'Shingle + Asphalt'
    when `char_roof_cnst` = '2' then 'Tar + Gravel'
    when `char_roof_cnst` = '3' then 'Slate'
    when `char_roof_cnst` = '4' then 'Shake'
    when `char_roof_cnst` = '5' then 'Tile'
    when `char_roof_cnst` = '6' then 'Other'
    else null end

case
    when `char_use` = '1' then 'Single-Family'
    when `char_use` = '2' then 'Multi-Family'
    else null end

case
    when `char_site` = '1' then 'Beneficial To Value'
    when `char_site` = '2' then 'Not Relevant To Value'
    when `char_site` = '3' then 'Detracts From Value'
    else null end

case
    when `char_porch` = '0' then 'None'
    when `char_porch` = '1' then 'Frame Enclosed'
    when `char_porch` = '2' then 'Masonry Enclosed'
    else null end

case
    when `char_renovation` = '1' then 'Yes'
    when `char_renovation` = '2' then 'No'
    else null end

case
    when `char_air` = '1' then 'Central A/C'
    when `char_air` = '2' then 'No Central A/C'
    else null end

case
    when `char_tp_plan` = '0' then 'None'
    when `char_tp_plan` = '1' then 'Architect'
    when `char_tp_plan` = '2' then 'Stock Plan'
    else null end