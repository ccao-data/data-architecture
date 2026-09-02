{%- set tests = [
    {
        "name": "iasworld_dweldat_attic_in_accepted_values",
        "description": "attic (Attic Type) should be an integer between 1 and 5",
        "category": "incorrect_values",
        "condition": "attic IN ('1', '2', '3', '4', '5')",
        "additional_select_columns": ["attic"]
    },
    {
        "name": "iasworld_dweldat_attic_not_null",
        "description": "attic (Attic Type) should not be null",
        "category": "missing_values",
        "condition": "attic IS NOT NULL"
    },
    {
        "name": "iasworld_dweldat_bsmt_in_accepted_values",
        "description": "bsmt (Basement Type) should be an integer between 1 and 7",
        "category": "incorrect_values",
        "condition": "bsmt IN ('1', '2', '3', '4', '5', '6', '7')",
        "additional_select_columns": ["bsmt"]
    },
    {
        "name": "iasworld_dweldat_bsmt_not_null",
        "description": "bsmt (Basement Type) should not be null",
        "category": "missing_values",
        "condition": "bsmt IS NOT NULL"
    },
    {
        "name": "iasworld_dweldat_bsmt_and_user12_match_234_class",
        "description": "bsmt (Basement Type) must be 3 - PARTIAL and user12 (Basement Finished) must be 1 - FAMILY ROOM when class is 234 (split level)",
        "category": "incorrect_values",
        "condition": "class != '234' OR (bsmt = '3' AND user12 = '1')",
        "additional_select_columns": ["bsmt", "user12"]
    },
    {
        "name": "iasworld_dweldat_calc_meth_accepted_values",
        "description": 'calc_meth (Calculation Method) should be null or "E"',
        "category": "incorrect_values",
        "condition": "calc_meth IS NULL OR calc_meth = 'E'",
        "additional_select_columns": ["calc_meth"]
    },
    {
        "name": "iasworld_dweldat_card_gte_1",
        "description": "card should be >= 1",
        "category": "incorrect_values",
        "condition": "card IS NULL OR card >= 1"
    },
    {
        "name": "iasworld_dweldat_card_not_null",
        "description": "card should not be null",
        "category": "missing_values",
        "condition": "card IS NOT NULL"
    },
    {
        "name": "iasworld_dweldat_card_proration_rate_between_0_and_100",
        "description": "user24 (Proration %) should be between 0 and 100",
        "category": "incorrect_values",
        "condition": "user24 IS NULL OR CAST(user24 AS decimal) BETWEEN 0.00 AND 100.00",
        "additional_select_columns": ["user24"]
    },
    {
        "name": "iasworld_dweldat_class_in_ccao_class_dict",
        "description": "class code must be valid",
        "category": "class_mismatch_or_issue",
        "condition": "class IN ('EX', 'RR') OR class_dict_class IS NOT NULL"
    },
    {
        "name": "iasworld_dweldat_class_matches_pardat_class",
        "description": "at least one class should match pardat class",
        "category": "class_mismatch_or_issue",
        "condition": "comdat_parid IS NOT NULL OR pardat_class IS NULL OR class = pardat_class",
        "additional_select_columns": ["pardat_class"]
    },
    {
        "name": "iasworld_dweldat_exempt_classes_match_pardat_class",
        "description": "at least one class should be exempt or omitted if pardat is exempt",
        "category": "class_mismatch_or_issue",
        "condition": "class LIKE 'OA%' OR pardat_class IS NULL OR pardat_class != 'EX' OR class = 'EX'",
        "additional_select_columns": ["pardat_class"]
    },
    {
        "name": "iasworld_dweldat_cur_in_accepted_values",
        "description": 'cur should be "Y" or "D"',
        "category": "incorrect_values",
        "condition": "cur IN ('Y', 'D')",
        "additional_select_columns": ["cur"]
    },
    {
        "name": "iasworld_dweldat_external_calc_rcnld_not_0",
        "description": "external_calc_rcnld (Net Market Value) should not be 0",
        "category": "incorrect_values",
        "condition": "external_calc_rcnld IS NULL OR external_calc_rcnld != 0",
        "additional_select_columns": ["external_calc_rcnld"]
    },
    {
        "name": "iasworld_dweldat_external_occpct_not_0",
        "description": "external_occpct (Occupancy %) should not be 0",
        "category": "incorrect_values",
        "condition": "external_occpct IS NULL OR external_occpct != 0",
        "additional_select_columns": ["external_occpct"]
    },
    {
        "name": "iasworld_dweldat_external_occpct_not_null_when_mktrsn_eq_5_or_5b_and_mktadj_is_null",
        "description": "external_occpct (Occupancy % [current]) should not be null if mktrsn is 5 or 5B and mktadj is null",
        "category": "missing_values",
        "condition": "mktrsn NOT IN ('5', '5B') OR mktadj IS NOT NULL OR external_occpct IS NOT NULL",
        "additional_select_columns": ["external_occpct", "mktrsn", "mktadj"]
    },
    {
        "name": "iasworld_dweldat_external_propct_not_0",
        "description": "external_propct (Proration %) should not be 0",
        "category": "incorrect_values",
        "condition": "external_propct IS NULL OR external_propct != 0",
        "additional_select_columns": ["external_propct"]
    },
    {
        "name": "iasworld_dweldat_external_rcnld_not_0",
        "description": "external_rcnld (Full Market Value) should not be 0",
        "category": "incorrect_values",
        "condition": "external_rcnld IS NULL OR external_rcnld != 0",
        "additional_select_columns": ["external_rcnld"]
    },
    {
        "name": "iasworld_dweldat_extwall_in_accepted_values",
        "description": "extwall (Exterior Construction) should be 1, 2, 3, 4, 6, 7, 8, or 9",
        "category": "incorrect_values",
        "condition": "extwall IN ('1', '2', '3', '4', '6', '7', '8', '9')",
        "additional_select_columns": ["extwall"]
    },
    {
        "name": "iasworld_dweldat_extwall_not_null",
        "description": "extwall (Exterior Construction) should not be null",
        "category": "missing_values",
        "condition": "extwall IS NOT NULL"
    },
    {
        "name": "iasworld_dweldat_fixbath_matches_number_of_units",
        "description": "fixbath (Number of Full Baths) should be <= 7 times the number of units (user14)",
        "category": "incorrect_values",
        "condition": "fixbath IS NULL OR (fixbath >= 1 AND fixbath <= CASE WHEN user14 IS NULL OR user14 = '0' OR user14 = '6' THEN 7 WHEN user14 = '1' THEN 14 WHEN user14 = '2' THEN 21 WHEN user14 = '3' THEN 28 WHEN user14 = '4' THEN 35 WHEN user14 = '5' THEN 42 ELSE 7 END)",
        "additional_select_columns": ["fixbath", "user14"]
    },
    {
        "name": "iasworld_dweldat_fixbath_not_null",
        "description": "fixbath (Number of Full Baths) should not be null",
        "category": "missing_values",
        "condition": "fixbath IS NOT NULL"
    },
    {
        "name": "iasworld_dweldat_fixhalf_gte_0",
        "description": "fixhalf (Number of Half Baths) should be >= 0",
        "category": "incorrect_values",
        "condition": "fixhalf IS NULL OR fixhalf >= 0",
        "additional_select_columns": ["fixhalf"]
    },
    {
        "name": "iasworld_dweldat_fixhalf_matches_number_of_units",
        "description": "fixhalf (Number of Half Baths) should be <= 5 times the number of units (user14)",
        "category": "incorrect_values",
        "condition": "fixhalf IS NULL OR (fixhalf >= 0 AND fixhalf <= CASE WHEN user14 IS NULL OR user14 = '0' OR user14 = '6' THEN 5 WHEN user14 = '1' THEN 10 WHEN user14 = '2' THEN 15 WHEN user14 = '3' THEN 20 WHEN user14 = '4' THEN 25 WHEN user14 = '5' THEN 30 ELSE 5 END)",
        "additional_select_columns": ["fixhalf", "user14"]
    },
    {
        "name": "iasworld_dweldat_heat_in_accepted_values",
        "description": "heat (Heating) should be an integer between 1 and 4",
        "category": "incorrect_values",
        "condition": "heat IN ('1', '2', '3', '4')",
        "additional_select_columns": ["heat"]
    },
    {
        "name": "iasworld_dweldat_heat_not_null",
        "description": "heat (Heating) should not be null",
        "category": "missing_values",
        "condition": "heat IS NOT NULL"
    },
    {
        "name": "iasworld_dweldat_mktadj_not_null_when_mktrsn_eq_5_or_5b_and_external_occpct_is_null",
        "description": "mktadj (Occupancy % [deprecated]) should not be null if mktrsn is 5 or 5B and external_occpct is null",
        "category": "missing_values",
        "condition": "mktrsn NOT IN ('5', '5B') OR external_occpct IS NOT NULL OR mktadj IS NOT NULL",
        "additional_select_columns": ["mktadj", "mktrsn", "external_occpct"]
    },
    {
        "name": "iasworld_dweldat_mktrsn_eq_5_or_5b_when_external_occpct_or_mktadj_not_null",
        "description": "mktrsn (Reason for Override) should be 5 or 5B if external_occpct or mktadj is not null",
        "category": "incorrect_values",
        "condition": "(external_occpct IS NULL AND mktadj IS NULL) OR mktrsn IN ('5', '5B')",
        "additional_select_columns": ["mktrsn", "external_occpct", "mktadj"]
    },
    {
        "name": "iasworld_dweldat_parid_not_null",
        "description": "parid should not be null",
        "category": "missing_values",
        "condition": "parid IS NOT NULL"
    },
    {
        "name": "iasworld_dweldat_parid_in_pardat_parid",
        "description": "parid should be in pardat",
        "category": "relationships",
        "condition": "pardat_parid IS NOT NULL"
    },
    {
        "name": "iasworld_dweldat_rmbed_lte_rmtot",
        "description": "rmbed (Number of Bedrooms) should be <= rmtot (Number of Rooms)",
        "category": "relationships",
        "condition": "rmbed IS NULL OR rmtot IS NULL OR rmbed <= rmtot",
        "additional_select_columns": ["rmbed", "rmtot"]
    },
    {
        "name": "iasworld_dweldat_rmbed_matches_number_of_units",
        "description": "rmbed (Number of Bedrooms) should be <= 8 times the number of units (user14)",
        "category": "incorrect_values",
        "condition": "rmbed IS NULL OR (rmbed >= 1 AND rmbed <= CASE WHEN user14 IS NULL OR user14 = '0' OR user14 = '6' THEN 8 WHEN user14 = '1' THEN 16 WHEN user14 = '2' THEN 24 WHEN user14 = '3' THEN 32 WHEN user14 = '4' THEN 40 WHEN user14 = '5' THEN 48 ELSE 8 END)",
        "additional_select_columns": ["rmbed", "user14"]
    },
    {
        "name": "iasworld_dweldat_rmbed_not_null",
        "description": "rmbed (Number of Bedrooms) should not be null",
        "category": "missing_values",
        "condition": "rmbed IS NOT NULL"
    },
    {
        "name": "iasworld_dweldat_rmtot_not_null",
        "description": "rmtot (Number of Rooms) should not be null",
        "category": "missing_values",
        "condition": "rmtot IS NOT NULL"
    },
    {
        "name": "iasworld_dweldat_rmtot_sf_between_1_and_40",
        "description": "rmtot (Number of Rooms) should be between 1 and 40",
        "category": "incorrect_values",
        "condition": "class IN ('211', '212') OR rmtot IS NULL OR (rmtot >= 1 AND rmtot <= 40)",
        "additional_select_columns": ["rmtot"]
    },
    {
        "name": "iasworld_dweldat_rmtot_sf_between_1_and_50",
        "description": "rmtot (Number of Rooms) should be between 1 and 50",
        "category": "incorrect_values",
        "condition": "class NOT IN ('211', '212') OR rmtot IS NULL OR (rmtot >= 1 AND rmtot <= 50)",
        "additional_select_columns": ["rmtot"]
    },
    {
        "name": "iasworld_dweldat_seq_all_sequential_exist",
        "description": "seq should be sequential",
        "category": "incorrect_values",
        "condition": "prev_seq IS NULL OR seq = prev_seq + 1",
        "additional_select_columns": ["seq", "prev_seq"]
    },
    {
        "name": "iasworld_dweldat_sfla_between_1_and_999_for_class_202",
        "description": "sfla (Building Square Footage) should be between 1 and 999 for class 202 cards",
        "category": "incorrect_values",
        "condition": "class != '202' OR sfla IS NULL OR (sfla >= 1 AND sfla <= 999)",
        "additional_select_columns": ["sfla"]
    },
    {
        "name": "iasworld_dweldat_sfla_between_1_and_2000_for_class_207",
        "description": "sfla (Building Square Footage) should be between 1 and 2000 for class 207 cards",
        "category": "incorrect_values",
        "condition": "class != '207' OR sfla IS NULL OR (sfla >= 1 AND sfla <= 2000)",
        "additional_select_columns": ["sfla"]
    },
    {
        "name": "iasworld_dweldat_sfla_between_1_and_2200_for_class_205",
        "description": "sfla (Building Square Footage) should be between 1 and 2200 for class 205 cards",
        "category": "incorrect_values",
        "condition": "class != '205' OR sfla IS NULL OR (sfla >= 1 AND sfla <= 2200)",
        "additional_select_columns": ["sfla"]
    },
    {
        "name": "iasworld_dweldat_sfla_between_1_and_10000_for_class_210",
        "description": "sfla (Building Square Footage) should be between 1 and 10000 for class 210 cards",
        "category": "incorrect_values",
        "condition": "class != '210' OR sfla IS NULL OR (sfla >= 1 AND sfla <= 10000)",
        "additional_select_columns": ["sfla"]
    },
    {
        "name": "iasworld_dweldat_sfla_between_1_and_10000_for_class_234",
        "description": "sfla (Building Square Footage) should be between 1 and 10000 for class 234 cards",
        "category": "incorrect_values",
        "condition": "class != '234' OR sfla IS NULL OR (sfla >= 1 AND sfla <= 10000)",
        "additional_select_columns": ["sfla"]
    },
    {
        "name": "iasworld_dweldat_sfla_between_1_and_20000_for_class_212",
        "description": "sfla (Building Square Footage) should be between 1 and 20000 for class 212 cards",
        "category": "incorrect_values",
        "condition": "class != '212' OR sfla IS NULL OR (sfla >= 1 AND sfla <= 20000)",
        "additional_select_columns": ["sfla"]
    },
    {
        "name": "iasworld_dweldat_sfla_between_1_and_20000_for_class_295",
        "description": "sfla (Building Square Footage) should be between 1 and 20000 for class 295 cards",
        "category": "incorrect_values",
        "condition": "class != '295' OR sfla IS NULL OR (sfla >= 1 AND sfla <= 20000)",
        "additional_select_columns": ["sfla"]
    },
    {
        "name": "iasworld_dweldat_sfla_between_1_and_40000_for_class_211",
        "description": "sfla (Building Square Footage) should be between 1 and 40000 for class 211 cards",
        "category": "incorrect_values",
        "condition": "class != '211' OR sfla IS NULL OR (sfla >= 1 AND sfla <= 40000)",
        "additional_select_columns": ["sfla"]
    },
    {
        "name": "iasworld_dweldat_sfla_between_1000_and_1800_for_class_203",
        "description": "sfla (Building Square Footage) should be between 1000 and 1800 for class 203 cards",
        "category": "incorrect_values",
        "condition": "class != '203' OR sfla IS NULL OR (sfla >= 1000 AND sfla <= 1800)",
        "additional_select_columns": ["sfla"]
    },
    {
        "name": "iasworld_dweldat_sfla_between_1801_and_25000_for_class_204",
        "description": "sfla (Building Square Footage) should be between 1801 and 25000 for class 204 cards",
        "category": "incorrect_values",
        "condition": "class != '204' OR sfla IS NULL OR (sfla >= 1801 AND sfla <= 25000)",
        "additional_select_columns": ["sfla"]
    },
    {
        "name": "iasworld_dweldat_sfla_between_2001_and_3800_for_class_278",
        "description": "sfla (Building Square Footage) should be between 2001 and 3800 for class 278 cards",
        "category": "incorrect_values",
        "condition": "class != '278' OR sfla IS NULL OR (sfla >= 2001 AND sfla <= 3800)",
        "additional_select_columns": ["sfla"]
    },
    {
        "name": "iasworld_dweldat_sfla_between_2201_and_4999_for_class_206",
        "description": "sfla (Building Square Footage) should be between 2201 and 4999 for class 206 cards",
        "category": "incorrect_values",
        "condition": "class != '206' OR sfla IS NULL OR (sfla >= 2201 AND sfla <= 4999)",
        "additional_select_columns": ["sfla"]
    },
    {
        "name": "iasworld_dweldat_sfla_between_3801_and_4999_for_class_208",
        "description": "sfla (Building Square Footage) should be between 3801 and 4999 for class 208 cards",
        "category": "incorrect_values",
        "condition": "class != '208' OR sfla IS NULL OR (sfla >= 3801 AND sfla <= 4999)",
        "additional_select_columns": ["sfla"]
    },
    {
        "name": "iasworld_dweldat_sfla_between_5000_and_50000_for_class_209",
        "description": "sfla (Building Square Footage) should be between 5000 and 50000 for class 209 cards",
        "category": "incorrect_values",
        "condition": "class != '209' OR sfla IS NULL OR (sfla >= 5000 AND sfla <= 50000)",
        "additional_select_columns": ["sfla"]
    },
    {
        "name": "iasworld_dweldat_sfla_not_null",
        "description": "sfla (Building Square Footage) should not be null",
        "category": "missing_values",
        "condition": "sfla IS NOT NULL"
    },
    {
        "name": "iasworld_dweldat_stories_in_accepted_values",
        "description": "stories (Type of Residence) should be one of 1.00, 2.00, 3.00, 4.00, 5.00, 6.00, 7.00, 8.00, 9.00, or 9.90",
        "category": "incorrect_values",
        "condition": "stories IN (1.00, 2.00, 3.00, 4.00, 5.00, 6.00, 7.00, 8.00, 9.00, 9.90)",
        "additional_select_columns": ["stories"]
    },
    {
        "name": "iasworld_dweldat_stories_not_null",
        "description": "stories (Type of Residence) should not be null",
        "category": "missing_values",
        "condition": "stories IS NOT NULL"
    },
    {
        "name": "iasworld_dweldat_taxyr_not_null",
        "description": "taxyr should not be null",
        "category": "missing_values",
        "condition": "taxyr IS NOT NULL"
    },
    {
        "name": "iasworld_dweldat_char_renovation_in_accepted_values",
        "description": 'user3 (Renovated) should be "0" or "1"',
        "category": "incorrect_values",
        "condition": "user3 IN ('0', '1')",
        "additional_select_columns": ["user3"]
    },
    {
        "name": "iasworld_dweldat_char_tp_plan_in_accepted_values",
        "description": 'user5 (Plan of Design) should be "0", "1", or "2"',
        "category": "incorrect_values",
        "condition": "user5 IN ('0', '1', '2')",
        "additional_select_columns": ["user5"]
    },
    {
        "name": "iasworld_dweldat_char_tp_plan_not_null",
        "description": "user5 (Plan of Design) should not be null",
        "category": "missing_values",
        "condition": "user5 IS NOT NULL"
    },
    {
        "name": "iasworld_dweldat_char_attic_fnsh_in_accepted_values",
        "description": 'user6 (Attic Finish) should be "1", "2", or "3"',
        "category": "incorrect_values",
        "condition": "user6 IN ('1', '2', '3')",
        "additional_select_columns": ["user6"]
    },
    {
        "name": "iasworld_dweldat_char_attic_fnsh_not_null",
        "description": "user6 (Attic Finish) should not be null unless attic is 3 (None)",
        "category": "missing_values",
        "condition": "attic = '3' OR user6 IS NOT NULL",
        "additional_select_columns": ["user6", "attic"]
    },
    {
        "name": "iasworld_dweldat_char_air_accepted_values",
        "description": 'user7 (Central Air Conditioning) should be "1" or "2"',
        "category": "incorrect_values",
        "condition": "user7 IN ('1', '2')",
        "additional_select_columns": ["user7"]
    },
    {
        "name": "iasworld_dweldat_char_air_not_null",
        "description": "user7 (Central Air Conditioning) should not be null",
        "category": "missing_values",
        "condition": "user7 IS NOT NULL"
    },
    {
        "name": "iasworld_dweldat_char_bsmt_fin_accepted_values",
        "description": "user12 (Basement Finished) should be an integer between 1 and 6",
        "category": "incorrect_values",
        "condition": "user12 IN ('1', '2', '3', '4', '5', '6')",
        "additional_select_columns": ["user12"]
    },
    {
        "name": "iasworld_dweldat_char_bsmt_fin_not_null",
        "description": "user12 (Basement Finished) should not be null unless bsmt is 2 (Slab)",
        "category": "missing_values",
        "condition": "bsmt = '2' OR user12 IS NOT NULL",
        "additional_select_columns": ["user12", "bsmt"]
    },
    {
        "name": "iasworld_dweldat_char_roof_cnst_accepted_values",
        "description": "user13 (Roof Construction) should be an integer between 1 and 6",
        "category": "incorrect_values",
        "condition": "user13 IN ('1', '2', '3', '4', '5', '6')",
        "additional_select_columns": ["user13"]
    },
    {
        "name": "iasworld_dweldat_char_roof_cnst_not_null",
        "description": "user13 (Roof Construction) should not be null",
        "category": "missing_values",
        "condition": "user13 IS NOT NULL"
    },
    {
        "name": "iasworld_dweldat_char_apts_accepted_values",
        "description": "user14 (Total Number of Units) should be an integer between 1 and 6 for class 211 and 212",
        "category": "incorrect_values",
        "condition": "class NOT IN ('211', '212') OR user14 IN ('1', '2', '3', '4', '5', '6')",
        "additional_select_columns": ["user14"]
    },
    {
        "name": "iasworld_dweldat_char_apts_not_null",
        "description": "user14 (Total Number of Units) should not be null for class 211 and 212",
        "category": "missing_values",
        "condition": "class NOT IN ('211', '212') OR user14 IS NOT NULL"
    },
    {
        "name": "iasworld_dweldat_char_use_accepted_values",
        "description": 'user15 (Use) should be "1" or "2"',
        "category": "incorrect_values",
        "condition": "user15 IN ('1', '2')",
        "additional_select_columns": ["user15"]
    },
    {
        "name": "iasworld_dweldat_char_use_not_null",
        "description": "user15 (Use) should not be null",
        "category": "missing_values",
        "condition": "user15 IS NOT NULL"
    },
    {
        "name": "iasworld_dweldat_char_ncu_between_0_and_5",
        "description": "user20 (No. of Commercial Units) should be an integer between 0 and 5",
        "category": "incorrect_values",
        "condition": "user20 IN ('0', '1', '2', '3', '4', '5')",
        "additional_select_columns": ["user20"]
    },
    {
        "name": "iasworld_dweldat_char_ncu_not_0_when_class_is_212",
        "description": "user20 (No. of Commercial Units) should not be 0 when class is 212",
        "category": "incorrect_values",
        "condition": "class != '212' OR user20 != '0'",
        "additional_select_columns": ["user20"]
    },
    {
        "name": "iasworld_dweldat_char_ncu_not_null_when_class_is_212",
        "description": "user20 (No. of Commercial Units) should not be null when class is 212",
        "category": "missing_values",
        "condition": "class != '212' OR user20 IS NOT NULL"
    },
    {
        "name": "iasworld_dweldat_char_ncu_null_when_class_is_not_212_and_char_ncu_not_0",
        "description": "user20 (No. of Commercial Units) should be null or 0 when class is not 212",
        "category": "incorrect_values",
        "condition": "class IN ('EX', 'RR') OR class = '212' OR user20 = '0' OR user20 IS NULL",
        "additional_select_columns": ["user20"]
    },
    {
        "name": "iasworld_dweldat_char_porch_accepted_values",
        "description": 'user30 (Porch) should be "0", "1", or "2"',
        "category": "incorrect_values",
        "condition": "user30 IN ('0', '1', '2')",
        "additional_select_columns": ["user30"]
    },
    {
        "name": "iasworld_dweldat_char_gar_att_accepted_values",
        "description": 'user31 (Garage Attached) should be "1" or "2"',
        "category": "incorrect_values",
        "condition": "user31 IN ('1', '2')",
        "additional_select_columns": ["user31"]
    },
    {
        "name": "iasworld_dweldat_char_gar_att_not_null",
        "description": "user31 (Garage Attached) should not be null if user33 (Garage Size) is not 7 (NONE)",
        "category": "missing_values",
        "condition": "user33 = '7' OR user31 IS NOT NULL",
        "additional_select_columns": ["user31", "user33"]
    },
    {
        "name": "iasworld_dweldat_char_gar_area_accepted_values",
        "description": 'user32 (Garage in Area) should be "1" or "2"',
        "category": "incorrect_values",
        "condition": "user32 IN ('1', '2')",
        "additional_select_columns": ["user32"]
    },
    {
        "name": "iasworld_dweldat_char_gar_area_not_null",
        "description": "user32 (Garage in Area) should not be null if user33 (Garage Size) is not 7 (NONE)",
        "category": "missing_values",
        "condition": "user33 = '7' OR user32 IS NOT NULL",
        "additional_select_columns": ["user32", "user33"]
    },
    {
        "name": "iasworld_dweldat_char_gar_size_accepted_values",
        "description": "user33 (Garage Size) should be an integer between 1 and 10",
        "category": "incorrect_values",
        "condition": "user33 IN ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10')",
        "additional_select_columns": ["user33"]
    },
    {
        "name": "iasworld_dweldat_char_gar_size_not_null",
        "description": "user33 (Garage Size) should not be null",
        "category": "missing_values",
        "condition": "user33 IS NOT NULL"
    },
    {
        "name": "iasworld_dweldat_char_gar_cnst_accepted_values",
        "description": "user34 (Garage Construction) should be an integer between 1 and 4",
        "category": "incorrect_values",
        "condition": "user34 IN ('1', '2', '3', '4')",
        "additional_select_columns": ["user34"]
    },
    {
        "name": "iasworld_dweldat_char_gar_cnst_not_null",
        "description": "user34 (Garage Construction) should not be null if user33 (Garage Size) is not 7 (NONE)",
        "category": "missing_values",
        "condition": "user33 = '7' OR user34 IS NOT NULL",
        "additional_select_columns": ["user34", "user33"]
    },
    {
        "name": "iasworld_dweldat_char_gar_cnst_null_if_gar_size_is_null",
        "description": "user34 (Garage Construction) should be null or 0 if user33 (Garage Size) is 7 (NONE)",
        "category": "incorrect_values",
        "condition": "user33 != '7' OR user34 IS NULL OR user34 = '0'",
        "additional_select_columns": ["user34", "user33"]
    },
    {
        "name": "iasworld_dweldat_char_frpl_between_0_and_6",
        "description": "wbfp_o (Number of Fireplaces) should be between 0 and 6",
        "category": "incorrect_values",
        "condition": "wbfp_o IS NULL OR (wbfp_o >= 0 AND wbfp_o <= 6)",
        "additional_select_columns": ["wbfp_o"]
    },
    {
        "name": "iasworld_dweldat_yrblt_205_more_than_62_years_old",
        "description": "yrblt should be > 62 years old when class is 205",
        "category": "incorrect_values",
        "condition": "class != '205' OR yrblt IS NULL OR yrblt <= year(current_date) - 63",
        "additional_select_columns": ["yrblt"]
    },
    {
        "name": "iasworld_dweldat_yrblt_206_more_than_62_years_old",
        "description": "yrblt should be > 62 years old when class is 206",
        "category": "incorrect_values",
        "condition": "class != '206' OR yrblt IS NULL OR yrblt <= year(current_date) - 63",
        "additional_select_columns": ["yrblt"]
    },
    {
        "name": "iasworld_dweldat_yrblt_207_less_than_62_years_old",
        "description": "yrblt should be <= 62 years old when class is 207",
        "category": "incorrect_values",
        "condition": "class != '207' OR yrblt IS NULL OR yrblt >= year(current_date) - 62",
        "additional_select_columns": ["yrblt"]
    },
    {
        "name": "iasworld_dweldat_yrblt_208_less_than_62_years_old",
        "description": "yrblt should be <= 62 years old when class is 208",
        "category": "incorrect_values",
        "condition": "class != '208' OR yrblt IS NULL OR yrblt >= year(current_date) - 62",
        "additional_select_columns": ["yrblt"]
    },
    {
        "name": "iasworld_dweldat_yrblt_210_more_than_62_years_old",
        "description": "yrblt should be > 62 years old when class is 210",
        "category": "incorrect_values",
        "condition": "class != '210' OR yrblt IS NULL OR yrblt <= year(current_date) - 63",
        "additional_select_columns": ["yrblt"]
    },
    {
        "name": "iasworld_dweldat_yrblt_278_less_than_62_years_old",
        "description": "yrblt should be <= 62 years old when class is 278",
        "category": "incorrect_values",
        "condition": "class != '278' OR yrblt IS NULL OR yrblt >= year(current_date) - 62",
        "additional_select_columns": ["yrblt"]
    },
    {
        "name": "iasworld_dweldat_yrblt_295_less_than_62_years_old",
        "description": "yrblt should be <= 62 years old when class is 295",
        "category": "incorrect_values",
        "condition": "class != '295' OR yrblt IS NULL OR yrblt >= year(current_date) - 62",
        "additional_select_columns": ["yrblt"]
    },
    {
        "name": "iasworld_dweldat_yrblt_between_1850_and_now",
        "description": "yrblt should be between 1850 and now",
        "category": "incorrect_values",
        "condition": "yrblt IS NULL OR (yrblt >= 1850 AND yrblt <= year(current_date))",
        "additional_select_columns": ["yrblt"]
    },
    {
        "name": "iasworld_dweldat_yrblt_not_null",
        "description": "yrblt should not be null",
        "category": "missing_values",
        "condition": "yrblt IS NOT NULL"
    },
    {
        "name": "iasworld_dweldat_unique_by_parid_card_taxyr",
        "description": "dweldat should be unique by parid, card, and taxyr",
        "category": "duplicate_records",
        "condition": "num_duplicates = 1",
        "additional_select_columns": ["num_duplicates"]
    }
] -%}

{%- set base_query %}
    SELECT
        -- Identifying columns
        dweldat.parid,
        dweldat.taxyr,
        ARRAY[dweldat.card] AS card,
        CAST(NULL AS INTEGER) AS lline,
        legdat.user1 AS township_code,
        dweldat.class,
        dweldat.who,
        dweldat.wen,
        -- Columns to test
        dweldat.attic,
        dweldat.bsmt,
        dweldat.calc_meth,
        dweldat.cur,
        dweldat.external_calc_rcnld,
        dweldat.external_occpct,
        dweldat.external_propct,
        dweldat.external_rcnld,
        dweldat.extwall,
        dweldat.fixbath,
        dweldat.fixhalf,
        dweldat.heat,
        dweldat.mktadj,
        dweldat.mktrsn,
        dweldat.rmbed,
        dweldat.rmtot,
        dweldat.seq,
        dweldat.sfla,
        dweldat.stories,
        dweldat.user3,
        dweldat.user5,
        dweldat.user6,
        dweldat.user7,
        dweldat.user12,
        dweldat.user13,
        dweldat.user14,
        dweldat.user15,
        dweldat.user20,
        dweldat.user24,
        dweldat.user30,
        dweldat.user31,
        dweldat.user32,
        dweldat.user33,
        dweldat.user34,
        dweldat.wbfp_o,
        dweldat.yrblt,
        -- Computed columns for tests
        pardat.parid AS pardat_parid,
        pardat.class AS pardat_class,
        comdat.parid AS comdat_parid,
        class_dict.class_code AS class_dict_class,
        LAG(dweldat.seq)
            OVER (
                PARTITION BY dweldat.parid, dweldat.taxyr, dweldat.card
                ORDER BY dweldat.seq
            ) AS prev_seq,
        COUNT(*)
            OVER (PARTITION BY dweldat.parid, dweldat.taxyr, dweldat.card)
            AS num_duplicates
    FROM {{ source('iasworld', 'dweldat') }} AS dweldat
    LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
        ON dweldat.parid = legdat.parid
        AND dweldat.taxyr = legdat.taxyr
        AND legdat.cur = 'Y'
        AND legdat.deactivat IS NULL
    LEFT JOIN {{ source('iasworld', 'pardat') }} AS pardat
        ON dweldat.parid = pardat.parid
        AND dweldat.taxyr = pardat.taxyr
        AND pardat.cur = 'Y'
        AND pardat.deactivat IS NULL
    LEFT JOIN (
        SELECT DISTINCT parid, taxyr
        FROM {{ source('iasworld', 'comdat') }}
        WHERE cur = 'Y'
            AND deactivat IS NULL
    ) AS comdat
        ON dweldat.parid = comdat.parid
        AND dweldat.taxyr = comdat.taxyr
    LEFT JOIN {{ ref('ccao.class_dict') }} AS class_dict
        ON dweldat.class = class_dict.class_code
    WHERE dweldat.cur = 'Y'
        AND dweldat.deactivat IS NULL
        AND dweldat.class NOT IN (
            '201', '213', '218', '219', '220', '221', '224', '225',
            '236', '240', '241', '290', '294', '297'
        )
{% endset %}

{{ generate_iasworld_qc_test_view(base_query, tests) }}
