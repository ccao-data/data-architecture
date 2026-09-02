WITH qc_agg AS (
    SELECT
        COUNT(*) FILTER (WHERE iasworld_dweldat_attic_in_accepted_values) AS iasworld_dweldat_attic_in_accepted_values,
        COUNT(*) FILTER (WHERE iasworld_dweldat_attic_not_null) AS iasworld_dweldat_attic_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_bsmt_in_accepted_values) AS iasworld_dweldat_bsmt_in_accepted_values,
        COUNT(*) FILTER (WHERE iasworld_dweldat_bsmt_not_null) AS iasworld_dweldat_bsmt_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_bsmt_and_user12_match_234_class) AS iasworld_dweldat_bsmt_and_user12_match_234_class,
        COUNT(*) FILTER (WHERE iasworld_dweldat_calc_meth_accepted_values) AS iasworld_dweldat_calc_meth_accepted_values,
        COUNT(*) FILTER (WHERE iasworld_dweldat_card_gte_1) AS iasworld_dweldat_card_gte_1,
        COUNT(*) FILTER (WHERE iasworld_dweldat_card_not_null) AS iasworld_dweldat_card_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_card_proration_rate_between_0_and_100) AS iasworld_dweldat_card_proration_rate_between_0_and_100,
        COUNT(*) FILTER (WHERE iasworld_dweldat_class_in_ccao_class_dict) AS iasworld_dweldat_class_in_ccao_class_dict,
        COUNT(*) FILTER (WHERE iasworld_dweldat_class_matches_pardat_class) AS iasworld_dweldat_class_matches_pardat_class,
        COUNT(*) FILTER (WHERE iasworld_dweldat_exempt_classes_match_pardat_class) AS iasworld_dweldat_exempt_classes_match_pardat_class,
        COUNT(*) FILTER (WHERE iasworld_dweldat_cur_in_accepted_values) AS iasworld_dweldat_cur_in_accepted_values,
        COUNT(*) FILTER (WHERE iasworld_dweldat_external_calc_rcnld_not_0) AS iasworld_dweldat_external_calc_rcnld_not_0,
        COUNT(*) FILTER (WHERE iasworld_dweldat_external_occpct_not_0) AS iasworld_dweldat_external_occpct_not_0,
        COUNT(*) FILTER (WHERE iasworld_dweldat_external_occpct_not_null_when_mktrsn_eq_5_or_5b_and_mktadj_is_null) AS iasworld_dweldat_external_occpct_not_null_when_mktrsn_eq_5_or_5b_and_mktadj_is_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_external_propct_not_0) AS iasworld_dweldat_external_propct_not_0,
        COUNT(*) FILTER (WHERE iasworld_dweldat_external_rcnld_not_0) AS iasworld_dweldat_external_rcnld_not_0,
        COUNT(*) FILTER (WHERE iasworld_dweldat_extwall_in_accepted_values) AS iasworld_dweldat_extwall_in_accepted_values,
        COUNT(*) FILTER (WHERE iasworld_dweldat_extwall_not_null) AS iasworld_dweldat_extwall_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_fixbath_matches_number_of_units) AS iasworld_dweldat_fixbath_matches_number_of_units,
        COUNT(*) FILTER (WHERE iasworld_dweldat_fixbath_not_null) AS iasworld_dweldat_fixbath_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_fixhalf_gte_0) AS iasworld_dweldat_fixhalf_gte_0,
        COUNT(*) FILTER (WHERE iasworld_dweldat_fixhalf_matches_number_of_units) AS iasworld_dweldat_fixhalf_matches_number_of_units,
        COUNT(*) FILTER (WHERE iasworld_dweldat_heat_in_accepted_values) AS iasworld_dweldat_heat_in_accepted_values,
        COUNT(*) FILTER (WHERE iasworld_dweldat_heat_not_null) AS iasworld_dweldat_heat_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_mktadj_not_null_when_mktrsn_eq_5_or_5b_and_external_occpct_is_null) AS iasworld_dweldat_mktadj_not_null_when_mktrsn_eq_5_or_5b_and_external_occpct_is_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_mktrsn_eq_5_or_5b_when_external_occpct_or_mktadj_not_null) AS iasworld_dweldat_mktrsn_eq_5_or_5b_when_external_occpct_or_mktadj_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_parid_not_null) AS iasworld_dweldat_parid_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_parid_in_pardat_parid) AS iasworld_dweldat_parid_in_pardat_parid,
        COUNT(*) FILTER (WHERE iasworld_dweldat_rmbed_lte_rmtot) AS iasworld_dweldat_rmbed_lte_rmtot,
        COUNT(*) FILTER (WHERE iasworld_dweldat_rmbed_matches_number_of_units) AS iasworld_dweldat_rmbed_matches_number_of_units,
        COUNT(*) FILTER (WHERE iasworld_dweldat_rmbed_not_null) AS iasworld_dweldat_rmbed_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_rmtot_not_null) AS iasworld_dweldat_rmtot_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_rmtot_sf_between_1_and_40) AS iasworld_dweldat_rmtot_sf_between_1_and_40,
        COUNT(*) FILTER (WHERE iasworld_dweldat_rmtot_sf_between_1_and_50) AS iasworld_dweldat_rmtot_sf_between_1_and_50,
        COUNT(*) FILTER (WHERE iasworld_dweldat_seq_all_sequential_exist) AS iasworld_dweldat_seq_all_sequential_exist,
        COUNT(*) FILTER (WHERE iasworld_dweldat_sfla_between_1_and_999_for_class_202) AS iasworld_dweldat_sfla_between_1_and_999_for_class_202,
        COUNT(*) FILTER (WHERE iasworld_dweldat_sfla_between_1_and_2000_for_class_207) AS iasworld_dweldat_sfla_between_1_and_2000_for_class_207,
        COUNT(*) FILTER (WHERE iasworld_dweldat_sfla_between_1_and_2200_for_class_205) AS iasworld_dweldat_sfla_between_1_and_2200_for_class_205,
        COUNT(*) FILTER (WHERE iasworld_dweldat_sfla_between_1_and_10000_for_class_210) AS iasworld_dweldat_sfla_between_1_and_10000_for_class_210,
        COUNT(*) FILTER (WHERE iasworld_dweldat_sfla_between_1_and_10000_for_class_234) AS iasworld_dweldat_sfla_between_1_and_10000_for_class_234,
        COUNT(*) FILTER (WHERE iasworld_dweldat_sfla_between_1_and_20000_for_class_212) AS iasworld_dweldat_sfla_between_1_and_20000_for_class_212,
        COUNT(*) FILTER (WHERE iasworld_dweldat_sfla_between_1_and_20000_for_class_295) AS iasworld_dweldat_sfla_between_1_and_20000_for_class_295,
        COUNT(*) FILTER (WHERE iasworld_dweldat_sfla_between_1_and_40000_for_class_211) AS iasworld_dweldat_sfla_between_1_and_40000_for_class_211,
        COUNT(*) FILTER (WHERE iasworld_dweldat_sfla_between_1000_and_1800_for_class_203) AS iasworld_dweldat_sfla_between_1000_and_1800_for_class_203,
        COUNT(*) FILTER (WHERE iasworld_dweldat_sfla_between_1801_and_25000_for_class_204) AS iasworld_dweldat_sfla_between_1801_and_25000_for_class_204,
        COUNT(*) FILTER (WHERE iasworld_dweldat_sfla_between_2001_and_3800_for_class_278) AS iasworld_dweldat_sfla_between_2001_and_3800_for_class_278,
        COUNT(*) FILTER (WHERE iasworld_dweldat_sfla_between_2201_and_4999_for_class_206) AS iasworld_dweldat_sfla_between_2201_and_4999_for_class_206,
        COUNT(*) FILTER (WHERE iasworld_dweldat_sfla_between_3801_and_4999_for_class_208) AS iasworld_dweldat_sfla_between_3801_and_4999_for_class_208,
        COUNT(*) FILTER (WHERE iasworld_dweldat_sfla_between_5000_and_50000_for_class_209) AS iasworld_dweldat_sfla_between_5000_and_50000_for_class_209,
        COUNT(*) FILTER (WHERE iasworld_dweldat_sfla_not_null) AS iasworld_dweldat_sfla_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_stories_in_accepted_values) AS iasworld_dweldat_stories_in_accepted_values,
        COUNT(*) FILTER (WHERE iasworld_dweldat_stories_not_null) AS iasworld_dweldat_stories_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_taxyr_not_null) AS iasworld_dweldat_taxyr_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_renovation_in_accepted_values) AS iasworld_dweldat_char_renovation_in_accepted_values,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_tp_plan_in_accepted_values) AS iasworld_dweldat_char_tp_plan_in_accepted_values,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_tp_plan_not_null) AS iasworld_dweldat_char_tp_plan_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_attic_fnsh_in_accepted_values) AS iasworld_dweldat_char_attic_fnsh_in_accepted_values,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_attic_fnsh_not_null) AS iasworld_dweldat_char_attic_fnsh_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_air_accepted_values) AS iasworld_dweldat_char_air_accepted_values,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_air_not_null) AS iasworld_dweldat_char_air_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_bsmt_fin_accepted_values) AS iasworld_dweldat_char_bsmt_fin_accepted_values,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_bsmt_fin_not_null) AS iasworld_dweldat_char_bsmt_fin_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_roof_cnst_accepted_values) AS iasworld_dweldat_char_roof_cnst_accepted_values,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_roof_cnst_not_null) AS iasworld_dweldat_char_roof_cnst_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_apts_accepted_values) AS iasworld_dweldat_char_apts_accepted_values,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_apts_not_null) AS iasworld_dweldat_char_apts_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_use_accepted_values) AS iasworld_dweldat_char_use_accepted_values,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_use_not_null) AS iasworld_dweldat_char_use_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_ncu_between_0_and_5) AS iasworld_dweldat_char_ncu_between_0_and_5,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_ncu_not_0_when_class_is_212) AS iasworld_dweldat_char_ncu_not_0_when_class_is_212,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_ncu_not_null_when_class_is_212) AS iasworld_dweldat_char_ncu_not_null_when_class_is_212,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_ncu_null_when_class_is_not_212_and_char_ncu_not_0) AS iasworld_dweldat_char_ncu_null_when_class_is_not_212_and_char_ncu_not_0,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_porch_accepted_values) AS iasworld_dweldat_char_porch_accepted_values,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_gar_att_accepted_values) AS iasworld_dweldat_char_gar_att_accepted_values,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_gar_att_not_null) AS iasworld_dweldat_char_gar_att_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_gar_area_accepted_values) AS iasworld_dweldat_char_gar_area_accepted_values,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_gar_area_not_null) AS iasworld_dweldat_char_gar_area_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_gar_size_accepted_values) AS iasworld_dweldat_char_gar_size_accepted_values,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_gar_size_not_null) AS iasworld_dweldat_char_gar_size_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_gar_cnst_accepted_values) AS iasworld_dweldat_char_gar_cnst_accepted_values,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_gar_cnst_not_null) AS iasworld_dweldat_char_gar_cnst_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_gar_cnst_null_if_gar_size_is_null) AS iasworld_dweldat_char_gar_cnst_null_if_gar_size_is_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_char_frpl_between_0_and_6) AS iasworld_dweldat_char_frpl_between_0_and_6,
        COUNT(*) FILTER (WHERE iasworld_dweldat_yrblt_205_more_than_62_years_old) AS iasworld_dweldat_yrblt_205_more_than_62_years_old,
        COUNT(*) FILTER (WHERE iasworld_dweldat_yrblt_206_more_than_62_years_old) AS iasworld_dweldat_yrblt_206_more_than_62_years_old,
        COUNT(*) FILTER (WHERE iasworld_dweldat_yrblt_207_less_than_62_years_old) AS iasworld_dweldat_yrblt_207_less_than_62_years_old,
        COUNT(*) FILTER (WHERE iasworld_dweldat_yrblt_208_less_than_62_years_old) AS iasworld_dweldat_yrblt_208_less_than_62_years_old,
        COUNT(*) FILTER (WHERE iasworld_dweldat_yrblt_210_more_than_62_years_old) AS iasworld_dweldat_yrblt_210_more_than_62_years_old,
        COUNT(*) FILTER (WHERE iasworld_dweldat_yrblt_278_less_than_62_years_old) AS iasworld_dweldat_yrblt_278_less_than_62_years_old,
        COUNT(*) FILTER (WHERE iasworld_dweldat_yrblt_295_less_than_62_years_old) AS iasworld_dweldat_yrblt_295_less_than_62_years_old,
        COUNT(*) FILTER (WHERE iasworld_dweldat_yrblt_between_1850_and_now) AS iasworld_dweldat_yrblt_between_1850_and_now,
        COUNT(*) FILTER (WHERE iasworld_dweldat_yrblt_not_null) AS iasworld_dweldat_yrblt_not_null,
        COUNT(*) FILTER (WHERE iasworld_dweldat_unique_by_parid_card_taxyr) AS iasworld_dweldat_unique_by_parid_card_taxyr
    FROM z_dev_damajor_qc.vw_report_iasworld_test_dweldat
),
qc_unpivoted AS (
    SELECT 'iasworld_dweldat_attic_in_accepted_values' AS test_name, iasworld_dweldat_attic_in_accepted_values AS qc_count FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_attic_not_null', iasworld_dweldat_attic_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_bsmt_in_accepted_values', iasworld_dweldat_bsmt_in_accepted_values FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_bsmt_not_null', iasworld_dweldat_bsmt_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_bsmt_and_user12_match_234_class', iasworld_dweldat_bsmt_and_user12_match_234_class FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_calc_meth_accepted_values', iasworld_dweldat_calc_meth_accepted_values FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_card_gte_1', iasworld_dweldat_card_gte_1 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_card_not_null', iasworld_dweldat_card_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_card_proration_rate_between_0_and_100', iasworld_dweldat_card_proration_rate_between_0_and_100 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_class_in_ccao_class_dict', iasworld_dweldat_class_in_ccao_class_dict FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_class_matches_pardat_class', iasworld_dweldat_class_matches_pardat_class FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_exempt_classes_match_pardat_class', iasworld_dweldat_exempt_classes_match_pardat_class FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_cur_in_accepted_values', iasworld_dweldat_cur_in_accepted_values FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_external_calc_rcnld_not_0', iasworld_dweldat_external_calc_rcnld_not_0 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_external_occpct_not_0', iasworld_dweldat_external_occpct_not_0 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_external_occpct_not_null_when_mktrsn_eq_5_or_5b_and_mktadj_is_null', iasworld_dweldat_external_occpct_not_null_when_mktrsn_eq_5_or_5b_and_mktadj_is_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_external_propct_not_0', iasworld_dweldat_external_propct_not_0 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_external_rcnld_not_0', iasworld_dweldat_external_rcnld_not_0 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_extwall_in_accepted_values', iasworld_dweldat_extwall_in_accepted_values FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_extwall_not_null', iasworld_dweldat_extwall_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_fixbath_matches_number_of_units', iasworld_dweldat_fixbath_matches_number_of_units FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_fixbath_not_null', iasworld_dweldat_fixbath_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_fixhalf_gte_0', iasworld_dweldat_fixhalf_gte_0 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_fixhalf_matches_number_of_units', iasworld_dweldat_fixhalf_matches_number_of_units FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_heat_in_accepted_values', iasworld_dweldat_heat_in_accepted_values FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_heat_not_null', iasworld_dweldat_heat_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_mktadj_not_null_when_mktrsn_eq_5_or_5b_and_external_occpct_is_null', iasworld_dweldat_mktadj_not_null_when_mktrsn_eq_5_or_5b_and_external_occpct_is_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_mktrsn_eq_5_or_5b_when_external_occpct_or_mktadj_not_null', iasworld_dweldat_mktrsn_eq_5_or_5b_when_external_occpct_or_mktadj_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_parid_not_null', iasworld_dweldat_parid_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_parid_in_pardat_parid', iasworld_dweldat_parid_in_pardat_parid FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_rmbed_lte_rmtot', iasworld_dweldat_rmbed_lte_rmtot FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_rmbed_matches_number_of_units', iasworld_dweldat_rmbed_matches_number_of_units FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_rmbed_not_null', iasworld_dweldat_rmbed_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_rmtot_not_null', iasworld_dweldat_rmtot_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_rmtot_sf_between_1_and_40', iasworld_dweldat_rmtot_sf_between_1_and_40 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_rmtot_sf_between_1_and_50', iasworld_dweldat_rmtot_sf_between_1_and_50 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_seq_all_sequential_exist', iasworld_dweldat_seq_all_sequential_exist FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_1_and_999_for_class_202', iasworld_dweldat_sfla_between_1_and_999_for_class_202 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_1_and_2000_for_class_207', iasworld_dweldat_sfla_between_1_and_2000_for_class_207 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_1_and_2200_for_class_205', iasworld_dweldat_sfla_between_1_and_2200_for_class_205 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_1_and_10000_for_class_210', iasworld_dweldat_sfla_between_1_and_10000_for_class_210 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_1_and_10000_for_class_234', iasworld_dweldat_sfla_between_1_and_10000_for_class_234 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_1_and_20000_for_class_212', iasworld_dweldat_sfla_between_1_and_20000_for_class_212 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_1_and_20000_for_class_295', iasworld_dweldat_sfla_between_1_and_20000_for_class_295 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_1_and_40000_for_class_211', iasworld_dweldat_sfla_between_1_and_40000_for_class_211 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_1000_and_1800_for_class_203', iasworld_dweldat_sfla_between_1000_and_1800_for_class_203 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_1801_and_25000_for_class_204', iasworld_dweldat_sfla_between_1801_and_25000_for_class_204 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_2001_and_3800_for_class_278', iasworld_dweldat_sfla_between_2001_and_3800_for_class_278 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_2201_and_4999_for_class_206', iasworld_dweldat_sfla_between_2201_and_4999_for_class_206 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_3801_and_4999_for_class_208', iasworld_dweldat_sfla_between_3801_and_4999_for_class_208 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_5000_and_50000_for_class_209', iasworld_dweldat_sfla_between_5000_and_50000_for_class_209 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_sfla_not_null', iasworld_dweldat_sfla_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_stories_in_accepted_values', iasworld_dweldat_stories_in_accepted_values FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_stories_not_null', iasworld_dweldat_stories_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_taxyr_not_null', iasworld_dweldat_taxyr_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_renovation_in_accepted_values', iasworld_dweldat_char_renovation_in_accepted_values FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_tp_plan_in_accepted_values', iasworld_dweldat_char_tp_plan_in_accepted_values FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_tp_plan_not_null', iasworld_dweldat_char_tp_plan_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_attic_fnsh_in_accepted_values', iasworld_dweldat_char_attic_fnsh_in_accepted_values FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_attic_fnsh_not_null', iasworld_dweldat_char_attic_fnsh_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_air_accepted_values', iasworld_dweldat_char_air_accepted_values FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_air_not_null', iasworld_dweldat_char_air_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_bsmt_fin_accepted_values', iasworld_dweldat_char_bsmt_fin_accepted_values FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_bsmt_fin_not_null', iasworld_dweldat_char_bsmt_fin_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_roof_cnst_accepted_values', iasworld_dweldat_char_roof_cnst_accepted_values FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_roof_cnst_not_null', iasworld_dweldat_char_roof_cnst_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_apts_accepted_values', iasworld_dweldat_char_apts_accepted_values FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_apts_not_null', iasworld_dweldat_char_apts_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_use_accepted_values', iasworld_dweldat_char_use_accepted_values FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_use_not_null', iasworld_dweldat_char_use_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_ncu_between_0_and_5', iasworld_dweldat_char_ncu_between_0_and_5 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_ncu_not_0_when_class_is_212', iasworld_dweldat_char_ncu_not_0_when_class_is_212 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_ncu_not_null_when_class_is_212', iasworld_dweldat_char_ncu_not_null_when_class_is_212 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_ncu_null_when_class_is_not_212_and_char_ncu_not_0', iasworld_dweldat_char_ncu_null_when_class_is_not_212_and_char_ncu_not_0 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_porch_accepted_values', iasworld_dweldat_char_porch_accepted_values FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_gar_att_accepted_values', iasworld_dweldat_char_gar_att_accepted_values FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_gar_att_not_null', iasworld_dweldat_char_gar_att_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_gar_area_accepted_values', iasworld_dweldat_char_gar_area_accepted_values FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_gar_area_not_null', iasworld_dweldat_char_gar_area_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_gar_size_accepted_values', iasworld_dweldat_char_gar_size_accepted_values FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_gar_size_not_null', iasworld_dweldat_char_gar_size_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_gar_cnst_accepted_values', iasworld_dweldat_char_gar_cnst_accepted_values FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_gar_cnst_not_null', iasworld_dweldat_char_gar_cnst_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_gar_cnst_null_if_gar_size_is_null', iasworld_dweldat_char_gar_cnst_null_if_gar_size_is_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_char_frpl_between_0_and_6', iasworld_dweldat_char_frpl_between_0_and_6 FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_yrblt_205_more_than_62_years_old', iasworld_dweldat_yrblt_205_more_than_62_years_old FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_yrblt_206_more_than_62_years_old', iasworld_dweldat_yrblt_206_more_than_62_years_old FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_yrblt_207_less_than_62_years_old', iasworld_dweldat_yrblt_207_less_than_62_years_old FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_yrblt_208_less_than_62_years_old', iasworld_dweldat_yrblt_208_less_than_62_years_old FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_yrblt_210_more_than_62_years_old', iasworld_dweldat_yrblt_210_more_than_62_years_old FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_yrblt_278_less_than_62_years_old', iasworld_dweldat_yrblt_278_less_than_62_years_old FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_yrblt_295_less_than_62_years_old', iasworld_dweldat_yrblt_295_less_than_62_years_old FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_yrblt_between_1850_and_now', iasworld_dweldat_yrblt_between_1850_and_now FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_yrblt_not_null', iasworld_dweldat_yrblt_not_null FROM qc_agg
    UNION ALL SELECT 'iasworld_dweldat_unique_by_parid_card_taxyr', iasworld_dweldat_unique_by_parid_card_taxyr FROM qc_agg
),
tf_counts AS (
    SELECT 'iasworld_dweldat_attic_in_accepted_values' AS test_name, COUNT(*) AS tf_count FROM z_dev_damajor_test_failure.iasworld_dweldat_attic_in_accepted_values
    UNION ALL SELECT 'iasworld_dweldat_attic_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_attic_not_null
    UNION ALL SELECT 'iasworld_dweldat_bsmt_in_accepted_values', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_bsmt_in_accepted_values
    UNION ALL SELECT 'iasworld_dweldat_bsmt_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_bsmt_not_null
    UNION ALL SELECT 'iasworld_dweldat_bsmt_and_user12_match_234_class', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_bsmt_and_user12_match_234_class
    UNION ALL SELECT 'iasworld_dweldat_calc_meth_accepted_values', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_calc_meth_accepted_values
    UNION ALL SELECT 'iasworld_dweldat_card_gte_1', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_card_gte_1
    UNION ALL SELECT 'iasworld_dweldat_card_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_card_not_null
    UNION ALL SELECT 'iasworld_dweldat_card_proration_rate_between_0_and_100', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_card_proration_rate_between_0_and_100
    UNION ALL SELECT 'iasworld_dweldat_class_in_ccao_class_dict', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_class_in_ccao_class_dict
    UNION ALL SELECT 'iasworld_dweldat_class_matches_pardat_class', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_class_matches_pardat_class
    UNION ALL SELECT 'iasworld_dweldat_exempt_classes_match_pardat_class', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_exempt_classes_match_pardat_class
    UNION ALL SELECT 'iasworld_dweldat_cur_in_accepted_values', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_cur_in_accepted_values
    UNION ALL SELECT 'iasworld_dweldat_external_calc_rcnld_not_0', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_external_calc_rcnld_not_0
    UNION ALL SELECT 'iasworld_dweldat_external_occpct_not_0', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_external_occpct_not_0
    UNION ALL SELECT 'iasworld_dweldat_external_occpct_not_null_when_mktrsn_eq_5_or_5b_and_mktadj_is_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_external_occpct_not_null_when_mktrsn_eq_5_or_5b_and_mktadj_is_null
    UNION ALL SELECT 'iasworld_dweldat_external_propct_not_0', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_external_propct_not_0
    UNION ALL SELECT 'iasworld_dweldat_external_rcnld_not_0', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_external_rcnld_not_0
    UNION ALL SELECT 'iasworld_dweldat_extwall_in_accepted_values', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_extwall_in_accepted_values
    UNION ALL SELECT 'iasworld_dweldat_extwall_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_extwall_not_null
    UNION ALL SELECT 'iasworld_dweldat_fixbath_matches_number_of_units', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_fixbath_matches_number_of_units
    UNION ALL SELECT 'iasworld_dweldat_fixbath_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_fixbath_not_null
    UNION ALL SELECT 'iasworld_dweldat_fixhalf_gte_0', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_fixhalf_gte_0
    UNION ALL SELECT 'iasworld_dweldat_fixhalf_matches_number_of_units', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_fixhalf_matches_number_of_units
    UNION ALL SELECT 'iasworld_dweldat_heat_in_accepted_values', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_heat_in_accepted_values
    UNION ALL SELECT 'iasworld_dweldat_heat_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_heat_not_null
    UNION ALL SELECT 'iasworld_dweldat_mktadj_not_null_when_mktrsn_eq_5_or_5b_and_external_occpct_is_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_mktadj_not_null_when_mktrsn_eq_5_or_5b_and_external_occpct_is_null
    UNION ALL SELECT 'iasworld_dweldat_mktrsn_eq_5_or_5b_when_external_occpct_or_mktadj_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_mktrsn_eq_5_or_5b_when_external_occpct_or_mktadj_not_null
    UNION ALL SELECT 'iasworld_dweldat_parid_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_parid_not_null
    UNION ALL SELECT 'iasworld_dweldat_parid_in_pardat_parid', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_parid_in_pardat_parid
    UNION ALL SELECT 'iasworld_dweldat_rmbed_lte_rmtot', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_rmbed_lte_rmtot
    UNION ALL SELECT 'iasworld_dweldat_rmbed_matches_number_of_units', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_rmbed_matches_number_of_units
    UNION ALL SELECT 'iasworld_dweldat_rmbed_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_rmbed_not_null
    UNION ALL SELECT 'iasworld_dweldat_rmtot_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_rmtot_not_null
    UNION ALL SELECT 'iasworld_dweldat_rmtot_sf_between_1_and_40', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_rmtot_sf_between_1_and_40
    UNION ALL SELECT 'iasworld_dweldat_rmtot_sf_between_1_and_50', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_rmtot_sf_between_1_and_50
    UNION ALL SELECT 'iasworld_dweldat_seq_all_sequential_exist', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_seq_all_sequential_exist
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_1_and_999_for_class_202', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_sfla_between_1_and_999_for_class_202
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_1_and_2000_for_class_207', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_sfla_between_1_and_2000_for_class_207
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_1_and_2200_for_class_205', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_sfla_between_1_and_2200_for_class_205
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_1_and_10000_for_class_210', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_sfla_between_1_and_10000_for_class_210
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_1_and_10000_for_class_234', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_sfla_between_1_and_10000_for_class_234
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_1_and_20000_for_class_212', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_sfla_between_1_and_20000_for_class_212
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_1_and_20000_for_class_295', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_sfla_between_1_and_20000_for_class_295
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_1_and_40000_for_class_211', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_sfla_between_1_and_40000_for_class_211
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_1000_and_1800_for_class_203', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_sfla_between_1000_and_1800_for_class_203
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_1801_and_25000_for_class_204', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_sfla_between_1801_and_25000_for_class_204
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_2001_and_3800_for_class_278', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_sfla_between_2001_and_3800_for_class_278
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_2201_and_4999_for_class_206', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_sfla_between_2201_and_4999_for_class_206
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_3801_and_4999_for_class_208', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_sfla_between_3801_and_4999_for_class_208
    UNION ALL SELECT 'iasworld_dweldat_sfla_between_5000_and_50000_for_class_209', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_sfla_between_5000_and_50000_for_class_209
    UNION ALL SELECT 'iasworld_dweldat_sfla_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_sfla_not_null
    UNION ALL SELECT 'iasworld_dweldat_stories_in_accepted_values', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_stories_in_accepted_values
    UNION ALL SELECT 'iasworld_dweldat_stories_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_stories_not_null
    UNION ALL SELECT 'iasworld_dweldat_taxyr_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_taxyr_not_null
    UNION ALL SELECT 'iasworld_dweldat_char_renovation_in_accepted_values', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_renovation_in_accepted_values
    UNION ALL SELECT 'iasworld_dweldat_char_tp_plan_in_accepted_values', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_tp_plan_in_accepted_values
    UNION ALL SELECT 'iasworld_dweldat_char_tp_plan_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_tp_plan_not_null
    UNION ALL SELECT 'iasworld_dweldat_char_attic_fnsh_in_accepted_values', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_attic_fnsh_in_accepted_values
    UNION ALL SELECT 'iasworld_dweldat_char_attic_fnsh_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_attic_fnsh_not_null
    UNION ALL SELECT 'iasworld_dweldat_char_air_accepted_values', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_air_accepted_values
    UNION ALL SELECT 'iasworld_dweldat_char_air_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_air_not_null
    UNION ALL SELECT 'iasworld_dweldat_char_bsmt_fin_accepted_values', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_bsmt_fin_accepted_values
    UNION ALL SELECT 'iasworld_dweldat_char_bsmt_fin_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_bsmt_fin_not_null
    UNION ALL SELECT 'iasworld_dweldat_char_roof_cnst_accepted_values', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_roof_cnst_accepted_values
    UNION ALL SELECT 'iasworld_dweldat_char_roof_cnst_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_roof_cnst_not_null
    UNION ALL SELECT 'iasworld_dweldat_char_apts_accepted_values', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_apts_accepted_values
    UNION ALL SELECT 'iasworld_dweldat_char_apts_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_apts_not_null
    UNION ALL SELECT 'iasworld_dweldat_char_use_accepted_values', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_use_accepted_values
    UNION ALL SELECT 'iasworld_dweldat_char_use_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_use_not_null
    UNION ALL SELECT 'iasworld_dweldat_char_ncu_between_0_and_5', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_ncu_between_0_and_5
    UNION ALL SELECT 'iasworld_dweldat_char_ncu_not_0_when_class_is_212', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_ncu_not_0_when_class_is_212
    UNION ALL SELECT 'iasworld_dweldat_char_ncu_not_null_when_class_is_212', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_ncu_not_null_when_class_is_212
    UNION ALL SELECT 'iasworld_dweldat_char_ncu_null_when_class_is_not_212_and_char_ncu_not_0', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_ncu_null_when_class_is_not_212_and_char_ncu_not_0
    UNION ALL SELECT 'iasworld_dweldat_char_porch_accepted_values', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_porch_accepted_values
    UNION ALL SELECT 'iasworld_dweldat_char_gar_att_accepted_values', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_gar_att_accepted_values
    UNION ALL SELECT 'iasworld_dweldat_char_gar_att_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_gar_att_not_null
    UNION ALL SELECT 'iasworld_dweldat_char_gar_area_accepted_values', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_gar_area_accepted_values
    UNION ALL SELECT 'iasworld_dweldat_char_gar_area_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_gar_area_not_null
    UNION ALL SELECT 'iasworld_dweldat_char_gar_size_accepted_values', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_gar_size_accepted_values
    UNION ALL SELECT 'iasworld_dweldat_char_gar_size_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_gar_size_not_null
    UNION ALL SELECT 'iasworld_dweldat_char_gar_cnst_accepted_values', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_gar_cnst_accepted_values
    UNION ALL SELECT 'iasworld_dweldat_char_gar_cnst_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_gar_cnst_not_null
    UNION ALL SELECT 'iasworld_dweldat_char_gar_cnst_null_if_gar_size_is_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_gar_cnst_null_if_gar_size_is_null
    UNION ALL SELECT 'iasworld_dweldat_char_frpl_between_0_and_6', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_char_frpl_between_0_and_6
    UNION ALL SELECT 'iasworld_dweldat_yrblt_205_more_than_62_years_old', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_yrblt_205_more_than_62_years_old
    UNION ALL SELECT 'iasworld_dweldat_yrblt_206_more_than_62_years_old', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_yrblt_206_more_than_62_years_old
    UNION ALL SELECT 'iasworld_dweldat_yrblt_207_less_than_62_years_old', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_yrblt_207_less_than_62_years_old
    UNION ALL SELECT 'iasworld_dweldat_yrblt_208_less_than_62_years_old', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_yrblt_208_less_than_62_years_old
    UNION ALL SELECT 'iasworld_dweldat_yrblt_210_more_than_62_years_old', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_yrblt_210_more_than_62_years_old
    UNION ALL SELECT 'iasworld_dweldat_yrblt_278_less_than_62_years_old', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_yrblt_278_less_than_62_years_old
    UNION ALL SELECT 'iasworld_dweldat_yrblt_295_less_than_62_years_old', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_yrblt_295_less_than_62_years_old
    UNION ALL SELECT 'iasworld_dweldat_yrblt_between_1850_and_now', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_yrblt_between_1850_and_now
    UNION ALL SELECT 'iasworld_dweldat_yrblt_not_null', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_yrblt_not_null
    UNION ALL SELECT 'iasworld_dweldat_unique_by_parid_card_taxyr', COUNT(*) FROM z_dev_damajor_test_failure.iasworld_dweldat_unique_by_parid_card_taxyr
)
SELECT
    q.test_name,
    q.qc_count,
    t.tf_count,
    q.qc_count - t.tf_count AS diff
FROM qc_unpivoted AS q
LEFT JOIN tf_counts AS t ON q.test_name = t.test_name
ORDER BY ABS(q.qc_count - t.tf_count) DESC
