library(arrow)
library(aws.s3)
library(dplyr)
library(purrr)
library(readr)
library(snakecase)
library(stringr)
library(tidyr)
source("utils.R")

AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "ccao", "legacy")


##### CC_DLI_SENFRR #####
files_cc_dli_senfrr <- aws.s3::get_bucket_df(
  bucket = AWS_S3_RAW_BUCKET,
  prefix = "ccao/legacy/CC_DLI_SENFRR",
  max = Inf
) %>%
  filter(Size > 0)

# Read the files into a single tibble. NOTE: these files are pipe-delimited
# and have been MANUALLY CLEANED to remove some errant pipes that were in the
# address fields. These extra pipes prevented proper reading with read_delim.
# They were cleaned by searching for pipes between fixed column-wise character
# positions and replacing them with the intended character.
# Can't be read as a FWF like the ones below because some rows have extra chars
cc_dli_senfrr <- map_dfr(files_cc_dli_senfrr$Key, \(f) {
  aws.s3::s3read_using(
    object = f,
    bucket = AWS_S3_RAW_BUCKET,
    FUN = readr::read_delim,
    delim = "|",
    trim_ws = TRUE,
    col_names = c(
      "pin", "year", "tax_year", "tax_type", "rec_code",
      "base_value_year_manual_calculation_ind", "base_value_year",
      "base_value_year_total_eav", "cur_year_total_eav", "cur_year_final_eav",
      "birth_date", "applicant_old_name", "applicant_address", "applicant_city",
      "applicant_state", "applicant_zip", "applicant_status",
      "first_app_received_date", "qualified_date",
      "homeowner_base_year_eq_factor", "bldg_units", "building_shares",
      "base_value_year_class", "num_units_w_homeowner_exemption",
      "num_units_w_homestead_exemption", "num_shares_with_senior_freeze",
      "coop_senior_shares", "pct_senior_shares",
      "num_shares", "pct_of_shares", "sf_percent"
    ),
    col_types = cols(
      pin = col_character(),
      year = col_character(),
      tax_year = col_character(),
      tax_type = col_character(),
      rec_code = col_character(),
      base_value_year_manual_calculation_ind = col_character(),
      base_value_year = col_character(),
      base_value_year_total_eav = col_integer(),
      cur_year_total_eav = col_integer(),
      cur_year_final_eav = col_integer(),
      birth_date = col_character(),
      applicant_old_name = col_character(),
      applicant_address = col_character(),
      applicant_city = col_character(),
      applicant_state = col_character(),
      applicant_zip = col_character(),
      applicant_status = col_character(),
      first_app_received_date = col_character(),
      qualified_date = col_character(),
      homeowner_base_year_eq_factor = col_double(),
      bldg_units = col_integer(),
      building_shares = col_integer(),
      base_value_year_class = col_character(),
      num_units_w_homeowner_exemption = col_integer(),
      num_units_w_homestead_exemption = col_integer(),
      num_shares_with_senior_freeze = col_integer(),
      coop_senior_shares = col_integer(),
      pct_senior_shares = col_integer(),
      num_shares = col_integer(),
      pct_of_shares = col_integer(),
      sf_percent = col_integer()
    )
  ) %>%
    mutate(
      across(
        c(year, tax_year),
        \(x) ifelse(substr(x, 1, 1) == "9", paste0("19", x), paste0("20", x))
      ),
      across(starts_with("applicant_"), \(x) str_trim(str_squish(x))),
      applicant_zip = str_sub(applicant_zip, 1, 5),
      birth_date = lubridate::mdy(birth_date),
      first_app_received_date = lubridate::ymd(first_app_received_date),
      base_value_year_class = str_sub(base_value_year_class, 4, 6),
      qualified_date = lubridate::ymd(qualified_date),
      source_file = {{ f }}
    ) %>%
    select(-X32)
})

# Write the files to S3, partitioned by year
cc_dli_senfrr %>%
  mutate(loaded_at = as.character(Sys.time())) %>%
  group_by(year) %>%
  arrow::write_dataset(
    path = file.path(output_bucket, "cc_dli_senfrr"),
    format = "parquet",
    hive_style = TRUE,
    compression = "zstd"
  )


##### CC_PIFDB_PIEXEMPTRE_STED #####
files_cc_pifdb_piexemptre_sted <- aws.s3::get_bucket_df(
  bucket = AWS_S3_RAW_BUCKET,
  prefix = "ccao/legacy/CC_PIFDB_PIEXEMPTRE_STED",
  max = Inf
) %>%
  filter(Size > 0)

cc_pifdb_piexemptre_sted <- map_dfr(files_cc_pifdb_piexemptre_sted$Key, \(f) {
  aws.s3::s3read_using(
    object = f,
    bucket = AWS_S3_RAW_BUCKET,
    FUN = readr::read_fwf,
    trim_ws = TRUE,
    col_positions = readr::fwf_cols(
      pin = c(1, 14),
      year = c(16, 17),
      tax_year = c(19, 20),
      tax_type = c(22, 22),
      segment_code = c(24, 24),
      coop_status = c(26, 26),
      birth_date = c(28, 35),
      response = c(37, 37),
      print_indicator = c(39, 39),
      year_applied = c(41, 45),
      applicant_name = c(47, 68),
      applicant_address = c(70, 91),
      applicant_city = c(93, 104),
      applicant_state = c(106, 107),
      applicant_zip = c(109, 117),
      batch_number = c(119, 123),
      maintenance_date = c(125, 133),
      filler = c(135, 235)
    ),
    col_types = cols(
      pin = col_character(),
      year = col_character(),
      tax_year = col_character(),
      tax_type = col_character(),
      segment_code = col_character(),
      coop_status = col_character(),
      birth_date = col_character(),
      response = col_character(),
      print_indicator = col_character(),
      year_applied = col_character(),
      applicant_name = col_character(),
      applicant_address = col_character(),
      applicant_city = col_character(),
      applicant_state = col_character(),
      applicant_zip = col_character(),
      batch_number = col_integer(),
      maintenance_date = col_character(),
      filler = col_character()
    )
  ) %>%
    mutate(
      across(
        c(year, tax_year),
        \(x) ifelse(substr(x, 1, 1) == "9", paste0("19", x), paste0("20", x))
      ),
      across(starts_with("applicant_"), \(x) str_trim(str_squish(x))),
      year_applied = str_sub(year_applied, 2, 5),
      applicant_zip = str_sub(applicant_zip, 5, 9),
      birth_date = lubridate::mdy(birth_date),
      maintenance_date = na_if(maintenance_date, "000000000"),
      source_file = {{ f }}
    ) %>%
    select(-filler)
})

# Write the files to S3, partitioned by year
cc_pifdb_piexemptre_sted %>%
  mutate(loaded_at = as.character(Sys.time())) %>%
  group_by(year) %>%
  arrow::write_dataset(
    path = file.path(
      output_bucket, "cc_pifdb_piexemptre_sted"
    ),
    format = "parquet",
    hive_style = TRUE,
    compression = "zstd"
  )


##### CC_PIFDB_PIEXEMPTRE_DISE #####
files_cc_pifdb_piexemptre_dise <- aws.s3::get_bucket_df(
  bucket = AWS_S3_RAW_BUCKET,
  prefix = "ccao/legacy/CC_PIFDB_PIEXEMPTRE_DISE",
  max = Inf
) %>%
  filter(Size > 0)

cc_pifdb_piexemptre_dise <- map_dfr(files_cc_pifdb_piexemptre_dise$Key, \(f) {
  aws.s3::s3read_using(
    object = f,
    bucket = AWS_S3_RAW_BUCKET,
    FUN = readr::read_fwf,
    trim_ws = TRUE,
    col_positions = readr::fwf_cols(
      pin = c(1, 14),
      year = c(16, 17),
      tax_year = c(19, 20),
      tax_type = c(22, 22),
      segment_code = c(24, 24),
      exemption_amount_indicator = c(26, 26),
      returning_vet_value = c(28, 38),
      disabled_person_value = c(40, 50),
      disabled_vet_gte_30 = c(52, 62),
      disabled_vet_gte_50 = c(64, 74),
      batch_number = c(76, 80),
      used_id = c(82, 88),
      date_entered = c(90, 96),
      disabled_vet_gte_70 = c(98, 108),
      filler = c(110, 235)
    ),
    col_types = cols(
      pin = col_character(),
      year = col_character(),
      tax_year = col_character(),
      tax_type = col_character(),
      segment_code = col_character(),
      exemption_amount_indicator = col_character(),
      returning_vet_value = col_integer(),
      disabled_person_value = col_integer(),
      disabled_vet_gte_30 = col_integer(),
      disabled_vet_gte_50 = col_integer(),
      batch_number = col_integer(),
      used_id = col_integer(),
      date_entered = col_character(),
      disabled_vet_gte_70 = col_integer(),
      filler = col_character()
    )
  ) %>%
    mutate(
      across(
        c(year, tax_year),
        \(x) ifelse(substr(x, 1, 1) == "9", paste0("19", x), paste0("20", x))
      ),
      date_entered = lubridate::ymd(paste0("2", date_entered)),
      source_file = {{ f }}
    ) %>%
    select(-filler)
})

# Write the files to S3, partitioned by year
cc_pifdb_piexemptre_dise %>%
  mutate(loaded_at = as.character(Sys.time())) %>%
  group_by(year) %>%
  arrow::write_dataset(
    path = file.path(
      output_bucket, "cc_pifdb_piexemptre_dise"
    ),
    format = "parquet",
    hive_style = TRUE,
    compression = "zstd"
  )


##### CC_PIFDB_PIEXEMPTRE_OWNR #####
files_cc_pifdb_piexemptre_ownr <- aws.s3::get_bucket_df(
  bucket = AWS_S3_RAW_BUCKET,
  prefix = "ccao/legacy/CC_PIFDB_PIEXEMPTRE_OWNR",
  max = Inf
) %>%
  filter(Size > 0)

# Read the files into a single tibble. NOTE: these files are fixed-width
# and have been MANUALLY CLEANED to remove some ASCII null characters that were
# being used instead of spaces in the base year field
cc_pifdb_piexemptre_ownr <- map_dfr(files_cc_pifdb_piexemptre_ownr$Key, \(f) {
  print(glue::glue("Transforming {f}"))
  aws.s3::s3read_using(
    object = f,
    bucket = AWS_S3_RAW_BUCKET,
    FUN = readr::read_fwf,
    trim_ws = TRUE,
    col_positions = readr::fwf_cols(
      pin = c(1, 14),
      year = c(16, 17),
      tax_year = c(19, 20),
      tax_type = c(22, 22),
      segment_code = c(24, 24),
      printed_indicator = c(26, 26),
      response = c(28, 28),
      year_applied = c(30, 33),
      maintenance_indicator = c(35, 35),
      proration_factor = c(37, 43),
      coop_quantity = c(45, 49),
      coop_status = c(51, 51),
      equalized_factor = c(53, 57),
      assessed_value = c(59, 67),
      equalized_value = c(69, 77),
      batch_number = c(79, 83),
      occupancy_factor = c(85, 89),
      exemption_amount = c(91, 99),
      exemption_base_year = c(101, 104),
      exemption_status = c(106, 107),
      filler = c(109, 234)
    ),
    col_types = cols(
      pin = col_character(),
      year = col_character(),
      tax_year = col_character(),
      tax_type = col_character(),
      segment_code = col_character(),
      printed_indicator = col_character(),
      response = col_character(),
      year_applied = col_character(),
      maintenance_indicator = col_character(),
      proration_factor = col_integer(),
      coop_quantity = col_integer(),
      coop_status = col_character(),
      equalized_factor = col_integer(),
      assessed_value = col_integer(),
      equalized_value = col_integer(),
      batch_number = col_integer(),
      occupancy_factor = col_integer(),
      exemption_amount = col_integer(),
      exemption_base_year = col_character(),
      exemption_status = col_character(),
      filler = col_character()
    )
  ) %>%
    mutate(
      across(
        c(year, tax_year),
        \(x) ifelse(substr(x, 1, 1) == "9", paste0("19", x), paste0("20", x))
      ),
      source_file = {{ f }}
    ) %>%
    select(-filler)
})

# Write the files to S3, partitioned by year
cc_pifdb_piexemptre_ownr %>%
  mutate(loaded_at = as.character(Sys.time())) %>%
  group_by(year) %>%
  arrow::write_dataset(
    path = file.path(
      output_bucket, "cc_pifdb_piexemptre_ownr"
    ),
    format = "parquet",
    hive_style = TRUE,
    compression = "zstd"
  )


##### CCT_AS_COFE_HDR
files_cct_as_cofe_hdr <- aws.s3::get_bucket_df(
  bucket = AWS_S3_RAW_BUCKET,
  prefix = "ccao/legacy/CCT_AS_COFE_HDR",
  max = Inf
) %>%
  filter(Size > 0)

cct_as_cofe_hdr <- map_dfr(files_cct_as_cofe_hdr$Key, \(f) {
  aws.s3::s3read_using(
    object = f,
    bucket = AWS_S3_RAW_BUCKET,
    FUN = readr::read_fwf,
    trim_ws = TRUE,
    col_positions = readr::fwf_cols(
      township_code = c(1, 3),
      volume = c(5, 7),
      pin = c(9, 22),
      process_year = c(24, 27),
      tax_year = c(29, 32),
      tax_type = c(34, 34),
      certificate_number = c(36, 42),
      date_issued = c(44, 51),
      control_number = c(53, 57),
      certificate_issued_by = c(59, 66),
      segment_counter = c(68, 69)
    ),
    col_types = cols(
      township_code = col_character(),
      volume = col_character(),
      pin = col_character(),
      process_year = col_character(),
      tax_year = col_character(),
      tax_type = col_character(),
      certificate_number = col_character(),
      date_issued = col_character(),
      control_number = col_character(),
      certificate_issued_by = col_character(),
      segment_counter = col_character()
    )
  ) %>%
    mutate(
      year = tax_year,
      # Parse MMDDYYYY date columns
      across(
        starts_with("date_"),
        \(x) lubridate::mdy(na_if(x, "00000000"))
      ),
      source_file = {{ f }}
    )
})

cct_as_cofe_hdr %>%
  mutate(loaded_at = as.character(Sys.time())) %>%
  group_by(year) %>%
  arrow::write_dataset(
    path = file.path(
      output_bucket, "cct_as_cofe_hdr"
    ),
    format = "parquet",
    hive_style = TRUE,
    compression = "zstd"
  )


##### CCT_AS_COFE_DTL
files_cct_as_cofe_dtl <- aws.s3::get_bucket_df(
  bucket = AWS_S3_RAW_BUCKET,
  prefix = "ccao/legacy/CCT_AS_COFE_DTL",
  max = Inf
) %>%
  filter(Size > 0)

cct_as_cofe_dtl <- map_dfr(files_cct_as_cofe_dtl$Key, \(f) {
  raw_path <- tempfile(fileext = ".txt")
  clean_path <- tempfile(fileext = ".txt")

  aws.s3::save_object(
    object = f,
    bucket = AWS_S3_RAW_BUCKET,
    file = raw_path
  )

  # Convert ASCII nulls to spaces in raw file
  raw_bytes <- readBin(raw_path, what = "raw", n = file.info(raw_path)$size)
  clean_bytes <- raw_bytes
  clean_bytes[clean_bytes == as.raw(0)] <- as.raw(32)
  writeBin(clean_bytes, clean_path)

  df_raw <- readr::read_fwf(
    file = clean_path,
    trim_ws = TRUE,
    col_positions = readr::fwf_cols(
      pin = c(1, 14),
      tax_year = c(16, 19),
      key_action_no = c(21, 23),
      coe_type = c(25, 26),
      coe_status = c(28, 28),
      coe_reason = c(30, 31),
      print_indicator = c(33, 33),
      adjudicated_indicator = c(35, 35),
      tax_code = c(37, 41),
      tax_rate = c(43, 51),
      amount_due = c(53, 65),
      amount_due_sign = c(67, 67),
      amount_paid = c(69, 81),
      amount_paid_sign = c(83, 83),
      applicant_name = c(85, 106),
      applicant_address = c(108, 129),
      applicant_city = c(131, 142),
      applicant_state = c(144, 145),
      applicant_zip = c(147, 155),
      applicant_phone_number = c(157, 167),
      applicant_name_secondary = c(169, 190),
      certified_av = c(192, 204),
      certified_exe_homeowner = c(206, 218),
      certified_exe_homestead_coop_qty = c(220, 224),
      certified_exe_homestead = c(226, 238),
      recommended_av = c(240, 252),
      recommended_original_eav = c(254, 266),
      recommended_exe_homeowner_1977_base_value = c(268, 280),
      recommended_exe_homeowner_proration = c(282, 288),
      recommended_exe_homeowner_occ_factor = c(290, 294),
      recommended_exe_homeowner_coop_qty = c(296, 300),
      recommended_exe_homeowner = c(302, 314),
      recommended_exe_homestead_coop_qty = c(316, 320),
      recommended_exe_tax_amount = c(322, 334),
      recommended_exe_tax_amount_sign = c(336, 336),
      adjudicated_new_av = c(338, 350),
      adjudicated_new_eav = c(352, 364),
      adjudicated_adjusted_tax_amount = c(366, 378),
      date_to_ba_from_assr = c(380, 387),
      date_from_ba_to_assr = c(389, 396),
      date_to_sa_from_assr = c(398, 405),
      date_from_sa_to_assr = c(407, 414),
      date_to_treasurer = c(416, 423),
      date_adjudicated = c(425, 432),
      amendment_number = c(434, 436),
      date_amended = c(438, 445),
      date_updated = c(447, 454),
      update_id = c(456, 463),
      date_refunded = c(465, 472),
      account_number = c(474, 476),
      d_o_number = c(478, 484),
      excess_valuation = c(486, 498),
      amount_of_credit = c(500, 512),
      amount_of_credit_sign = c(514, 514),
      refund_amount = c(516, 528),
      refund_amount_sign = c(530, 530),
      mmyyyy_date_interest = c(532, 537),
      interest_percent = c(539, 543),
      filler = c(545, 553),
      will_call_name = c(555, 576),
      will_call_phone_number = c(578, 588),
      federal_id_number = c(590, 598),
      interest_amount = c(600, 610),
      interest_amount_sign = c(612, 612),
      rsf_resp = c(614, 614),
      rsf_value = c(616, 624),
      date_action_issued = c(626, 633),
      date_track = c(635, 642),
      tracking_disposition = c(644, 645),
      certified_sen_freeze_resp = c(647, 647),
      certified_land_valuation = c(649, 657),
      certified_building_valuation = c(659, 667),
      certified_exe_freeze_valuation = c(689, 697),
      date_interest = c(699, 706),
      date_check = c(708, 715),
      check_number = c(717, 723),
      certified_home_av = c(725, 733),
      date_treasurer_track = c(735, 742),
      certified_exe_vetdis = c(744, 748),
      recommended_exe_vetdis = c(750, 754),
      certified_exe_homeowner_proration = c(756, 762),
      certified_exe_homeowner_coop_qty = c(764, 768),
      certified_exe_homeowner_occ_factor = c(770, 774),
      cho_resp = c(776, 776),
      chs_resp = c(778, 778),
      will_call_zip = c(780, 788),
      refund_indicator = c(790, 790),
      recommended_exe_homestead = c(792, 804),
      homeowner_response = c(806, 806),
      homestead_response = c(808, 808),
      certified_eav = c(810, 822),
      jrno = c(824, 829),
      amendment_indicator = c(831, 831),
      recommended_lt_response = c(833, 833),
      recommended_lt_value = c(835, 843),
      certified_lt_response = c(845, 845),
      certified_lt_value = c(847, 855)
    ),
    col_types = cols(
      pin = col_character(),
      tax_year = col_character(),
      key_action_no = col_integer(),
      coe_type = col_character(),
      coe_status = col_character(),
      coe_reason = col_character(),
      print_indicator = col_character(),
      adjudicated_indicator = col_character(),
      tax_code = col_character(),
      tax_rate = col_integer(),
      amount_due = col_integer(),
      amount_due_sign = col_character(),
      amount_paid = col_integer(),
      amount_paid_sign = col_character(),
      applicant_name = col_character(),
      applicant_address = col_character(),
      applicant_city = col_character(),
      applicant_state = col_character(),
      applicant_zip = col_character(),
      applicant_phone_number = col_character(),
      applicant_name_secondary = col_character(),
      certified_av = col_integer(),
      certified_exe_homeowner = col_integer(),
      certified_exe_homestead_coop_qty = col_integer(),
      certified_exe_homestead = col_integer(),
      recommended_av = col_integer(),
      recommended_original_eav = col_integer(),
      recommended_exe_homeowner_1977_base_value = col_integer(),
      recommended_exe_homeowner_proration = col_integer(),
      recommended_exe_homeowner_occ_factor = col_double(),
      recommended_exe_homeowner_coop_qty = col_integer(),
      recommended_exe_homeowner = col_integer(),
      recommended_exe_homestead_coop_qty = col_integer(),
      recommended_exe_tax_amount = col_integer(),
      recommended_exe_tax_amount_sign = col_character(),
      adjudicated_new_av = col_integer(),
      adjudicated_new_eav = col_integer(),
      adjudicated_adjusted_tax_amount = col_integer(),
      date_to_ba_from_assr = col_character(),
      date_from_ba_to_assr = col_character(),
      date_to_sa_from_assr = col_character(),
      date_from_sa_to_assr = col_character(),
      date_to_treasurer = col_character(),
      date_adjudicated = col_character(),
      amendment_number = col_character(),
      date_amended = col_character(),
      date_updated = col_character(),
      update_id = col_character(),
      date_refunded = col_character(),
      account_number = col_character(),
      d_o_number = col_character(),
      excess_valuation = col_integer(),
      amount_of_credit = col_integer(),
      amount_of_credit_sign = col_character(),
      refund_amount = col_integer(),
      refund_amount_sign = col_character(),
      mmyyyy_date_interest = col_character(),
      interest_percent = col_integer(),
      filler = col_character(),
      will_call_name = col_character(),
      will_call_phone_number = col_character(),
      federal_id_number = col_character(),
      interest_amount = col_integer(),
      interest_amount_sign = col_character(),
      rsf_resp = col_character(),
      rsf_value = col_integer(),
      date_action_issued = col_character(),
      date_track = col_character(),
      tracking_disposition = col_character(),
      certified_sen_freeze_resp = col_character(),
      certified_land_valuation = col_integer(),
      certified_building_valuation = col_integer(),
      certified_exe_freeze_valuation = col_integer(),
      date_interest = col_character(),
      date_check = col_character(),
      check_number = col_character(),
      certified_home_av = col_integer(),
      date_treasurer_track = col_character(),
      certified_exe_vetdis = col_integer(),
      recommended_exe_vetdis = col_integer(),
      certified_exe_homeowner_proration = col_integer(),
      certified_exe_homeowner_coop_qty = col_integer(),
      certified_exe_homeowner_occ_factor = col_integer(),
      cho_resp = col_character(),
      chs_resp = col_character(),
      will_call_zip = col_character(),
      refund_indicator = col_character(),
      recommended_exe_homestead = col_integer(),
      homeowner_response = col_character(),
      homestead_response = col_character(),
      certified_eav = col_integer(),
      jrno = col_character(),
      amendment_indicator = col_character(),
      recommended_lt_response = col_character(),
      recommended_lt_value = col_integer(),
      certified_lt_response = col_character(),
      certified_lt_value = col_integer()
    )
  )
  df <- df_raw %>%
    mutate(
      year = tax_year,
      # Clean string columns to remove extraneous whitespace
      across(
        c(
          starts_with("applicant_"),
          "update_id",
          "d_o_number",
          "will_call_name"
        ),
        \(x) str_trim(str_squish(x))
      ),
      # Replace numeric columns with nulls for all-zero values
      will_call_phone_number = na_if(will_call_phone_number, "0000000000"),
      federal_id_number = na_if(federal_id_number, "000000000"),
      # Parse MMDDYYYY date columns
      across(
        starts_with("date_"),
        \(x) lubridate::mdy(na_if(x, "00000000"))
      ),
      # Parse MMYYYY date columns
      mmyyyy_date_interest = lubridate::my(
        na_if(mmyyyy_date_interest, "000000")
      ),
      # Logical conversion for some response columns
      across(
        c("rsf_resp", "certified_sen_freeze_resp"),
        \(x) {
          case_when(
            x == "Y" ~ TRUE,
            x == "N" ~ FALSE,
            is.na(x) ~ NA
          )
        }
      ),
      # Zip code truncation
      across(ends_with("_zip"), \(x) str_sub(x, 1, 5)),
      # Numeric scaling for percentages and doubles
      across(contains("exe_homeowner_proration"), \(x) x / 1000000),
      across(
        c(contains("exe_homeowner_occ_factor"), "interest_percent", "tax_rate"),
        \(x) x / 1000
      ),
      across(
        c(
          "amount_due", "amount_paid", "recommended_exe_tax_amount",
          "adjudicated_adjusted_tax_amount", "amount_of_credit",
          "refund_amount", "interest_amount"
        ),
        \(x) x / 100
      ),
      source_file = f
    ) %>%
    select(-filler)

  # Clean up temp files
  file.remove(c(raw_path, clean_path))

  df
})

cct_as_cofe_dtl %>%
  mutate(loaded_at = as.character(Sys.time())) %>%
  group_by(year) %>%
  arrow::write_dataset(
    path = file.path(
      output_bucket, "cct_as_cofe_dtl"
    ),
    format = "parquet",
    hive_style = TRUE,
    compression = "zstd"
  )
