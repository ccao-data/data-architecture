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
      date_issued = c(59, 66),
      control_number = c(59, 66),
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
    mutate(year = tax_year)
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
