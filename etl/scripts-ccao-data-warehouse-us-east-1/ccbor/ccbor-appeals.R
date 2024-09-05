library(arrow)
library(dplyr)
library(purrr)
library(stringr)
library(tidyr)
source("utils.R")

# This script retrieves and BOR appeals data and formats it for use in Athena
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
input_bucket <- file.path(AWS_S3_RAW_BUCKET, "ccabor", "appeals")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "ccbor", "appeals")

# Grab the file paths for the raw data on S3
raw_paths <- aws.s3::get_bucket_df(
  AWS_S3_RAW_BUCKET,
  prefix = "ccbor/appeals/"
) %>%
  filter(str_detect(Key, "parquet")) %>%
  pull(Key)

# Load the raw appeals data, rename and clean up columns, then write to S3
# partitioned by year
map(raw_paths, \(x) {
  read_parquet(file.path(AWS_S3_RAW_BUCKET, x))
}) %>%
  bind_rows() %>%
  select(
    appeal_id = appealid,
    pin,
    class,
    township_code,
    tax_code = taxcode,
    appeal_trk = appealtrk,
    appeal_seq = appealseq,
    appeal_type = appealtype,
    appeal_type_desc = appealtypedescription,
    assessor_land_value = assessor_landvalue,
    assessor_improvement_value = assessor_improvementvalue,
    assessor_total_value = assessor_totalvalue,
    bor_land_value = bor_landvalue,
    bor_improvement_value = bor_improvementvalue,
    bor_total_value = bor_totalvalue,
    result,
    change_reason = changereason,
    change_reason_desc = changereasondescription,
    no_change_reason = nochangereason,
    no_change_reason_desc = nochangereasondescription,
    appellant,
    appellant_address,
    appellant_city,
    appellant_state,
    appellant_zip,
    attorney_code = attorneycode,
    attorney_id = attny,
    attorney_first_name = attorney_firstname,
    attorney_last_name = attorney_lastname,
    attorney_firm_name = attorney_firmname,
    taxyr = tax_year
  ) %>%
  mutate(
    across(.cols = everything(), ~ na_if(.x, "N/A")),
    across(.cols = everything(), ~ na_if(.x, "")),
    across(contains("value"), as.integer)
  ) %>%
  group_by(taxyr) %>%
  write_partitions_to_s3(
    output_bucket,
    is_spatial = FALSE,
    overwrite = TRUE
  )
