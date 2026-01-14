library(arrow)
library(aws.s3)
library(dplyr)
library(openxlsx)
library(purrr)
library(readr)
library(snakecase)
library(stringr)
library(tidyr)
source("utils.R")

################################################################################
# This is static data and this script does not need to be re-run unless the data
# is no longer availale in the Data Department's warehouse S3 bucket.
################################################################################

# This script retrieves and cleans land value spreadsheets provided by
# the Valuations department and formats them for use in Athena
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
input_bucket <- file.path(AWS_S3_RAW_BUCKET, "ccao", "land")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "ccao", "land")

# Location of remote files
remote_file_raw_site_rate_2022 <- file.path(
  input_bucket, "site_rate", "2022.xlsx"
)
remote_file_warehouse_site_rate <- file.path(
  output_bucket, "land_site_rate"
)

# Temp file to download workbook
tmp_file_site_rate_2022 <- tempfile(fileext = ".xlsx")

# Grab the workbook from the raw S3 bucket
aws.s3::save_object(
  object = remote_file_raw_site_rate_2022,
  file = tmp_file_site_rate_2022
)

# Load the raw workbook, rename and clean up columns, then write to S3
# partitioned by year
land_site_rate <- openxlsx::read.xlsx(tmp_file_site_rate_2022) %>%
  set_names(snakecase::to_snake_case(names(.))) %>%
  select(
    pin = parid,
    class,
    town_nbhd = nbhd,
    land_rate_per_pin = flat_townhome_value_2022,
    land_rate_per_sqft = rate_sf_2022,
    land_pct_tot_fmv = flat_tot_mv
  ) %>%
  mutate(
    year = "2022",
    across(c(town_nbhd, class), str_remove_all, "-"),
    land_rate_per_pin = as.integer(land_rate_per_pin)
  ) %>%
  drop_na(pin, land_rate_per_pin) %>%
  mutate(loaded_at = as.character(Sys.time())) %>%
  group_by(year) %>%
  write_partitions_to_s3(
    remote_file_warehouse_site_rate,
    is_spatial = FALSE,
    overwrite = TRUE
  )
