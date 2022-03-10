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

# This script retrieves and cleans land value spreadsheets provided by
# the Valuations department and formats them for use in Athena
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
input_bucket <- file.path(AWS_S3_RAW_BUCKET, "ccao", "land")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "ccao", "land")

# Location of remote files
remote_file_raw_nbhd_rate_2022 <- file.path(
  input_bucket, "nbhd_rate", "2022.xlsx"
)
remote_file_warehouse_nbhd_rate <- file.path(
  output_bucket, "land_nbhd_rate"
)

# Temp file to download workbook
tmp_file_nbhd_rate_2022 <- tempfile(fileext = ".xlsx")

# Grab the workbook from the raw S3 bucket
aws.s3::save_object(
  object = remote_file_raw_nbhd_rate_2022,
  file = tmp_file_nbhd_rate_2022
)

# Load the raw workbook, rename and clean up columns, then write to S3
# partitioned by year
land_nbhd_rate <- openxlsx::read.xlsx(tmp_file_nbhd_rate_2022) %>%
  set_names(snakecase::to_snake_case(names(.))) %>%
  select(
    township_code = twp_number,
    township_name = twp_name,
    town_nbhd = twp_nbhd,
    `2019` = `2019_rate`,
    `2022` = `2022_rate`
  ) %>%
  pivot_longer(
    c(`2019`, `2022`),
    names_to = "year", values_to = "land_rate_per_sqft"
  ) %>%
  mutate(
    across(c(township_code:town_nbhd, year), as.character),
    town_nbhd = str_remove_all(town_nbhd, "-"),
    land_rate_per_sqft = parse_number(land_rate_per_sqft)
  ) %>%
  group_by(year) %>%
  arrow::write_dataset(
    path = remote_file_warehouse_nbhd_rate,
    format = "parquet",
    hive_style = TRUE,
    compression = "snappy"
  )
