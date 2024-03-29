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
remote_file_raw_nbhd_rate_2023 <- file.path(
  input_bucket, "nbhd_rate", "2023.xlsx"
)
remote_file_raw_nbhd_rate_2024 <- file.path(
  input_bucket, "nbhd_rate", "2024.xlsx"
)
remote_file_warehouse_nbhd_rate <- file.path(
  output_bucket, "land_nbhd_rate"
)


# Temp file to download workbook
tmp_file_nbhd_rate_2022 <- tempfile(fileext = ".xlsx")
tmp_file_nbhd_rate_2023 <- tempfile(fileext = ".xlsx")
tmp_file_nbhd_rate_2024 <- tempfile(fileext = ".xlsx")

# Grab the workbook from the raw S3 bucket
aws.s3::save_object(
  object = remote_file_raw_nbhd_rate_2022,
  file = tmp_file_nbhd_rate_2022
)
aws.s3::save_object(
  object = remote_file_raw_nbhd_rate_2023,
  file = tmp_file_nbhd_rate_2023
)
aws.s3::save_object(
  object = remote_file_raw_nbhd_rate_2024,
  file = tmp_file_nbhd_rate_2024
)

# List of regression classes
class <- ccao::class_dict %>%
  filter(regression_class) %>%
  pull(class_code)

# Load the raw workbooks, rename and clean up columns
land_nbhd_rate_2022 <- openxlsx::read.xlsx(tmp_file_nbhd_rate_2022) %>%
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
  expand_grid(class)

land_nbhd_rate_2023 <- openxlsx::read.xlsx(tmp_file_nbhd_rate_2023) %>%
  set_names(snakecase::to_snake_case(names(.))) %>%
  select(
    town_nbhd = neighborhood_id,
    `2020` = `2020_2_00_class_unit_price`,
    `2023` = `2023_2_00_class_unit_price`
  ) %>%
  mutate(
    town_nbhd = gsub("\\D", "", town_nbhd),
    township_code = substr(town_nbhd, 1, 2),
    township_name = ccao::town_convert(township_code)
  ) %>%
  relocate(c(township_code, township_name)) %>%
  pivot_longer(
    c(`2020`, `2023`),
    names_to = "year", values_to = "land_rate_per_sqft"
  ) %>%
  mutate(across(c(township_code:town_nbhd, year), as.character)) %>%
  expand_grid(class)

land_nbhd_rate_2024 <- openxlsx::read.xlsx(tmp_file_nbhd_rate_2024) %>%
  set_names(snakecase::to_snake_case(names(.))) %>%
  mutate(
    town_nbhd = paste0(
      township_code, str_pad(neighborhood, 3, side = "left", pad = "0")
    )
  ) %>%
  select(
    town_nbhd,
    classes,
    `2021` = `2021_unit_price`,
    `2024` = `2024_unit_price`
  ) %>%
  mutate(
    town_nbhd = gsub("\\D", "", town_nbhd),
    township_code = substr(town_nbhd, 1, 2),
    township_name = ccao::town_convert(township_code)
  ) %>%
  relocate(c(township_code, township_name)) %>%
  pivot_longer(
    c(`2021`, `2024`),
    names_to = "year", values_to = "land_rate_per_sqft"
  ) %>%
  mutate(across(c(township_code:town_nbhd, year), as.character)) %>%
  expand_grid(class) %>%
  # 2024 contains bifurcated neighborhood land rates across class
  filter(
    !(classes == "all other regression classes" & class %in% c("210", "295")),
    !(classes == "2-10s/2-95s" & !(class %in% c("210", "295")))
  ) %>%
  select(-classes)

# Write the rates to S3, partitioned by year
bind_rows(
  land_nbhd_rate_2022,
  land_nbhd_rate_2023,
  land_nbhd_rate_2024
) %>%
  relocate(land_rate_per_sqft, .after = last_col()) %>%
  group_by(year) %>%
  arrow::write_dataset(
    path = remote_file_warehouse_nbhd_rate,
    format = "parquet",
    hive_style = TRUE,
    compression = "snappy"
  )
