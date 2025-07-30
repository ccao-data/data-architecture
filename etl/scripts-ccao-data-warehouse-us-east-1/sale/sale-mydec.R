library(arrow)
library(dplyr)
library(lubridate)
library(readr)
library(stringr)
library(tidyr)
library(tools)
source("utils.R")

# This script cleans and combines raw mydec data for the warehouse
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "sale", "mydec_test")

# Read in MyDec column name crosswalk
columns_crosswalk <- read_delim("../mydec_crosswalk.csv", delim = ",")
lookup <- columns_crosswalk %>% pull(mydec_api)
names(lookup) <- columns_crosswalk$ccao_warehouse

# Load raw sales files
sales <- open_dataset(file.path(AWS_S3_RAW_BUCKET, "sale", "mydec_test")) %>%
  collect()

# Clean up, then write to S3
temp <- sales %>%
  rename(any_of(lookup)) %>%
  select(any_of(names(lookup)), loaded_at, year) %>%
  filter(!is.na(document_number)) %>%
  mutate(
    across(where(is.character), str_squish),
    across(where(is.character), ~ na_if(.x, "")),
    across(where(is.character), ~ na_if(.x, "NULL")),
    document_number = str_remove_all(document_number, "D"),
    # Convert all columns that are character and contain only "TRUE", "FALSE"
    # or NA to booleans. This could potentially convert an empty column to a
    # boolean but it doesn't matter what type empty columns are.
    across(where(~ all(unique(.x) %in% c("FALSE", "TRUE", NA))), as.logical),
    # Convert columns that only contain numbers to numeric
    across(where(~ all(grepl("[0-9.]", .x[!is.na(.x)]))), as.numeric),
    across(ends_with("consideration"), as.integer),
  ) %>%
  group_by(document_number) %>%
  # Remove sales that have multiple lines with the same document number where
  # the total number of parcels don't match line_2_total_parcels or the sales
  # took place on different days. These sales are too dirty to be useful.
  filter(
    (n() == 1 | max(line_2_total_parcels) == n()),
    n_distinct(line_4_instrument_date) == 1
  ) %>%
  arrange(line_4_instrument_date, date_recorded, .by_group = TRUE) %>%
  mutate(is_multisale = n() > 1) %>%
  relocate(year_of_sale = year, .after = last_col()) %>%
  group_by(year_of_sale) %>%
  write_partitions_to_s3(output_bucket, is_spatial = FALSE, overwrite = TRUE)
