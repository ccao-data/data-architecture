library(arrow)
library(dplyr)
library(lubridate)
library(readr)
library(stringr)
library(tools)
source("utils.R")

# This script cleans and combines raw mydec data for the warehouse
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "sale", "mydec")

# Crosswalk ----

# Read in MyDec column name crosswalk
columns_crosswalk <- read_delim(
  "../dbt/seeds/sale/sale.mydec_crosswalk.csv",
  delim = ","
) %>%
  # We store dates as strings in Athena
  mutate(field_type = str_replace_all(field_type, "date", "character"))

# We can convert two columns from the crosswalk to a named list and use them to
# rename all the columns from the raw data
lookup <- columns_crosswalk %>% pull(mydec_api)
names(lookup) <- columns_crosswalk$ccao_warehouse

# Data cleaning ----

# Load raw sales files
sales <- open_dataset(file.path(AWS_S3_RAW_BUCKET, "sale", "mydec")) %>%
  collect()

# Clean up, then write to S3
sales %>%
  rename(any_of(lookup)) %>%
  (\(x) {
    # If the lookup has column names that are not in the dataset, add them as
    # empty columns
    x[setdiff(names(lookup), names(x))] <- NA
    return(x)
  }) %>%
  select(all_of(names(lookup)), loaded_at, year) %>%
  filter(!is.na(document_number)) %>%
  mutate(
    across(where(is.character), str_squish),
    across(where(is.character), ~ na_if(.x, "")),
    across(where(is.character), ~ na_if(.x, "NULL")),
    across(contains("date"), ~ str_sub(.x, 1, 10)), # Remove time from dates,
    document_number = gsub("\\D", "", document_number)
  ) %>%
  # This function is slow, but too convenient not to use. It will convert all
  # columns types according to the crosswalk.
  type_convert(paste(
    # Add two more c's for loaded_at and year since they're not in the crosswalk
    c(str_sub(columns_crosswalk$field_type, 1, 1), "c", "c"),
    collapse = ""
  )) %>%
  group_by(document_number) %>%
  mutate(line_2_total_parcels = max(line_2_total_parcels)) %>%
  # Remove sales that have multiple lines with the same document number where
  # the total number of parcels don't match line_2_total_parcels or the sales
  # took place on different days. These sales are too dirty to be useful.
  filter(
    (n() == 1 | line_2_total_parcels == n()),
    n_distinct(line_4_instrument_date) == 1
  ) %>%
  arrange(line_4_instrument_date, date_recorded, .by_group = TRUE) %>%
  mutate(is_multisale = n() > 1 | line_2_total_parcels > 1) %>%
  relocate(year_of_sale = year, .after = last_col()) %>%
  group_by(year_of_sale) %>%
  write_partitions_to_s3(output_bucket, is_spatial = FALSE, overwrite = TRUE)
