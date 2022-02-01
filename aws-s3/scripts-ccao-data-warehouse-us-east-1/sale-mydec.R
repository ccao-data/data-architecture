library(arrow)
library(aws.s3)
library(data.table)
library(dplyr)
library(glue)
library(lubridate)
library(purrr)
library(readr)
library(sf)
library(stringr)
library(tidyr)
library(tools)
source("utils.R")

# This script cleans and combines raw mydec data for the warehouse
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "sale", "mydec")

# Destination for upload
dest_file <- file.path(
  AWS_S3_WAREHOUSE_BUCKET,
  max(aws.s3::get_bucket_df(AWS_S3_RAW_BUCKET, prefix = "sale/mydec/")$Key)
)

# Get S3 file addresses
files <- grep(
  ".parquet",
  file.path(
    AWS_S3_RAW_BUCKET,
    aws.s3::get_bucket_df(AWS_S3_RAW_BUCKET, prefix = "sale/mydec/")$Key
  ),
  value = TRUE
)

# Function to make sure mydec data can be stacked across years
clean_up <- function(x) {

  read_parquet(x) %>%
    mutate(across(where(is.Date), as.character)) %>%
    rename_with(~ str_replace_all(.x, c("\\?" = "", "Step 4 - " = "", "Step 3 - " = ""))) %>%
    rename_with(~"Legal Description 1", ends_with("Legal Description"))

}

# Load raw files, cleanup, then write to warehouse S3
map(files, clean_up) %>%
  rbindlist(fill = TRUE) %>%
  na_if("NULL") %>%
  rename_with(~ tolower(
    str_replace_all(
      str_squish(
        str_replace_all(.x, "[[:punct:]]", "")
      ), " ", "_")
  )) %>%
  filter(!is.na(document_number) & line_1_county == "Cook") %>%
  group_by(document_number) %>%
  # Data isn't unique by document number, or even transaction date and document number
  # so we arrange by transaction date and then recorder date within codument number and
  # create an indicator for the first row withinduplicated document numbers
  arrange(line_4_instrument_date, date_recorded, .by_group = TRUE) %>%
  mutate(year_of_sale = lubridate::year(line_4_instrument_date),
         declaration_id = as.character(declaration_id),
         is_earliest_within_doc_no = case_when(
           1:n() == 1 ~ TRUE,
           TRUE ~ FALSE
         )) %>%
  group_by(year_of_sale) %>%
  write_partitions_to_s3(output_bucket, is_spatial = FALSE, overwrite = TRUE)
