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
    rename_with(
      ~ str_replace_all(.x, c("\\?" = "", "Step 4 - " = "", "Step 3 - " = ""))
    ) %>%
    rename_with(
      ~ paste0(.x, " 1", recycle0 = TRUE),
      .cols = ends_with("Legal Description")
    )
}

# MyDec columns we want to use, tend to be different across duplicate sales...
mydec_vars <- c(
  "line_7_property_advertised",
  "line_10a",
  "line_10b",
  "line_10c",
  "line_10d",
  "line_10e",
  "line_10f",
  "line_10g",
  "line_10h",
  "line_10i",
  "line_10j",
  "line_10k",
  "line_10l",
  "line_10m",
  "line_10n",
  "line_10o",
  "line_10p",
  "line_10q",
  "line_10s",
  "line_10s",
  "line_10s_generalalternative",
  "line_10s_senior_citizens",
  "line_10s_senior_citizens_assessment_freeze"
)

# Load raw files, cleanup, then write to warehouse S3
map(files, clean_up) %>%
  rbindlist(fill = TRUE) %>%
  rename_with(~ tolower(
    str_replace_all(
      str_squish(
        str_replace_all(.x, "[[:punct:]]", "")
      ), " ", "_"
    )
  )) %>%
  mutate(
    across(where(is.character), str_squish),
    across(where(is.character), ~ na_if(.x, "")),
    across(where(is.character), ~ na_if(.x, "NULL")),
    across(ends_with("consideration"), as.integer)
  ) %>%
  filter(!is.na(document_number) & line_1_county == "Cook") %>%
  mutate(document_number = str_replace_all(document_number, "D", "")) %>%
  group_by(document_number) %>%
  # Because MyDec variables can be different across duplicate sales doc #s,
  # we'll take the max values
  mutate(across(all_of(mydec_vars), ~ max(.x, na.rm = TRUE))) %>%
  mutate(across(all_of(mydec_vars), ~ na_if(.x, -Inf))) %>%
  distinct(
    document_number,
    line_7_property_advertised,
    line_10a,
    line_10b,
    line_10c,
    line_10d,
    line_10e,
    line_10f,
    line_10g,
    line_10h,
    line_10i,
    line_10j,
    line_10k,
    line_10l,
    line_10m,
    line_10n,
    line_10o,
    line_10p,
    line_10q,
    line_10s,
    line_10s,
    line_10s_senior_citizens,
    line_10s_senior_citizens_assessment_freeze,
    .keep_all = TRUE
  ) %>%
  # Data isn't unique by document number, or even transaction date and document
  # number so we arrange by transaction date and then recorded date within
  # document number and create an indicator for the first row within duplicated
  # document numbers
  arrange(line_4_instrument_date, date_recorded, .by_group = TRUE) %>%
  mutate(
    year_of_sale = as.character(lubridate::year(line_4_instrument_date)),
    declaration_id = as.character(declaration_id),
    is_earliest_within_doc_no = row_number() == 1
  ) %>%
  group_by(year_of_sale) %>%
  write_partitions_to_s3(output_bucket, is_spatial = FALSE, overwrite = TRUE)
