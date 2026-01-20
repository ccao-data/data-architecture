library(arrow)
library(aws.s3)
library(dplyr)
library(glue)
library(purrr)
library(readr)
library(stringr)
library(tools)
library(readxl)
source("utils.R")

# This file downloads zoning data from the CCAO's O Drive. It is created by
# a CCAO employee and requested ad-hoc by the data team. It mostly renames
# the columns and removes duplicate Pins.

# Prevent scientific notation in console and I/O
options(scipen = 999)

# Define S3 root
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "ccao", "other", "zoning")

zoning <- read_excel("O:/CCAODATA/data/zoning/2025.xlsx") %>%
  select(pin = Name, zoning_date = ZoningDate, zoning_code = MunZone) %>%
  distinct(pin, zoning_date, zoning_code, .keep_all = TRUE) %>%
  group_by(pin) %>%
  summarise(
    # A small number of observations have two zoning codes, likely
    # due to fuzzy geo-spatial techniques
    zoning_code = paste(unique(zoning_code), collapse = ", "),
    # Most recent date of update
    zoning_date = last(zoning_date),
    .groups = "drop"
  ) %>%
  # Created year since data is downloaded from different data sources.
  mutate(year = "2025") %>%
  group_by(year)


# Raise an error if the observation count is not 1417359 observations.
# We do this because the original excel file is larger than it's limit
# and overwriting it may accidentally cut observations

if (nrow(zoning) != 1417359) {
  stop(
    glue(
      "Zoning data observation count {nrow(data)} does not equal expected ",
      "count of 1417359. Please verify the source data."
    )
  )
}

write_partitions_to_s3(
  df = zoning,
  s3_output_path = output_bucket,
  is_spatial = FALSE,
  overwrite = TRUE
)
