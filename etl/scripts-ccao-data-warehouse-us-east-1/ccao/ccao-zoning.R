# Prevent scientific notation in console and I/O
options(scipen = 999)

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

# Define S3 root
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "ccao", "other", "zoning")

# Grab all xlsx data from our created folder
township_paths <- list.files("O:/CCAODATA/data/zoning/",
  pattern = "\\.xlsx$", full.names = TRUE
)

township_data <- map(
  list.files("O:/CCAODATA/data/zoning/", full.names = TRUE),
  \(file_path) {
    read_excel(file_path) %>%
      select(
        pin = matches(c("PIN14", "PARID")),
        zoning_code = contains(c("MunZone", "zone_class"))
      ) %>%
      mutate(across(everything(), as.character)) %>%
      # Drop observations without a zoning code
      filter(!is.na(pin), !is.na(zoning_code))
  }
)

# === Combine all data and write one Parquet file ===
zoning <- bind_rows(township_data) %>%
  mutate(pin = str_pad(pin, 14, side = "left", pad = "0")) %>%
  distinct(pin, zoning_code, .keep_all = TRUE) %>%
  group_by(pin) %>%
  summarise(
    # A small number of observations have two zoning codes, likely
    # due to fuzzy geo-spatial techniques
    zoning_code = paste(unique(zoning_code), collapse = ", "),
    .groups = "drop"
  ) %>%
  # Created year since data is downloaded from different data sources.
  mutate(year = "2025") %>%
  group_by(year)

write_partitions_to_s3(
  df = zoning,
  s3_output_path = output_bucket,
  is_spatial = FALSE,
  overwrite = TRUE
)
