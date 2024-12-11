library(arrow)
library(aws.s3)
library(dplyr)
library(openxlsx)
library(purrr)
library(readr)
library(tools)

# This script retrieves raw condominium characteristics from the CCAO's O Drive
# compiled by the valuations department
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "ccao", "condominium")

##### QUESTIONABLE GARAGE UNITS #####

openxlsx::read.xlsx(
  "O:/Condo Worksheets/Condo 2022/2022 Desk Review - Questionable Garage Units.xlsx", # nolint
  sheet = 1
) %>%
  write_parquet(
    tolower(
      file.path(
        output_bucket, "pin_questionable_garage_units", "2022.parquet"
      )
    )
  )

##### 399 GARAGE UNITS & NEGATIVE PREDICTED VALUE #####

# Negative predicted values were detected by our intern Caroline while
# investigating condo data and suggest a unit should be non-livable.

# Retrieve data and write to S3
file_path <- "O:/CCAODATA/data/condos"
source_files <- file_path_sans_ext(list.files(file_path))
walk(source_files, function(x) {
  if (!aws.s3::object_exists(file.path(output_bucket, x, "2023.parquet"))) {
    read_delim(
      file.path(file_path, paste0(x, ".csv")),
      delim = ",",
      col_types = rep("c", 3)
    ) %>%
      write_parquet(file.path(output_bucket, x, "2023.parquet"))
  }
})
