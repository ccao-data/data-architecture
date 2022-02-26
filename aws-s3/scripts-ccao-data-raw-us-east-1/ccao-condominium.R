library(arrow)
library(aws.s3)
library(dplyr)
library(glue)
library(purrr)
library(openxlsx)
library(tools)
library(stringr)
source("utils.R")

# This script retrieves raw condominium characteristics from the CCAO's O Drive
# compiled by the valuations department
AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "ccao", "condominium", "condo_pin_char")

# Get local file addresses
source_paths <- c("//fileserver/ocommon/2022 Data Collection/Condo Project/William Approved Layout North Tri Condo Project FINAL COMPLETED/")

source_files <- grep(
  "completed",
  list.files(
    source_paths,
    recursive = TRUE, full.names = TRUE
  ),
  ignore.case = TRUE,
  value = TRUE
)

# Function to retrieve data and write to S3
read_write <- function(x) {
  openxlsx::read.xlsx(x, sheet = 1) %>%
    write_parquet(
      tolower(
        str_replace(
          file.path(
            output_bucket,
            str_sub(str_extract(x, '20.*'), 1, 4),
            paste0(tools::file_path_sans_ext(basename(x)), ".parquet")
            ),
          " ", "_"
          )
        )
      )
}

# Apply function to foreclosure data
walk(source_files, read_write)
