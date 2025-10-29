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
output_bucket <- file.path(
  AWS_S3_RAW_BUCKET, "ccao", "condominium", "pin_condo_char"
)

# Get local file addresses
source_paths <- c(
  "//fileserver/ocommon/2022 Data Collection/Condo Project/William Approved Layout North Tri Condo Project FINAL COMPLETED/", # nolint
  "//fileserver/ocommon/2023 Data Collection/South Tri Condo Project COMPLETED",
  "//fileserver/ocommon/2024 Data Collection/City Tri Condo Characteristics COMPLETED", # nolint
  "//fileserver/ocommon/CCAODATA/data/condo_chars/2025_completed", # Local copy of sharepoint file # nolint
  "//fileserver/ocommon/CCAODATA/data/condo_chars/2026_completed" # Local copy of sharepoint file # nolint
)

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
  destination <- tolower(
    str_replace(
      file.path(
        output_bucket,
        str_extract(x, "[0-9]{4}"),
        paste0(
          str_extract(x, paste(ccao::town_dict$township_name, collapse = "|")),
          ".parquet"
        )
      ),
      " ", "_"
    )
  )

  if (grepl("Lake View", x, ignore.case = TRUE)) {
    destination <- str_replace(destination, "lake", "lake_view")
  }

  if (!aws.s3::object_exists(destination)) {
    openxlsx::read.xlsx(x, sheet = 1) %>%
      write_parquet(destination)
  }
}

# Apply function to foreclosure data
walk(source_files, read_write)
