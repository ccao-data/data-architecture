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
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "ccao", "condominium")

##### QUESTIONABLE GARAGE UNITS #####

# Get local file addresses
source_paths <- c("O:/Condo Worksheets/Condo 2022/")

source_files <- list.files(
  source_paths,
  recursive = TRUE, full.names = TRUE
  )


# Function to retrieve data and write to S3
read_write_questionable <- function(x) {
  openxlsx::read.xlsx(x, sheet = 1) %>%
    write_parquet(
      tolower(
        file.path(
          output_bucket, "pin_questionable_garage_units",
          paste0(str_sub(str_extract(x, '20.*'), 1, 4),".parquet")
        )

      )
    )
}

# Apply function to foreclosure data
walk(source_files, read_write_questionable)
