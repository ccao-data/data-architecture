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
output_bucket <- file.path(AWS_S3_RAW_BUCKET, "condominium", "characteristic")

# Get local file addresses
source_paths <- c("//fileserver/ocommon/2022 Data Collection/Condo Project/")

source_files <- grep(
  "completed",
  list.files(
    source_paths,
    recursive = TRUE, full.names = TRUE
  ),
  ignore.case = TRUE,
  value = TRUE
)

remove <- c("completed", "condo", "project", "[[:punct:]]", "[[:number:]]")

# Function to retrieve data and write to S3
read_write <- function(x) {
  openxlsx::read.xlsx(x, sheet = 1) %>%
    write_parquet(
      tolower(
        str_replace(
          file.path(
            output_bucket,
            str_sub(str_extract(x, '20.*'), 1, 4),
            paste0(
              str_squish(
                str_remove_all(
                  tools::file_path_sans_ext(basename(x)), regex(str_c(remove, collapse = '|'), ignore_case = T)
                )
              ),
              ".parquet"
            )
          ),
          " ", "_"
        )
      )
    )
}

# Apply function to foreclosure data
walk(source_files, read_write)


