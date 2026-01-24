library(aws.s3)
library(openxlsx)
library(arrow)
library(dplyr)
library(purrr)
library(readr)
library(stringr)
library(tools)
source("utils.R")

# This script cleans the Area Risk Index data, standardizing column names to
# our conventions, and creating a data_year column based on the final year of
# census data.

AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
AWS_S3_WAREHOUSE_BUCKET <- Sys.getenv("AWS_S3_WAREHOUSE_BUCKET")
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "housing", "ari")

# Get raw file paths
output <- file.path(
  AWS_S3_RAW_BUCKET,
  get_bucket_df(AWS_S3_RAW_BUCKET, prefix = "housing/ari")$Key
) %>%
  # Load, clean, and write data back to S3
  map(\(x) {
    file_ext <- paste0(".", file_ext(x))
    tmp_file <- tempfile(fileext = file_ext)

    save_s3_to_local(
      s3_uri = x,
      path = tmp_file
    )

    # Use switch to select the correct reading function
    data <- switch(file_ext,
      ".csv" = read_delim(tmp_file, delim = ","),
      ".xlsx" = read.xlsx(tmp_file, startRow = 3, sep.names = " ")
    ) %>%
      mutate(
        geoid = as.character(`Census Tract`),
        ari_score = `Total ARI Score`,
        # Year refers to final year of census data.
        # Data_year refers to year of data construction.
        # For ARI 2023, census data is from 2021.
        # For ARI 2025, census data is from 2023.
        data_year = as.character(as.numeric(str_extract(x, "[0-9]{4}")) - 2),
        year = str_extract(x, "[0-9]{4}")
      ) %>%
      select(geoid, ari_score, data_year, year)

    file.remove(tmp_file)

    return(data)
  }) %>%
  bind_rows() %>%
  group_by(year)

output %>%
  write_partitions_to_s3(
    output_bucket,
    is_spatial = FALSE,
    overwrite = TRUE
  )
