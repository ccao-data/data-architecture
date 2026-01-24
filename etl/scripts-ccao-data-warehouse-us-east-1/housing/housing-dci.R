library(aws.s3)
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
output_bucket <- file.path(AWS_S3_WAREHOUSE_BUCKET, "housing", "dci")

# Get raw file paths
output <- file.path(
  AWS_S3_RAW_BUCKET,
  get_bucket_df(AWS_S3_RAW_BUCKET, prefix = "housing/dci")$Key
) %>%
  # Load, clean, and write data back to S3
  map(\(x) {
    file_ext <- paste0(".", file_ext(x))
    tmp_file <- tempfile(fileext = file_ext)

    save_s3_to_local(
      s3_uri = x,
      path = tmp_file
    )

    data <- read_delim(tmp_file, delim = ",") %>%
      mutate(
        data_year = as.character(as.numeric(str_extract(x, "[0-9]{4}")) - 3),
        year = str_extract(x, "[0-9]{4}")
      )
    file.remove(tmp_file)

    return(data)
  }) %>%
  bind_rows() %>%
  select(
    geoid = "Zip Code", dci = "2018-2022 Final Distress Score", data_year, year
  ) %>%
  group_by(year)

output %>%
  write_partitions_to_s3(
    output_bucket,
    is_spatial = FALSE,
    overwrite = TRUE
  )
